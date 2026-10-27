import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
from functools import partial
import json
import os
from pathlib import Path
import sys
import tempfile

from aioquic.asyncio.client import connect
from aioquic.asyncio.protocol import QuicConnectionProtocol

from production_memory import zero
from production_resources import current, quiescent, read_resources, refusals, stream_limit
from verify import Client, configuration, readiness, response_complete, retain_flow_trace, retain_tail, shutdown


CASES = (
    "lifecycle-restart", "pending-handshake-reset-churn", "connection-ceiling",
    "maximum-connection-ceiling", "maximum-handshake-ceiling",
)


async def command(ready_path, action, identifier):
    path = Path(str(ready_path) + ".command.json")
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps({"id": identifier, "action": action}), encoding="utf-8")
    temporary.replace(path)
    ack_path = Path(str(ready_path) + ".ack.json")
    async with asyncio.timeout(12):
        while True:
            if ack_path.exists():
                value = json.loads(ack_path.read_text(encoding="utf-8"))
                if value["id"] == identifier:
                    assert value["action"] == action, value
                    return value
            await asyncio.sleep(0.05)


async def wait_usage(path, predicate, byte_limit=67108864, timeout=8):
    async with asyncio.timeout(timeout):
        while True:
            value = read_resources(path, byte_limit=byte_limit)
            if predicate(value):
                return value
            await asyncio.sleep(0.05)


@asynccontextmanager
async def client_group():
    clients = []
    async with AsyncExitStack() as stack:
        try:
            yield stack, clients
        finally:
            for client in clients:
                client.close()
            await asyncio.wait_for(asyncio.gather(*(client.wait_closed() for client in clients)), timeout=8)


class HandshakeGate(asyncio.DatagramProtocol):
    def __init__(self, origin_port):
        self.origin = ("127.0.0.1", origin_port)
        self.client = None
        self.transport = None
        self.released = False
        self.withheld = 0
        self.forwarded = 0

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, address):
        if address == self.origin:
            assert self.client is not None
            self.transport.sendto(data, self.client)
        elif not self.released:
            self.client = address
            self.withheld += 1
        else:
            assert self.client is None or address == self.client, "gate supports one actual upstream session"
            self.client = address
            self.forwarded += 1
            self.transport.sendto(data, self.origin)


class InitialOnlyClient(QuicConnectionProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.received_packets = 0

    def datagram_received(self, data, address):
        self.received_packets += 1


async def restart(ready, authority, certificate, ready_path):
    async with connect("127.0.0.1", ready["port"], configuration=configuration(certificate, "localhost"),
                       create_protocol=Client) as client:
        limit = await stream_limit(client, authority, ready["resourcesPath"], response_complete)
        already = await command(ready_path, "startWhileRunning", "already-running")
        assert already["result"] == "alreadyRunning", already
    before = await quiescent(ready["resourcesPath"])
    assert before["local"]["connections"] == 0, before
    stop = await command(ready_path, "stop", "first-stop")
    assert stop["result"] == "success", stop
    first = read_resources(str(ready_path) + ".lifecycle-first-stop-after.json", stopped=True)
    assert refusals(first, "local", "requestStreams") >= 1, first
    started = await command(ready_path, "invalidPortThenStart", "restart")
    assert started["result"] == "invalidConfigurationThenStarted", started
    second = read_resources(str(ready_path) + ".lifecycle-restart-after.json")
    first_refusals = {(item["scope"], item["resource"]): item["count"] for item in first["refusals"]}
    second_refusals = {(item["scope"], item["resource"]): item["count"] for item in second["refusals"]}
    assert len(first_refusals) == len(first["refusals"]), first
    assert len(second_refusals) == len(second["refusals"]), second
    assert second_refusals == first_refusals, (first, second)
    peaks = ("maximumNativeStreamReservationBytes", "maximumParserReservationBytes",
             "peakForwardedPayloadBytesPerStream", "peakObservedRawReadCapacity",
             "peakSampledNativePendingBytesPerStream", "peakSampledNativeStorageTicketsPerStream")
    assert all(second[key] >= first[key] for key in peaks), (first, second)
    async with connect("127.0.0.1", started["port"], configuration=configuration(certificate, "localhost"),
                       create_protocol=Client) as client:
        assert await response_complete(client.request(authority, "/after-restart")[1]) == b"origin"
    return {"callerOwnedGroupRestart": True, "runningStartError": already["result"],
            "invalidPortPreflightRecovery": True, "cumulativeRefusals": first["refusals"],
            "firstIncarnation": limit, "postRestartResponse": "origin",
            "acquiredResourceStartupRollback": "not established by invalid-port preflight"}


async def handshake_churn(ready, authority, certificate, gate, evidence):
    samples = []
    path = ready["resourcesPath"]
    async with connect("127.0.0.1", ready["port"], configuration=configuration(certificate, "localhost"),
                       create_protocol=Client) as client:
        for sequence in range(32):
            stream, response = client.request(authority, f"/pending-cancel/{sequence}", end=False)
            live = await wait_usage(path, lambda value: value["local"]["streams"] == 1
                                    and value["pendingHandshakes"] == 1 and gate.withheld > 0)
            assert not response.done.is_set() and gate.withheld > 0
            client.cancel(stream)
            recovered = await wait_usage(path, lambda value: value["local"]["streams"] == 0
                                         and value["forwardingReservedBytes"] == 0)
            assert recovered["pendingHandshakes"] == 1 and recovered["local"]["connections"] == 2, recovered
            assert recovered["local"]["retainedBytes"] <= live["local"]["retainedBytes"], (live, recovered)
            samples.append({"sequence": sequence, "admitted": live, "afterReset": recovered})
            await asyncio.wait_for(client.ping(), timeout=3)
        gate.released = True
        assert await response_complete(client.request(authority, "/survivor-after-handshake-release")[1]) == b"origin"
        await quiescent(path)
        assert client.connection_error is None
    evidence.parent.mkdir(parents=True, exist_ok=True)
    evidence.write_text(json.dumps(samples, indent=2), encoding="utf-8")
    return {"cancelledWhileOriginHandshakePending": 32, "withheldActualPackets": gate.withheld,
            "forwardedAfterRelease": gate.forwarded, "sameDownstreamSurvivorCompleted": True,
            "cancelledWaiterObjectRetention": "not exposed by public resource counters"}


async def connection_ceiling(ready, authority, certificate, maximum, byte_limit, evidence):
    samples = []
    path = ready["resourcesPath"]
    initial = read_resources(path, byte_limit=byte_limit)
    observed_refusal = None
    usable = []
    expected_paths = []
    failed_probe_path = None
    survivor = None
    evidence.parent.mkdir(parents=True, exist_ok=True)
    loop = asyncio.get_running_loop()
    started = loop.time()
    peer_states = []
    with evidence.with_name("connection-progress.jsonl").open("w", encoding="utf-8") as progress:
        def record(phase, **fields):
            progress.write(json.dumps({"phase": phase, "elapsedSeconds": loop.time() - started,
                                       "offeredConnections": len(peer_states), "usableConnections": len(usable),
                                       "completedHandshakes": sum(item["handshakeCompletedSeconds"] is not None
                                                                  for item in peer_states), **fields}) + "\n")
            progress.flush()

        class CeilingClient(Client):
            def __init__(self, *args, peer_index, **kwargs):
                super().__init__(*args, diagnostics={}, **kwargs)
                self.peer_index = peer_index
                self.termination_recorded = False

            def quic_event_received(self, event):
                super().quic_event_received(event)
                if "termination" in self.diagnostics and not self.termination_recorded:
                    self.termination_recorded = True
                    record("peer-terminated", peerIndex=self.peer_index,
                           peerState=peer_states[self.peer_index], **self.diagnostics)

        async def ping(index, client):
            await client.ping()
            peer_states[index]["lastSuccessfulPingSeconds"] = loop.time() - started
            record("peer-ping-completed", peerIndex=index, peerState=peer_states[index])

        async def keep_alive():
            while True:
                await asyncio.wait_for(asyncio.gather(*(ping(index, item) for index, item in enumerate(usable))), timeout=5)
                await asyncio.sleep(0.5)

        async def settled_ownership(expected=None):
            previous = None
            after = read_resources(path, byte_limit=byte_limit)["sampleEndedNanoseconds"]
            async with asyncio.timeout(8):
                while True:
                    value = read_resources(path, byte_limit=byte_limit)
                    if value["sampleStartedNanoseconds"] > after:
                        after = value["sampleEndedNanoseconds"]
                        observed = current(value)
                        counts_match = (
                            value["local"]["connections"] == 2 * len(usable)
                            and value["nativeListenerConnections"] == len(usable)
                            and value["activeConnectionStorageLifetimes"] == 2 * len(usable)
                            and value["pendingHandshakes"] == value["nativeListenerPendingHandshakes"] == 0
                            and value["local"]["streams"] == (1 if survivor is not None else 0)
                        )
                        if counts_match and observed == previous and (expected is None or observed == expected):
                            return value
                        previous = observed if counts_match else None
                    await asyncio.sleep(0.05)

        record("initial", resources=initial)
        async with client_group() as (stack, clients):
            keeper = asyncio.create_task(keep_alive())
            try:
                for attempt in range(maximum + 1):
                    assert all(item.connection_error is None for item in usable), "previously usable peer terminated"
                    attempt_before = await settled_ownership()
                    peer_states.append({"handshakeCompletedSeconds": None, "usableSeconds": None,
                                        "lastSuccessfulPingSeconds": None})
                    record("attempt-started", peerIndex=attempt, resources=attempt_before)
                    context = connect("127.0.0.1", ready["port"], configuration=configuration(certificate, "localhost"),
                                      create_protocol=partial(CeilingClient, peer_index=attempt), wait_connected=False)
                    client = await stack.enter_async_context(context)
                    clients.append(client)
                    client.transmit()
                    new_failure = None
                    try:
                        await asyncio.wait_for(client.wait_connected(), timeout=2)
                    except (TimeoutError, ConnectionError) as error:
                        new_failure = type(error).__name__
                        record("handshake-failed", peerIndex=attempt, errorType=new_failure,
                               diagnostics=client.diagnostics)
                    if new_failure is None:
                        peer_states[attempt]["handshakeCompletedSeconds"] = loop.time() - started
                        assert client.alpn == "h3", client.alpn
                        record("handshake-completed", peerIndex=attempt, peerState=peer_states[attempt],
                               resources=read_resources(path, byte_limit=byte_limit))
                        if client.connection_error is not None:
                            new_failure = f"connection terminated: {client.connection_error}"
                            record("bootstrap-failed", peerIndex=attempt, diagnostics=client.diagnostics)
                    if new_failure is None:
                        probe_path = f"/connection-ceiling-warmup/{attempt}"
                        _, probe = client.request(authority, probe_path)
                        try:
                            await asyncio.wait_for(probe.done.wait(), timeout=15)
                        except TimeoutError:
                            new_failure = "warmupTimeout"
                        if new_failure is None and probe.error:
                            new_failure = probe.error
                        if new_failure is None:
                            assert (b":status", b"200") in probe.headers and bytes(probe.body) == b"origin", probe
                            assert client.connection_error is None, client.diagnostics
                            expected_paths.append(probe_path)
                        else:
                            failed_probe_path = probe_path
                            record("warmup-failed", peerIndex=attempt, failure=new_failure,
                                   diagnostics=client.diagnostics)
                    if new_failure is not None:
                        usage = await wait_usage(path, lambda value: (
                            refusals(value, "nativeListener", "connections") > refusals(attempt_before, "nativeListener", "connections")
                            or refusals(value, "local", "connections") > refusals(attempt_before, "local", "connections")
                            or refusals(value, "local", "retainedBytes") > refusals(attempt_before, "local", "retainedBytes")
                        ), byte_limit=byte_limit)
                        assert all(item.connection_error is None for item in usable), "refusal terminated an older usable peer"
                        observed_refusal = usage
                        record("typed-new-work-refusal", peerIndex=attempt, resources=usage,
                               beforeAttempt=attempt_before)
                        client.close()
                        await asyncio.wait_for(client.wait_closed(), timeout=8)
                        settled = await settled_ownership(expected=current(attempt_before))
                        record("failed-attempt-disposed", resources=settled)
                        break
                    usable.append(client)
                    peer_states[attempt]["usableSeconds"] = loop.time() - started
                    usage = await wait_usage(path, lambda value: (
                        value["local"]["connections"] == 2 * len(usable)
                        and value["nativeListenerConnections"] == len(usable)
                        and value["pendingHandshakes"] == value["nativeListenerPendingHandshakes"] == 0
                    ), byte_limit=byte_limit)
                    assert usage["local"]["connections"] <= maximum, usage
                    samples.append(usage)
                    record("usable-connection", peerIndex=attempt, peerState=peer_states[attempt], resources=usage)
                    if survivor is None:
                        stream, response = client.request(authority, "/duplex", end=False)
                        await asyncio.wait_for(response.first_data.wait(), timeout=8)
                        assert not response.error and not response.done.is_set() and bytes(response.body) == b"early:"
                        survivor = (client, stream, response)
                        expected_paths.append("/duplex")
                    if len(usable) % 16 == 0:
                        record("ping-batch-started", peerIndex=attempt)
                        await asyncio.wait_for(asyncio.gather(*(ping(index, item) for index, item in enumerate(usable))),
                                               timeout=5)
                        record("ping-batch-completed", peerIndex=attempt)
                assert observed_refusal is not None and usable and survivor is not None, "no typed refusal at usable capacity"
                await asyncio.wait_for(asyncio.gather(*(ping(index, item) for index, item in enumerate(usable))), timeout=5)
                assert all(item.connection_error is None for item in usable), "older usable peer terminated at capacity"
                actual = len(usable)
                byte_refused = refusals(observed_refusal, "local", "retainedBytes") > refusals(
                    attempt_before, "local", "retainedBytes")
                assert byte_refused or settled["local"]["connections"] == maximum, (actual, maximum, observed_refusal, settled)
                survivor_client, survivor_stream, survivor_response = survivor
                survivor_client.body(survivor_stream, b"survives-connection-refusal", end=True)
                assert await response_complete(survivor_response) == b"early:survives-connection-refusal"
                assert all(item.connection_error is None for item in usable), "survivor progress terminated an older peer"
                completed_handshakes = sum(item["handshakeCompletedSeconds"] is not None for item in peer_states)
            finally:
                keeper.cancel()
                try:
                    await keeper
                except asyncio.CancelledError:
                    pass
                finally:
                    record("admission-scope-ended", peerStates=peer_states,
                           peerDiagnostics=[client.diagnostics for client in clients])
    after_release = await zero(path, byte_limit)
    async with connect("127.0.0.1", ready["port"], configuration=configuration(certificate, "localhost"),
                       create_protocol=Client) as client:
        assert await response_complete(client.request(authority, "/after-connection-refusal")[1]) == b"origin"
    expected_paths.append("/after-connection-refusal")
    after_recovery = await zero(path, byte_limit)
    evidence.write_text(json.dumps(samples, indent=2), encoding="utf-8")
    return {"configuredCombinedConnections": maximum, "offeredDownstreamConnections": len(peer_states),
            "actualCompletedDownstreamHandshakes": completed_handshakes, "actualUsableDownstreamConnections": actual,
            "settledCombinedConnectionsAtRefusal": settled["local"]["connections"],
            "byteLimit": byte_limit, "typedRefusal": observed_refusal["refusals"],
            "allUsablePeersPingSurvived": True, "heldExchangeExactBodySurvived": True,
            "failedAttemptDisposed": settled, "afterRelease": after_release, "afterRecovery": after_recovery,
            "successfulOriginPaths": expected_paths, "failedProbePath": failed_probe_path,
            "postReleaseRequestCompleted": True}


async def pending_ceiling(ready, certificate, evidence):
    path = ready["resourcesPath"]
    byte_limit = 268435456
    samples = []
    async with client_group() as (stack, clients):
        for _ in range(257):
            clients.append(await stack.enter_async_context(connect(
                "127.0.0.1", ready["port"], configuration=configuration(certificate, "localhost"),
                create_protocol=InitialOnlyClient, wait_connected=False
            )))
            clients[-1].transmit()
        peak = await wait_usage(path, lambda value: (
            refusals(value, "nativeListener", "pendingHandshakes") > 0
            or refusals(value, "local", "pendingHandshakes") > 0
            or refusals(value, "local", "retainedBytes") > 0
        ), byte_limit=byte_limit)
        assert 0 < peak["pendingHandshakes"] <= 256, peak
        assert peak["local"]["connections"] <= 1024, peak
        samples.append(peak)
        initial_refusals = sum(item["count"] for item in peak["refusals"])
        for cycle in range(3):
            for _ in range(8):
                clients.append(await stack.enter_async_context(connect(
                    "127.0.0.1", ready["port"], configuration=configuration(certificate, "localhost"),
                    create_protocol=InitialOnlyClient, wait_connected=False
                )))
                clients[-1].transmit()
            await asyncio.sleep(0.12)
            usage = read_resources(path, byte_limit=byte_limit)
            assert usage["pendingHandshakes"] <= 256 and usage["local"]["connections"] <= 1024, usage
            samples.append(dict(usage, refusalChurnCycle=cycle))
        await wait_usage(path, lambda value: sum(item["count"] for item in value["refusals"]) > initial_refusals,
                         byte_limit=byte_limit)
    recovered = await wait_usage(path, lambda value: value["local"]["connections"] == 0
                                 and value["pendingHandshakes"] == 0, byte_limit=byte_limit, timeout=35)
    evidence.parent.mkdir(parents=True, exist_ok=True)
    evidence.write_text(json.dumps(samples, indent=2), encoding="utf-8")
    return {"configuredPendingHandshakes": 256, "actualPendingAtRefusal": peak["pendingHandshakes"],
            "refusalChurnInitials": 24, "refusals": samples[-1]["refusals"],
            "eventualActualRelease": current(recovered)}


async def run_case(binary, name, artifacts):
    assert name in CASES
    with tempfile.TemporaryDirectory(prefix="swiftmitm-h3-lifecycle-") as temporary:
        directory = Path(temporary)
        ready_path = directory / "host-ready.json"
        origin_ready = directory / "origin" / "ready.json"
        environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
        host, origin, gate_transport = None, None, None
        with (directory / "origin.log").open("wb") as origin_log, (directory / "host.log").open("wb") as host_log:
            try:
                origin_arguments = [sys.executable, str(Path(__file__).with_name("peer.py")),
                                    "--ready-file", str(origin_ready)]
                if name in ("connection-ceiling", "maximum-connection-ceiling"):
                    origin_arguments.append("--capacity-peer")
                origin = await asyncio.create_subprocess_exec(
                    *origin_arguments, stdout=origin_log, stderr=asyncio.subprocess.STDOUT, env=environment
                )
                peer = await readiness(origin_ready, origin)
                destination = peer["port"]
                gate = None
                if name == "pending-handshake-reset-churn":
                    gate_transport, gate = await asyncio.get_running_loop().create_datagram_endpoint(
                        lambda: HandshakeGate(peer["port"]), local_addr=("127.0.0.1", 0)
                    )
                    destination = gate_transport.get_extra_info("sockname")[1]
                maximum = 1024 if name.startswith("maximum-") else 128
                byte_limit = 268435456 if name.startswith("maximum-") else 67108864
                config = {"authority": "localhost:443", "destinationAddress": "127.0.0.1",
                          "destinationPort": destination, "originCAPath": peer["certificate"],
                          "readyPath": str(ready_path), "allowInternal": True,
                          "maximumConnections": 2 if name == "connection-ceiling" else maximum,
                          "maximumPendingHandshakes": 256 if name.startswith("maximum-") else 64,
                          "maximumRequestStreamsPerConnection": 2 if name == "lifecycle-restart" else 128,
                          "maximumRetainedBytes": byte_limit, "callerOwnedGroup": name == "lifecycle-restart",
                          "handshakeDeadlineSeconds": 30}
                configuration_path = directory / "configuration.json"
                configuration_path.write_text(json.dumps(config), encoding="utf-8")
                host = await asyncio.create_subprocess_exec(
                    str(binary), "--configuration", str(configuration_path), stdout=host_log,
                    stderr=asyncio.subprocess.STDOUT, env=environment
                )
                ready = await readiness(ready_path, host)
                certificate = Path(ready["certificate"])
                evidence = artifacts / name / "lifecycle-samples.json"
                if name == "lifecycle-restart":
                    report = await restart(ready, config["authority"], certificate, ready_path)
                elif name == "pending-handshake-reset-churn":
                    report = await handshake_churn(ready, config["authority"], certificate, gate, evidence)
                elif name == "maximum-handshake-ceiling":
                    report = await pending_ceiling(ready, certificate, evidence)
                else:
                    report = await connection_ceiling(ready, config["authority"], certificate,
                                                      config["maximumConnections"], byte_limit, evidence)
                await shutdown(host)
                assert host.returncode == 0, "host lifecycle failed or did not stop cleanly"
                report["finalResources"] = read_resources(ready["resourcesPath"], stopped=True, byte_limit=byte_limit)
                if name in ("connection-ceiling", "maximum-connection-ceiling"):
                    await shutdown(origin)
                    assert origin.returncode == 0, "capacity peer failed to stop cleanly"
                stats = json.loads(Path(peer["stats"]).read_text(encoding="utf-8"))
                if name in ("connection-ceiling", "maximum-connection-ceiling"):
                    keepalive = stats["capacityKeepalive"]
                    assert keepalive["failures"] == keepalive["activeTasks"] == 0, keepalive
                    assert keepalive["acknowledgedPings"] > 0, keepalive
                    assert keepalive["peakTasks"] >= report["actualUsableDownstreamConnections"], keepalive
                    report["originKeepalive"] = keepalive
                    failed_path = report["failedProbePath"]
                    assert failed_path is None or stats["paths"].count(failed_path) <= 1, stats
                    successful_paths = [item for item in stats["paths"] if item != failed_path]
                    assert successful_paths == report["successfulOriginPaths"], stats
                    assert stats["requests"] == len(stats["paths"]), stats
                    report["failedProbeReachedOrigin"] = failed_path is not None and failed_path in stats["paths"]
                else:
                    expected_requests = 5 if name == "lifecycle-restart" else (
                        0 if name == "maximum-handshake-ceiling" else 1)
                    assert stats["requests"] == expected_requests, stats
                report["originRequests"] = stats["requests"]
                return report
            finally:
                try:
                    if host is not None:
                        await shutdown(host)
                finally:
                    if gate_transport is not None:
                        gate_transport.close()
                    if origin is not None:
                        await shutdown(origin)
                    for source, target in ((directory / "host.log", "host.log"),
                                           (directory / "origin.log", "origin.log"),
                                           (Path(str(ready_path) + ".resources.json"), "resources.json"),
                                           (origin_ready.parent / "stats.json", "origin-stats.json")):
                        retain_tail(source, artifacts / name / target)
                    for snapshot in directory.glob("host-ready.json.lifecycle-*.json"):
                        retain_flow_trace(snapshot, artifacts / name / snapshot.name)
