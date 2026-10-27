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
    assert second["refusals"] == first["refusals"], (first, second)
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
    admitted = []
    evidence.parent.mkdir(parents=True, exist_ok=True)
    loop = asyncio.get_running_loop()
    started = loop.time()
    peer_states = []
    with evidence.with_name("connection-progress.jsonl").open("w", encoding="utf-8") as progress:
        def record(phase, **fields):
            progress.write(json.dumps({"phase": phase, "elapsedSeconds": loop.time() - started,
                                       "completedHandshakes": len(admitted), **fields}) + "\n")
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

        record("initial", resources=initial)
        async with client_group() as (stack, clients):
            try:
                for attempt in range(maximum + 1):
                    peer_states.append({"handshakeCompletedSeconds": None, "lastSuccessfulPingSeconds": None})
                    record("attempt-started", peerIndex=attempt,
                           resources=read_resources(path, byte_limit=byte_limit))
                    context = connect("127.0.0.1", ready["port"], configuration=configuration(certificate, "localhost"),
                                      create_protocol=partial(CeilingClient, peer_index=attempt), wait_connected=False)
                    client = await stack.enter_async_context(context)
                    clients.append(client)
                    client.transmit()
                    try:
                        await asyncio.wait_for(client.wait_connected(), timeout=2)
                        admitted.append(client)
                        peer_states[attempt]["handshakeCompletedSeconds"] = loop.time() - started
                    except (TimeoutError, ConnectionError) as error:
                        record("handshake-failed", peerIndex=attempt, errorType=type(error).__name__,
                               diagnostics=client.diagnostics)
                        usage = await wait_usage(path, lambda value: (
                            refusals(value, "nativeListener", "connections") > refusals(initial, "nativeListener", "connections")
                            or refusals(value, "local", "connections") > refusals(initial, "local", "connections")
                            or refusals(value, "local", "retainedBytes") > refusals(initial, "local", "retainedBytes")
                        ), byte_limit=byte_limit)
                        observed_refusal = usage
                        record("typed-refusal", peerIndex=attempt, resources=usage)
                        break
                    record("handshake-completed", peerIndex=attempt, peerState=peer_states[attempt],
                           resources=read_resources(path, byte_limit=byte_limit))
                    if len(admitted) % 16 == 0:
                        record("ping-batch-started", peerIndex=attempt)
                        await asyncio.wait_for(asyncio.gather(*(ping(index, item) for index, item in enumerate(admitted))),
                                               timeout=5)
                        record("ping-batch-completed", peerIndex=attempt)
                    usage = read_resources(path, byte_limit=byte_limit)
                    assert usage["local"]["connections"] <= maximum, usage
                    samples.append(usage)
                assert observed_refusal is not None and admitted, "no typed admission refusal at configured ceiling"
                for index, client in enumerate(admitted[:3]):
                    await asyncio.wait_for(ping(index, client), timeout=3)
                actual = len(admitted)
                byte_refused = refusals(observed_refusal, "local", "retainedBytes") > refusals(
                    initial, "local", "retainedBytes")
                assert byte_refused or actual == maximum, (actual, maximum, observed_refusal)
            finally:
                record("admission-scope-ended", peerStates=peer_states,
                       peerDiagnostics=[client.diagnostics for client in clients])
    await wait_usage(path, lambda value: value["local"]["connections"] == 0, byte_limit=byte_limit)
    async with connect("127.0.0.1", ready["port"], configuration=configuration(certificate, "localhost"),
                       create_protocol=Client) as client:
        assert await response_complete(client.request(authority, "/after-connection-refusal")[1]) == b"origin"
    evidence.parent.mkdir(parents=True, exist_ok=True)
    evidence.write_text(json.dumps(samples, indent=2), encoding="utf-8")
    return {"configuredConnections": maximum, "actualCompletedDownstreamHandshakes": actual,
            "byteLimit": byte_limit, "typedRefusal": observed_refusal["refusals"],
            "admittedPingSurvived": True, "postReleaseRequestCompleted": True}


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
                origin = await asyncio.create_subprocess_exec(
                    sys.executable, str(Path(__file__).with_name("peer.py")), "--ready-file", str(origin_ready),
                    stdout=origin_log, stderr=asyncio.subprocess.STDOUT, env=environment
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
                stats = json.loads(Path(peer["stats"]).read_text(encoding="utf-8"))
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
