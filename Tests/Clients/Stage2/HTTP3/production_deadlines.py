import asyncio
import json
import os
from pathlib import Path
import sys
import tempfile

from aioquic.asyncio.client import connect

from flow import FlowLogger
from production_lifecycle import client_group, wait_usage
from production_memory import zero
from production_resources import read_resources, refusals
from verify import Client, configuration, readiness, response_complete, retain_tail, shutdown


CASES = ("handshake-deadline-1", "handshake-deadline-30")


class FlightGate(asyncio.DatagramProtocol):
    def __init__(self, server_port):
        self.server = ("127.0.0.1", server_port)
        self.transport = None
        self.client = None
        self.first_forwarded = None
        self.released_at = None
        self.server_packets = []
        self.server_bytes = 0
        self.discarded_retransmissions = 0
        self.error = None

    def connection_made(self, transport):
        self.transport = transport

    def error_received(self, error):
        self.error = error

    def datagram_received(self, data, address):
        if self.error is not None:
            return
        if address == self.server:
            if self.client is None:
                self.error = RuntimeError("server flight arrived before a client Initial")
            elif self.released_at is not None:
                self.transport.sendto(data, self.client)
            elif len(self.server_packets) >= 64 or self.server_bytes + len(data) > 65536:
                self.error = RuntimeError("withheld handshake flight exceeded diagnostic bound")
            else:
                self.server_packets.append(data)
                self.server_bytes += len(data)
        elif self.client is not None and address != self.client:
            self.error = RuntimeError("handshake relay accepts one client")
        else:
            self.client = address
            if self.first_forwarded is None:
                self.first_forwarded = asyncio.get_running_loop().time()
                self.transport.sendto(data, self.server)
            elif self.released_at is not None:
                self.transport.sendto(data, self.server)
            else:
                self.discarded_retransmissions += 1

    def release(self):
        assert self.error is None, self.error
        assert self.server_packets and self.first_forwarded is not None
        self.released_at = asyncio.get_running_loop().time()
        for packet in self.server_packets:
            self.transport.sendto(packet, self.client)
        self.server_packets.clear()

    def record(self):
        assert self.error is None, self.error
        return {"firstInitialForwardedSeconds": self.first_forwarded,
                "flightReleasedSeconds": self.released_at,
                "withheldServerBytes": self.server_bytes,
                "discardedInitialRetransmissions": self.discarded_retransmissions}


def capture(ready):
    return json.loads(Path(ready["capturePath"]).read_text(encoding="utf-8"))


def origin_stats(peer):
    return json.loads(Path(peer["stats"]).read_text(encoding="utf-8"))


async def held_clients(ready, certificate, deadline, stack, clients, gates, traces):
    for _ in range(2):
        transport, gate = await asyncio.get_running_loop().create_datagram_endpoint(
            lambda: FlightGate(ready["port"]), local_addr=("127.0.0.1", 0)
        )
        stack.callback(transport.close)
        gates.append(gate)
        logger = FlowLogger()
        traces.append(logger)
        config = configuration(certificate, "localhost")
        config.idle_timeout = deadline + 10
        config.quic_logger = logger
        client = await stack.enter_async_context(connect(
            "127.0.0.1", transport.get_extra_info("sockname")[1], configuration=config,
            create_protocol=Client, wait_connected=False
        ))
        clients.append(client)
        client.transmit()
    live = await wait_usage(ready["resourcesPath"], lambda value: (
        value["pendingHandshakes"] == 2 and value["nativeListenerPendingHandshakes"] == 2
        and all(gate.server_packets for gate in gates)
    ), timeout=deadline)
    assert live["local"]["connections"] == live["nativeListenerConnections"] == 2, live
    assert live["local"]["streams"] == 0 and all(client.alpn is None for client in clients)
    return live


async def completion(ready, peer, certificate, deadline, output):
    gates, traces = [], []
    try:
        async with client_group() as (stack, clients):
            live = await held_clients(ready, certificate, deadline, stack, clients, gates, traces)
            assert origin_stats(peer)["datagrams"] == 0
            assert not any(event["kind"] == "requestHead" for event in capture(ready))
            before = sum(refusals(live, scope, "pendingHandshakes") for scope in ("local", "nativeListener"))
            extra = await stack.enter_async_context(connect(
                "127.0.0.1", ready["port"], configuration=configuration(certificate, "localhost"),
                create_protocol=Client, wait_connected=False
            ))
            clients.append(extra)
            extra.transmit()
            refused = await wait_usage(ready["resourcesPath"], lambda value: (
                sum(refusals(value, scope, "pendingHandshakes") for scope in ("local", "nativeListener")) > before
            ), timeout=deadline)
            assert refused["pendingHandshakes"] == refused["nativeListenerPendingHandshakes"] == 2, refused
            assert refused["local"]["connections"] == refused["nativeListenerConnections"] == 2, refused
            extra.close()
            for gate in gates:
                gate.release()
                assert gate.released_at < gate.first_forwarded + deadline, gate.record()
            await asyncio.wait_for(asyncio.gather(*(client.wait_connected() for client in clients[:2])), timeout=deadline)
            assert all(client.alpn == "h3" and client.connection_error is None for client in clients[:2])
            complete = await wait_usage(ready["resourcesPath"], lambda value: value["pendingHandshakes"] == 0
                                        and value["nativeListenerPendingHandshakes"] == 0)
            for index, client in enumerate(clients[:2]):
                assert await response_complete(client.request("localhost:443", f"/deadline-completed/{index}")[1]) == b"origin"
            assert extra.alpn is None, "over-capacity handshake completed"
            result = {"pendingAtCapacity": live, "excessHandshakeRefusal": refused,
                      "completedBeforeApplication": complete, "gates": [gate.record() for gate in gates]}
        result["afterDisconnect"] = await zero(ready["resourcesPath"], 67108864)
        return result
    finally:
        for index, trace in enumerate(traces):
            trace.write(output / f"completion-{index}.qlog")


async def expiry(ready, peer, certificate, deadline, output):
    gates, traces = [], []
    prior = origin_stats(peer)
    before_failures = sum(event["kind"] == "connectionFailure" and "setupTimedOut" in event.get("failure", "")
                          for event in capture(ready))
    try:
        with (output / "deadline-timeline.jsonl").open("w", encoding="utf-8") as timeline:
            async with client_group() as (stack, clients):
                live = await held_clients(ready, certificate, deadline, stack, clients, gates, traces)
                first = min(gate.first_forwarded for gate in gates)
                expired = None
                async with asyncio.timeout(deadline + 8):
                    while expired is None:
                        now = asyncio.get_running_loop().time()
                        value = read_resources(ready["resourcesPath"])
                        events = capture(ready)
                        timeouts = sum(event["kind"] == "connectionFailure" and "setupTimedOut" in event.get("failure", "")
                                       for event in events) - before_failures
                        timeline.write(json.dumps({"elapsedSinceFirstInitial": now - first, "resources": value,
                                                   "typedHandshakeTimeouts": timeouts}) + "\n")
                        timeline.flush()
                        assert all(gate.error is None and gate.released_at is None for gate in gates)
                        assert all(client.alpn is None for client in clients)
                        assert value["pendingHandshakes"] <= 2 and value["nativeListenerPendingHandshakes"] <= 2, value
                        assert value["local"]["connections"] <= 2 and value["nativeListenerConnections"] <= 2, value
                        if value["pendingHandshakes"] < 2:
                            assert now >= first + deadline, (now - first, deadline, value)
                        current_origin = origin_stats(peer)
                        assert current_origin["handshakes"] == prior["handshakes"], current_origin
                        assert current_origin["requests"] == prior["requests"], current_origin
                        assert sum(event["kind"] == "requestHead" for event in events) == 2, events
                        if timeouts == 2 and value["local"]["connections"] == 0 and value["nativeListenerConnections"] == 0:
                            expired = value
                        else:
                            await asyncio.sleep(0.05)
                result = {"pendingBeforeExpiry": live, "typedHandshakeTimeouts": 2,
                          "elapsedAtObservedExpiry": now - first, "expiredBeforeClientTeardown": expired,
                          "gates": [gate.record() for gate in gates]}
            result["afterExpiryQuiescence"] = await zero(ready["resourcesPath"], 67108864)
        async with connect("127.0.0.1", ready["port"], configuration=configuration(certificate, "localhost"),
                           create_protocol=Client) as client:
            assert await response_complete(client.request("localhost:443", "/deadline-recovery")[1]) == b"origin"
        result["afterRecovery"] = await zero(ready["resourcesPath"], 67108864)
        return result
    finally:
        for index, trace in enumerate(traces):
            trace.write(output / f"expiry-{index}.qlog")


async def run_case(binary, name, artifacts):
    assert name in CASES
    deadline = int(name.rsplit("-", 1)[1])
    output = artifacts / name
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="swiftmitm-h3-deadline-") as temporary:
        directory = Path(temporary)
        ready_path, origin_ready = directory / "host-ready.json", directory / "origin" / "ready.json"
        environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
        host, origin = None, None
        with (directory / "host.log").open("wb") as host_log, (directory / "origin.log").open("wb") as origin_log:
            try:
                origin = await asyncio.create_subprocess_exec(
                    sys.executable, str(Path(__file__).with_name("peer.py")), "--ready-file", str(origin_ready),
                    stdout=origin_log, stderr=asyncio.subprocess.STDOUT, env=environment
                )
                peer = await readiness(origin_ready, origin)
                config = {"authority": "localhost:443", "destinationAddress": "127.0.0.1",
                          "destinationPort": peer["port"], "originCAPath": peer["certificate"],
                          "readyPath": str(ready_path), "allowInternal": True, "maximumConnections": 16,
                          "maximumPendingHandshakes": 2, "maximumRequestStreamsPerConnection": 128,
                          "maximumRetainedBytes": 67108864, "handshakeDeadlineSeconds": deadline}
                config_path = directory / "configuration.json"
                config_path.write_text(json.dumps(config), encoding="utf-8")
                host = await asyncio.create_subprocess_exec(
                    str(binary), "--configuration", str(config_path), stdout=host_log,
                    stderr=asyncio.subprocess.STDOUT, env=environment
                )
                ready = await readiness(ready_path, host)
                certificate = Path(ready["certificate"])
                report = {"configuredHandshakeDeadlineSeconds": deadline}
                report["completion"] = await completion(ready, peer, certificate, deadline, output)
                report["expiry"] = await expiry(ready, peer, certificate, deadline, output)
                await shutdown(host)
                assert host.returncode == 0, "deadline host failed or did not stop cleanly"
                report["finalResources"] = read_resources(ready["resourcesPath"], stopped=True)
                stats = origin_stats(peer)
                assert stats["requests"] == 3 and stats["handshakes"] == 3, stats
                assert stats["paths"] == ["/deadline-completed/0", "/deadline-completed/1", "/deadline-recovery"], stats
                assert stats["alpn"] == ["h3", "h3", "h3"] and not stats["earlyDataAccepted"], stats
                report["origin"] = stats
                return report
            finally:
                try:
                    if host is not None:
                        await shutdown(host)
                finally:
                    if origin is not None:
                        await shutdown(origin)
                    for source, target in ((directory / "host.log", "host.log"),
                                           (directory / "origin.log", "origin.log"),
                                           (Path(str(ready_path) + ".resources.json"), "resources.json"),
                                           (Path(str(ready_path) + ".capture.json"), "capture.json"),
                                           (origin_ready.parent / "stats.json", "origin-stats.json")):
                        retain_tail(source, output / target)
