import asyncio
from contextlib import AsyncExitStack
import json
import os
from pathlib import Path
import sys
import tempfile

from aioquic.asyncio.client import connect

from flow import FlowLogger
from production_resources import read_resources
from production_security import await_stats
from verify import Client, configuration, readiness, response_complete, retain_tail, shutdown


CASES = ("validated-address-migration",)


class RelayEndpoint(asyncio.DatagramProtocol):
    def __init__(self, relay, upstream):
        self.relay = relay
        self.upstream = upstream
        self.transport = None
        self.active = True

    def connection_made(self, transport):
        self.transport = transport

    def error_received(self, error):
        self.relay.error = error

    def datagram_received(self, data, address):
        if not self.active:
            return
        if self.upstream:
            if self.relay.client is not None:
                self.relay.front.transport.sendto(data, self.relay.client)
        else:
            if self.relay.client is None:
                self.relay.client = address
            if address != self.relay.client:
                self.relay.error = RuntimeError("migration relay accepts exactly one client")
                return
            self.relay.back.transport.sendto(data)

    def close(self):
        self.active = False
        if self.transport is not None:
            self.transport.close()


class RebindingRelay:
    def __init__(self, server_port):
        self.server_port = server_port
        self.client = None
        self.front = None
        self.back = None
        self.endpoints = []
        self.error = None

    async def endpoint(self, upstream):
        endpoint = RelayEndpoint(self, upstream)
        self.endpoints.append(endpoint)
        arguments = {"local_addr": ("127.0.0.1", 0)}
        if upstream:
            arguments["remote_addr"] = ("127.0.0.1", self.server_port)
        await asyncio.get_running_loop().create_datagram_endpoint(lambda: endpoint, **arguments)
        return endpoint

    async def start(self):
        self.back = await self.endpoint(True)
        self.front = await self.endpoint(False)
        return self.front.transport.get_extra_info("sockname")[1]

    async def rebind(self):
        previous = self.back
        old_port = previous.transport.get_extra_info("sockname")[1]
        self.back = await self.endpoint(True)
        new_port = self.back.transport.get_extra_info("sockname")[1]
        assert new_port != old_port, (old_port, new_port)
        previous.close()
        return old_port, new_port

    def close(self):
        for endpoint in self.endpoints:
            endpoint.close()


async def path_response(trace, start):
    async with asyncio.timeout(10):
        while True:
            challenges = set()
            responses = {}
            for event in trace.to_dict()["events"][start:]:
                if event["name"] not in ("transport:packet_sent", "transport:packet_received"):
                    continue
                packet = event["data"]
                if packet["header"]["packet_type"] != "1RTT":
                    continue
                for frame in packet["frames"]:
                    if event["name"] == "transport:packet_sent":
                        if frame["frame_type"] == "path_response" and frame["data"] in challenges:
                            responses[packet["header"]["packet_number"]] = frame["data"]
                    elif frame["frame_type"] == "path_challenge":
                        challenges.add(frame["data"])
                    elif frame["frame_type"] == "ack":
                        acknowledged = {number: token for number, token in responses.items()
                                        if any(lower <= number <= upper for lower, upper in frame["acked_ranges"])}
                        if acknowledged:
                            return {"challengeTokens": sorted(set(acknowledged.values())),
                                    "acknowledgedResponsePackets": sorted(acknowledged)}
            await asyncio.sleep(0.02)


async def migrate(ready, authority, certificate, peer, artifacts):
    relay = RebindingRelay(ready["port"])
    logger = FlowLogger()
    try:
        port = await relay.start()
        config = configuration(certificate, "localhost")
        config.quic_logger = logger
        async with connect("127.0.0.1", port, configuration=config, create_protocol=Client) as client:
            assert client.alpn == "h3"
            assert await response_complete(client.request(authority, "/security/migration-before")[1]) == b"origin"
            stream, held = client.request(authority, "/duplex", end=False)
            await asyncio.wait_for(held.first_data.wait(), timeout=10)
            assert bytes(held.body) == b"early:" and not held.done.is_set()
            before = await await_stats(peer, lambda value: value["requests"] == 2)
            assert before["handshakes"] == 1, before
            trace = client._quic._quic_logger
            start = len(trace.to_dict()["events"])
            old_port, new_port = await relay.rebind()
            await asyncio.wait_for(client.ping(), timeout=10)
            validation = await path_response(trace, start)
            assert not held.done.is_set(), held.error
            assert await response_complete(client.request(authority, "/security/migration-after")[1]) == b"origin"
            client.body(stream, b"migration-kept-this-exchange", end=True)
            assert await response_complete(held) == b"early:migration-kept-this-exchange"
            await asyncio.wait_for(client.ping(), timeout=10)
            assert client.connection_error is None
            async with connect("127.0.0.1", ready["port"], configuration=configuration(certificate, "localhost"),
                               create_protocol=Client) as other:
                assert await response_complete(other.request(authority, "/security/migration-other-client")[1]) == b"origin"
                assert other.connection_error is None
            stats = await await_stats(peer, lambda value: value["requests"] == 4)
            migrated = {entry["connection"] for entry in stats["securityRequests"]
                        if entry["path"] != "/security/migration-other-client"}
            independent = {entry["connection"] for entry in stats["securityRequests"]
                           if entry["path"] == "/security/migration-other-client"}
            assert len(migrated) == len(independent) == 1 and migrated.isdisjoint(independent), stats
            assert stats["handshakes"] == 2 and stats["alpn"] == ["h3", "h3"], stats
            assert not stats["earlyDataAccepted"], stats
            assert relay.error is None, relay.error
            return {"originalPort": old_port, "migratedPort": new_port,
                    "pathValidation": validation, "migratedOriginConnections": sorted(migrated),
                    "otherClientOriginConnections": sorted(independent), "originHandshakes": stats["handshakes"],
                    "activeExchangeAndSiblingCompleted": True,
                    "validationObservation": "matching PATH_CHALLENGE/PATH_RESPONSE, response packet ACK and continued traffic"}
    finally:
        relay.close()
        logger.write(artifacts / "client.qlog")


async def run_case(binary, name, artifacts):
    assert name in CASES, name
    artifacts.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="swiftmitm-h3-migration-") as temporary:
        directory = Path(temporary)
        environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
        origin = host = ready = peer = None
        async with AsyncExitStack() as stack:
            try:
                origin_log = stack.enter_context((directory / "origin.log").open("wb"))
                origin_ready = directory / "origin" / "ready.json"
                origin = await asyncio.create_subprocess_exec(
                    sys.executable, str(Path(__file__).with_name("production_security.py")),
                    "--ready-file", str(origin_ready), stdout=origin_log, stderr=asyncio.subprocess.STDOUT,
                    env=environment
                )
                peer = await readiness(origin_ready, origin)
                authority = "localhost:443"
                config = {"authority": authority, "destinationAddress": "127.0.0.1", "destinationPort": peer["port"],
                          "originCAPath": peer["certificate"], "readyPath": str(directory / "host-ready.json"),
                          "allowInternal": True, "maximumConnections": 128, "maximumPendingHandshakes": 64,
                          "maximumRequestStreamsPerConnection": 128, "maximumRetainedBytes": 67108864}
                path = directory / "configuration.json"
                path.write_text(json.dumps(config), encoding="utf-8")
                host_log = stack.enter_context((directory / "host.log").open("wb"))
                host = await asyncio.create_subprocess_exec(str(binary), "--configuration", str(path),
                                                          stdout=host_log, stderr=asyncio.subprocess.STDOUT,
                                                          env=environment)
                ready = await readiness(Path(config["readyPath"]), host)
                result = await migrate(ready, authority, Path(ready["certificate"]), peer, artifacts)
                await shutdown(host)
                assert host.returncode == 0, host.returncode
                result["finalResources"] = read_resources(ready["resourcesPath"], stopped=True)
                capture = json.loads(Path(ready["capturePath"]).read_text(encoding="utf-8"))
                heads = [event for event in capture if event["kind"] == "requestHead"]
                assert len(heads) == 4, heads
                for event in heads:
                    assert event["authority"] == authority and event["tlsServerName"] == "localhost", event
                    assert event["destinationAddress"] == "127.0.0.1" and event["destinationPort"] == peer["port"], event
                    assert event["provenance"] == "fixedRouteHTTP3", event
                    assert event["originalClientAddress"] == "127.0.0.1", event
                    if event["path"] != "/security/migration-other-client":
                        assert event["originalClientPort"] == result["originalPort"], event
                identifiers = {event["requestID"] for event in heads}
                assert not any(event["kind"] == "streamError" and event.get("requestID") in identifiers
                               for event in capture), capture
                result["fixedDestination"] = {"address": "127.0.0.1", "port": peer["port"]}
                result["originalClientProvenancePreserved"] = True
                return result
            finally:
                cleanup = await asyncio.gather(*(shutdown(process) for process in (host, origin) if process is not None),
                                               return_exceptions=True)
                for label in ("host", "origin"):
                    retain_tail(directory / f"{label}.log", artifacts / f"{label}.log")
                if peer is not None:
                    retain_tail(Path(peer["stats"]), artifacts / "origin-stats.json")
                if ready is not None:
                    retain_tail(Path(ready["resourcesPath"]), artifacts / "resources.json")
                    retain_tail(Path(ready["capturePath"]), artifacts / "capture.json")
                failures = [error for error in cleanup if isinstance(error, Exception)]
                if failures:
                    raise AssertionError("migration peer cleanup failed") from failures[0]
