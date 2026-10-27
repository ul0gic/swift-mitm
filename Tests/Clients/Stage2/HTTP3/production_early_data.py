import asyncio
from functools import partial
import json
import os
from pathlib import Path
import sys
import tempfile

from aioquic.asyncio.client import connect
from aioquic.asyncio.server import QuicServer
from aioquic.buffer import Buffer
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import HandshakeCompleted
from aioquic.quic.packet import QuicPacketType, pull_quic_header

from flow import FlowLogger
from production_resources import read_resources
from verify import Client, configuration, create_identity, readiness, response_complete, retain_tail, shutdown


CASES = ("unsolicited-early-data",)


async def foreign_ticket(directory):
    directory.mkdir()
    certificate, key = create_identity(directory)
    server_configuration = QuicConfiguration(is_client=False, alpn_protocols=["h3"])
    server_configuration.load_cert_chain(certificate, key)
    server_tickets, client_tickets = [], []
    received = asyncio.Event()

    def save_server(ticket):
        assert len(server_tickets) < 4
        server_tickets.append(ticket)

    def save_client(ticket):
        assert len(client_tickets) < 4
        client_tickets.append(ticket)
        received.set()

    transport, server = await asyncio.get_running_loop().create_datagram_endpoint(
        lambda: QuicServer(configuration=server_configuration, session_ticket_handler=save_server),
        local_addr=("127.0.0.1", 0)
    )
    try:
        async with connect("127.0.0.1", transport.get_extra_info("sockname")[1],
                           configuration=configuration(certificate, "localhost"),
                           session_ticket_handler=save_client) as client:
            await asyncio.wait_for(received.wait(), timeout=5)
            await asyncio.wait_for(client.ping(), timeout=3)
            ticket = client_tickets[0]
            assert ticket.is_valid and ticket.server_name == "localhost"
            assert ticket.max_early_data_size == 0xFFFFFFFF
            assert any(item.ticket == ticket.ticket for item in server_tickets)
            return ticket
    finally:
        server.close()
        transport.close()


class HandshakeReplyGate(asyncio.DatagramProtocol):
    def __init__(self, server_port, connection_id_length):
        self.server = ("127.0.0.1", server_port)
        self.connection_id_length = connection_id_length
        self.client = None
        self.transport = None
        self.held = []
        self.held_bytes = 0
        self.server_datagrams = 0
        self.forwarded_server_datagrams = 0
        self.forwarded_early_packets = 0
        self.forwarded_padding_bytes = 0
        self.forwarded_padded_datagrams = 0
        self.released = False
        self.error = None

    def connection_made(self, transport):
        self.transport = transport

    def error_received(self, error):
        self.error = error

    def datagram_received(self, data, address):
        if address == self.server:
            self.server_datagrams += 1
            if self.released:
                self.transport.sendto(data, self.client)
                self.forwarded_server_datagrams += 1
            elif len(self.held) < 256 and self.held_bytes + len(data) <= 256 * 1024:
                self.held.append(data)
                self.held_bytes += len(data)
            else:
                self.error = RuntimeError("independent handshake reply buffer exceeded")
        else:
            if self.client is None:
                self.client = address
            if address != self.client:
                self.error = RuntimeError("handshake gate received a second client")
                return
            buffer = Buffer(data=data)
            early_packets, padding_bytes = 0, 0
            try:
                while not buffer.eof():
                    start = buffer.tell()
                    # aioquic 1.7.0 packet_builder.py:362–374 appends zero bytes after the final coalesced packet.
                    if start > 0 and not any(data[start:]):
                        padding_bytes = len(data) - start
                        break
                    header = pull_quic_header(buffer, host_cid_length=self.connection_id_length)
                    if header.packet_type == QuicPacketType.ZERO_RTT:
                        early_packets += 1
                    buffer.seek(start + header.packet_length)
            except ValueError as error:
                if self.error is None:
                    self.error = ValueError(f"independent QUIC header parse failed at byte {buffer.tell()}: {str(error)[:256]}")
                return
            self.transport.sendto(data, self.server)
            self.forwarded_early_packets += early_packets
            self.forwarded_padding_bytes += padding_bytes
            self.forwarded_padded_datagrams += int(padding_bytes > 0)

    def release(self):
        assert not self.released and self.client is not None
        self.released = True
        for data in self.held:
            self.transport.sendto(data, self.client)
            self.forwarded_server_datagrams += 1
        self.held.clear()
        self.held_bytes = 0


class EarlyDataClient(Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.handshake = None

    def quic_event_received(self, event):
        if isinstance(event, HandshakeCompleted):
            self.handshake = {"alpn": event.alpn_protocol, "earlyDataAccepted": event.early_data_accepted,
                              "sessionResumed": event.session_resumed}
        super().quic_event_received(event)


def transmitted_stream(trace, packet_type, stream):
    return any(event["name"] == "transport:packet_sent"
               and event["data"]["header"]["packet_type"] == packet_type
               and any(frame["frame_type"] == "stream" and frame["stream_id"] == stream and frame["length"] > 0
                       for frame in event["data"]["frames"])
               for event in trace.to_dict()["events"])


async def exercise(ready, peer, ticket, artifacts):
    logger = FlowLogger()
    config = configuration(Path(ready["certificate"]), "localhost")
    config.session_ticket, config.quic_logger = ticket, logger
    gate = HandshakeReplyGate(ready["port"], config.connection_id_length)
    transport, _ = await asyncio.get_running_loop().create_datagram_endpoint(
        lambda: gate, local_addr=("127.0.0.1", 0)
    )
    diagnostics, samples = {}, []
    try:
        async with connect("127.0.0.1", transport.get_extra_info("sockname")[1], configuration=config,
                           create_protocol=partial(EarlyDataClient, diagnostics=diagnostics),
                           wait_connected=False) as client:
            stream, response = client.request(ready["authority"], "/unsolicited-early-data")
            trace = client._quic._quic_logger
            last_sample = None
            async with asyncio.timeout(5):
                while len(samples) < 5:
                    assert gate.error is None, gate.error
                    assert client.handshake is None and gate.forwarded_server_datagrams == 0
                    assert not response.done.is_set() and client.connection_error is None
                    if gate.forwarded_early_packets and gate.server_datagrams and Path(ready["capturePath"]).exists():
                        usage = read_resources(ready["resourcesPath"])
                        capture = json.loads(Path(ready["capturePath"]).read_text(encoding="utf-8"))
                        stats = json.loads(Path(peer["stats"]).read_text(encoding="utf-8"))
                        assert stats["datagrams"] == 0 and stats["handshakes"] == 0 and stats["requests"] == 0, stats
                        assert not any(event["kind"].startswith(("request", "response")) for event in capture), capture
                        assert usage["local"]["streams"] == 0 and usage["forwardingReservedBytes"] == 0, usage
                        if usage["sampleEndedNanoseconds"] != last_sample:
                            last_sample = usage["sampleEndedNanoseconds"]
                            samples.append({"resources": usage, "capture": capture, "origin": stats,
                                            "forwardedEarlyPackets": gate.forwarded_early_packets,
                                            "forwardedPaddingBytes": gate.forwarded_padding_bytes,
                                            "forwardedPaddedDatagrams": gate.forwarded_padded_datagrams,
                                            "withheldServerDatagrams": gate.server_datagrams})
                    await asyncio.sleep(0.05)
            assert transmitted_stream(trace, "0RTT", stream), "no actual early application STREAM packet sent"
            gate.release()
            await asyncio.wait_for(client.wait_connected(), timeout=5)
            assert client.handshake == {"alpn": "h3", "earlyDataAccepted": False, "sessionResumed": False}
            assert await response_complete(response) == b"origin"
            assert transmitted_stream(trace, "1RTT", stream), "request did not recover under authenticated 1RTT"
            assert await response_complete(client.request(ready["authority"], "/after-early-rejection")[1]) == b"origin"
            assert gate.error is None and client.connection_error is None
            return {"ticketScope": "genuine independent-server ticket, unknown to product",
                    "forwardedEarlyPackets": gate.forwarded_early_packets, "freshPreHandshakeSamples": len(samples),
                    "forwardedPaddingBytes": gate.forwarded_padding_bytes,
                    "forwardedPaddedDatagrams": gate.forwarded_padded_datagrams,
                    "preHandshakeOriginDatagrams": 0, "preHandshakeApplicationCapture": 0,
                    "authenticatedHandshake": client.handshake, "sameConnectionRecovery": True}
    finally:
        transport.close()
        logger.write(artifacts / "client.qlog")
        (artifacts / "pre-handshake-samples.json").write_text(json.dumps(samples, indent=2), encoding="utf-8")
        (artifacts / "client-termination.json").write_text(json.dumps(diagnostics), encoding="utf-8")
        (artifacts / "gate-diagnostics.json").write_text(json.dumps({
            "forwardedEarlyPackets": gate.forwarded_early_packets,
            "forwardedPaddingBytes": gate.forwarded_padding_bytes,
            "forwardedPaddedDatagrams": gate.forwarded_padded_datagrams,
            "withheldServerDatagrams": gate.server_datagrams - gate.forwarded_server_datagrams,
            "error": str(gate.error)[:512] if gate.error is not None else None,
        }), encoding="utf-8")


async def run_case(binary, name, artifacts):
    assert name in CASES
    artifacts.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="swiftmitm-h3-early-data-") as temporary:
        directory = Path(temporary)
        ticket = await foreign_ticket(directory / "foreign")
        ready_path, origin_ready = directory / "host-ready.json", directory / "origin" / "ready.json"
        environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
        host = origin = ready = peer = None
        with (directory / "origin.log").open("wb") as origin_log, (directory / "host.log").open("wb") as host_log:
            try:
                origin = await asyncio.create_subprocess_exec(
                    sys.executable, str(Path(__file__).with_name("peer.py")), "--ready-file", str(origin_ready),
                    stdout=origin_log, stderr=asyncio.subprocess.STDOUT, env=environment
                )
                peer = await readiness(origin_ready, origin)
                config = {"authority": "localhost:443", "destinationAddress": "127.0.0.1",
                          "destinationPort": peer["port"], "originCAPath": peer["certificate"],
                          "readyPath": str(ready_path), "allowInternal": True,
                          "maximumConnections": 128, "maximumPendingHandshakes": 64,
                          "maximumRequestStreamsPerConnection": 128, "maximumRetainedBytes": 67108864}
                config_path = directory / "configuration.json"
                config_path.write_text(json.dumps(config), encoding="utf-8")
                host = await asyncio.create_subprocess_exec(str(binary), "--configuration", str(config_path),
                                                          stdout=host_log, stderr=asyncio.subprocess.STDOUT,
                                                          env=environment)
                ready = await readiness(ready_path, host)
                result = await exercise(ready, peer, ticket, artifacts)
                await shutdown(host)
                assert host.returncode == 0, host.returncode
                result["finalResources"] = read_resources(ready["resourcesPath"], stopped=True)
                stats = json.loads(Path(peer["stats"]).read_text(encoding="utf-8"))
                assert stats["paths"] == ["/unsolicited-early-data", "/after-early-rejection"], stats
                assert stats["requests"] == 2 and stats["handshakes"] == 1 and not stats["earlyDataAccepted"], stats
                capture = json.loads(Path(ready["capturePath"]).read_text(encoding="utf-8"))
                assert [event["path"] for event in capture if event["kind"] == "requestHead"] == stats["paths"], capture
                result["originRequestsAfterAuthenticatedRecovery"] = stats["requests"]
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
                    raise RuntimeError("early-data peer cleanup failed") from failures[0]
