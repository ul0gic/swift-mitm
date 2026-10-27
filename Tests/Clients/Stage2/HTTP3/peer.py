import argparse
import asyncio
from functools import partial
import json
from pathlib import Path
import signal
import sys

from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.asyncio.server import QuicServer
from aioquic.buffer import encode_uint_var
from aioquic.h3.connection import FrameType, H3Connection, Setting, encode_frame
from aioquic.h3.events import DataReceived, HeadersReceived
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import ConnectionTerminated, HandshakeCompleted, StopSendingReceived, StreamReset

from wire import FRAGMENT_VALUE, fragmented_headers, oversized_headers_prefix
from flow import FlowLogger, SLOW_BYTES, SLOW_CHUNK
from fidelity import OCTET_FIELDS, RESPONSE_BODY, field_hex, octet_headers

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from client_tls import create_identity


class Origin(QuicConnectionProtocol):
    def __init__(self, *args, stats, publish, release_file, flow_proof, **kwargs):
        super().__init__(*args, **kwargs)
        self.http = H3Connection(self._quic)
        self.stats = stats
        self.publish = publish
        self.paths = {}
        self.release_file = release_file
        self.fin_tasks = set()
        self.flow_task = None
        self.slow_stream = None
        if flow_proof:
            self.flow_task = asyncio.create_task(self.sample_flow())

    async def sample_flow(self):
        while True:
            if self.slow_stream is not None:
                self.stats["slowResponseSentOffset"] = self._quic._quic_logger.sent_offsets.get(self.slow_stream, 0)
                self.stats["flowSamples"] += 1
                self.publish()
            await asyncio.sleep(0.1)

    def quic_event_received(self, event):
        if isinstance(event, HandshakeCompleted):
            self.stats["handshakes"] += 1
            self.stats["alpn"].append(event.alpn_protocol)
            self.stats["earlyDataAccepted"] |= event.early_data_accepted
        elif isinstance(event, (StreamReset, StopSendingReceived)):
            self.stats["cancelSignals"] += 1
            self.paths.pop(event.stream_id, None)
        elif isinstance(event, ConnectionTerminated):
            self.stats["connectionErrors"].append(event.error_code)
            if self.flow_task is not None:
                self.flow_task.cancel()
        for received in self.http.handle_event(event):
            if isinstance(received, HeadersReceived):
                self.headers(received)
            elif isinstance(received, DataReceived):
                self.data(received)
        settings = self.http.received_settings
        if settings is not None:
            self.stats["peerQpackCapacity"] = settings.get(Setting.QPACK_MAX_TABLE_CAPACITY, 0)
            self.stats["peerQpackBlocked"] = settings.get(Setting.QPACK_BLOCKED_STREAMS, 0)
        self.publish()
        self.transmit()

    def headers(self, event):
        values = dict(event.headers)
        path = values.get(b":path")
        if path is None:
            path = self.paths.get(event.stream_id, b"")
            if path == b"/octet-fidelity":
                self.stats["octetTrailers"] = field_hex(event.headers)
                self._quic.send_stream_data(event.stream_id, octet_headers(OCTET_FIELDS), end_stream=True)
                self.paths.pop(event.stream_id)
            if path.startswith(b"/split-fin/"):
                sequence = int(path.rsplit(b"/", 1)[1])
                assert event.headers == [(b"x-request-trailer", str(sequence).encode("ascii"))]
                self.stats["splitRequestTrailers"].append(sequence)
                self.http.send_headers(event.stream_id, [
                    (b"x-response-trailer", str(sequence).encode("ascii"))
                ], end_stream=False)
                task = asyncio.create_task(self.finish_split_response(event.stream_id, sequence))
                self.fin_tasks.add(task)
                task.add_done_callback(self.fin_tasks.discard)
            return
        if len(self.paths) >= 64:
            raise RuntimeError("independent origin request bound exceeded")
        self.paths[event.stream_id] = path
        self.stats["requests"] += 1
        self.stats["paths"].append(path.decode("ascii"))
        if path == b"/octet-fidelity":
            self.stats["octetHeaders"] = field_hex([(name, value) for name, value in event.headers
                                                    if not name.startswith(b":")])
            for status in (b"103", b"200"):
                self._quic.send_stream_data(event.stream_id,
                                           octet_headers([(b":status", status)] + OCTET_FIELDS), end_stream=False)
            self._quic.send_stream_data(event.stream_id, encode_frame(0, RESPONSE_BODY), end_stream=False)
            return
        if path == b"/oversized-response-headers":
            self._quic.send_stream_data(event.stream_id, oversized_headers_prefix(), end_stream=False)
            return
        if path == b"/fragmented-headers":
            assert values.get(b"x-fragment") == FRAGMENT_VALUE
            self.stats["fragmentedRequest"] = True
            task = asyncio.create_task(self.fragmented_response(event.stream_id))
            self.fin_tasks.add(task)
            task.add_done_callback(self.fin_tasks.discard)
            return
        self.http.send_headers(event.stream_id, [
            (b":status", b"200"), (b"x-duplicate", b"first"), (b"x-duplicate", b"second")
        ])
        if path in (b"/duplex", b"/goaway-drain"):
            self.http.send_data(event.stream_id, b"early:", end_stream=False)
            if path == b"/goaway-drain":
                assert event.stream_id == 0
                for stream_id in (8, 4, 4):
                    self._quic.send_stream_data(self.http._local_control_stream_id,
                                                encode_frame(FrameType.GOAWAY, encode_uint_var(stream_id)))
                    self.stats["goawayIDs"].append(stream_id)
        elif path == b"/hold":
            self.http.send_data(event.stream_id, b"held", end_stream=False)
        elif path == b"/large":
            self.http.send_data(event.stream_id, bytes(range(256)) * 1024, end_stream=True)
            self.paths.pop(event.stream_id)
        elif path == b"/slow-receiver":
            self.slow_stream = event.stream_id
            for offset in range(0, SLOW_BYTES, len(SLOW_CHUNK)):
                self.http.send_data(event.stream_id, SLOW_CHUNK, end_stream=offset + len(SLOW_CHUNK) == SLOW_BYTES)
            self.paths.pop(event.stream_id)
        elif path.startswith(b"/split-fin/"):
            self.http.send_data(event.stream_id, b"split-response", end_stream=False)
        else:
            self.http.send_data(event.stream_id, b"origin", end_stream=True)
            self.paths.pop(event.stream_id)

    async def fragmented_response(self, stream):
        count = await fragmented_headers(self, stream, [
            (b":status", b"200"), (b"x-fragment", FRAGMENT_VALUE)
        ], end_stream=False)
        self._quic.send_stream_data(stream, encode_frame(0, b"fragmented"), end_stream=True)
        self.transmit()
        self.stats["fragmentedResponseBytes"] = count
        self.paths.pop(stream, None)
        self.publish()

    async def finish_split_response(self, stream, sequence):
        # The driver releases this only after the client received these trailers.
        async with asyncio.timeout(15):
            while not self.release_file.exists() or int(self.release_file.read_text()) < sequence:
                await asyncio.sleep(0.01)
        self._quic.send_stream_data(stream, b"", end_stream=True)
        self.transmit()
        self.stats["splitResponseFin"].append(sequence)
        self.paths.pop(stream, None)
        self.publish()

    def data(self, event):
        path = self.paths.get(event.stream_id)
        self.stats["uploadBytes"] += len(event.data)
        if path == b"/octet-fidelity":
            assert len(self.stats["octetBody"]) + len(event.data) * 2 <= 128
            self.stats["octetBody"] += event.data.hex()
        if path in (b"/duplex", b"/goaway-drain"):
            self.http.send_data(event.stream_id, event.data, end_stream=False)
            if event.stream_ended:
                self.http.send_headers(event.stream_id, [
                    (b"x-trailer", b"first"), (b"x-trailer", b"second")
                ], end_stream=True)
                self.paths.pop(event.stream_id)


class CountingServer(QuicServer):
    def __init__(self, *args, stats, publish, **kwargs):
        super().__init__(*args, **kwargs)
        self.stats = stats
        self.publish = publish

    def datagram_received(self, data, address):
        self.stats["datagrams"] += 1
        self.publish()
        super().datagram_received(data, address)


async def serve(arguments, origin_type=Origin):
    directory = arguments.ready_file.parent
    directory.mkdir(parents=True, exist_ok=True)
    certificate, key = create_identity(directory)
    stats = {"datagrams": 0, "handshakes": 0, "alpn": [], "earlyDataAccepted": False,
             "requests": 0, "paths": [], "uploadBytes": 0, "cancelSignals": 0,
             "peerQpackCapacity": None, "peerQpackBlocked": None,
             "splitRequestTrailers": [], "splitResponseFin": [], "connectionErrors": [],
             "fragmentedRequest": False, "fragmentedResponseBytes": 0,
             "slowResponseSentOffset": 0, "flowSamples": 0, "goawayIDs": [],
             "octetHeaders": [], "octetTrailers": [], "octetBody": ""}
    stats_file = directory / "stats.json"
    release_file = directory / "release-fin.txt"

    def publish():
        temporary = stats_file.with_suffix(".tmp")
        temporary.write_text(json.dumps(stats), encoding="utf-8")
        temporary.replace(stats_file)

    configuration = QuicConfiguration(is_client=False, alpn_protocols=["h3"], idle_timeout=15)
    flow_logger = FlowLogger() if arguments.flow_proof else None
    configuration.quic_logger = flow_logger
    configuration.load_cert_chain(certificate, key)
    loop = asyncio.get_running_loop()
    transport, server = await loop.create_datagram_endpoint(
        lambda: CountingServer(
            configuration=configuration, stats=stats, publish=publish,
            create_protocol=partial(origin_type, stats=stats, publish=publish, release_file=release_file,
                                    flow_proof=arguments.flow_proof)
        ), local_addr=("127.0.0.1", 0)
    )
    stopped = asyncio.Event()
    for name in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(name, stopped.set)
    publish()
    arguments.ready_file.write_text(json.dumps({
        "port": transport.get_extra_info("sockname")[1], "hostname": "localhost",
        "certificate": str(certificate), "key": str(key), "stats": str(stats_file),
        "releaseFin": str(release_file)
    }), encoding="utf-8")
    try:
        await asyncio.wait_for(stopped.wait(), timeout=180)
    finally:
        server.close()
        transport.close()
        publish()
        if flow_logger is not None:
            flow_logger.write(directory / "flow.qlog")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ready-file", type=Path, required=True)
    parser.add_argument("--flow-proof", action="store_true")
    asyncio.run(serve(parser.parse_args()))
