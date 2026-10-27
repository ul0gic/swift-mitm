import argparse
import asyncio
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
import hashlib
import json
import os
from pathlib import Path
import signal
import shutil
import ssl
import sys
import tempfile

import aioquic
from aioquic.asyncio.client import connect
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.h3.connection import ErrorCode, H3Connection, Setting
from aioquic.h3.events import DataReceived, HeadersReceived
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import ConnectionTerminated, HandshakeCompleted, StreamReset

from wire import ENCODED_HEADER_LIMIT, FRAGMENT_VALUE, fragmented_headers, oversized_headers_prefix
from flow import (
    FlowLogger, SLOW_BYTES, STREAM_CREDIT, budget_refusal, cancelled_credit_churn, read_metrics, slow_receiver,
)
from lifecycle import goaway_drain, sni_policy

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from client_tls import create_identity


@dataclass
class Response:
    headers: list = field(default_factory=list)
    trailers: list = field(default_factory=list)
    body: bytearray = field(default_factory=bytearray)
    first_data: asyncio.Event = field(default_factory=asyncio.Event)
    trailers_received: asyncio.Event = field(default_factory=asyncio.Event)
    done: asyncio.Event = field(default_factory=asyncio.Event)
    error: str | None = None
    digest: object | None = None
    body_bytes: int = 0


class Client(QuicConnectionProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.http = H3Connection(self._quic)
        self.responses = {}
        self.alpn = None
        self.connection_error = None

    def quic_event_received(self, event):
        if isinstance(event, HandshakeCompleted):
            self.alpn = event.alpn_protocol
            assert not event.early_data_accepted
        elif isinstance(event, ConnectionTerminated):
            self.connection_error = event.error_code
            for response in self.responses.values():
                if not response.done.is_set():
                    response.error = f"connection terminated: {event.error_code}"
                    response.first_data.set()
                    response.done.set()
        elif isinstance(event, StreamReset) and event.stream_id in self.responses:
            response = self.responses[event.stream_id]
            response.error = f"stream reset: {event.error_code}"
            response.first_data.set()
            response.done.set()
        for received in self.http.handle_event(event):
            response = self.responses.get(received.stream_id)
            if response is None:
                raise RuntimeError("response for an unissued request")
            if isinstance(received, HeadersReceived):
                if response.headers:
                    response.trailers.extend(received.headers)
                    response.trailers_received.set()
                else:
                    response.headers.extend(received.headers)
            elif isinstance(received, DataReceived):
                response.body_bytes += len(received.data)
                if response.digest is None:
                    if len(response.body) + len(received.data) > 512 * 1024:
                        raise RuntimeError("response exceeds independent peer bound")
                    response.body.extend(received.data)
                else:
                    if response.body_bytes > SLOW_BYTES:
                        raise RuntimeError("streaming response exceeds independent peer bound")
                    response.digest.update(received.data)
                    response.body.extend(received.data[:max(0, STREAM_CREDIT - len(response.body))])
                if received.data:
                    response.first_data.set()
            if received.stream_ended:
                response.done.set()

    def request(self, authority, path, end=True, stream_body=False):
        if len(self.responses) >= 64:
            raise RuntimeError("independent client request bound exceeded")
        stream = self._quic.get_next_available_stream_id()
        response = Response(digest=hashlib.sha256() if stream_body else None)
        self.responses[stream] = response
        self.http.send_headers(stream, [
            (b":method", b"GET" if end else b"POST"), (b":scheme", b"https"),
            (b":authority", authority.encode("ascii")), (b":path", path.encode("ascii"))
        ], end_stream=end)
        self.transmit()
        return stream, response

    def body(self, stream, value, end):
        self.http.send_data(stream, value, end_stream=end)
        self.transmit()

    def cancel(self, stream):
        self._quic.stop_stream(stream, error_code=ErrorCode.H3_REQUEST_CANCELLED)
        self._quic.reset_stream(stream, error_code=ErrorCode.H3_REQUEST_CANCELLED)
        self.transmit()

    def trailers(self, stream, sequence):
        self.http.send_headers(stream, [
            (b"x-request-trailer", str(sequence).encode("ascii"))
        ], end_stream=False)
        self.transmit()

    def finish(self, stream):
        self._quic.send_stream_data(stream, b"", end_stream=True)
        self.transmit()

    def raw_request(self):
        if len(self.responses) >= 64:
            raise RuntimeError("independent client request bound exceeded")
        stream = self._quic.get_next_available_stream_id()
        response = Response()
        self.responses[stream] = response
        return stream, response


async def response_complete(response):
    await asyncio.wait_for(response.done.wait(), timeout=15)
    if response.error:
        raise AssertionError(response.error)
    assert (b":status", b"200") in response.headers, response.headers
    return bytes(response.body)


async def readiness(path, process):
    async with asyncio.timeout(20):
        while not path.exists():
            if process.returncode is not None:
                raise RuntimeError(f"process exited before readiness: {process.returncode}")
            await asyncio.sleep(0.02)
    return json.loads(path.read_text(encoding="utf-8"))


async def shutdown(process):
    if process.returncode is None:
        process.send_signal(signal.SIGTERM)
        try:
            await asyncio.wait_for(process.wait(), timeout=10)
        except TimeoutError:
            process.kill()
            await process.wait()
            raise AssertionError("process ignored graceful shutdown deadline")


def configuration(certificate, hostname):
    value = QuicConfiguration(is_client=True, alpn_protocols=["h3"], server_name=hostname,
                              verify_mode=ssl.CERT_REQUIRED, idle_timeout=10)
    value.load_verify_locations(cafile=str(certificate))
    return value


async def positive(port, authority, certificate):
    async with connect("127.0.0.1", port, configuration=configuration(certificate, "localhost"),
                       create_protocol=Client) as client:
        assert client.alpn == "h3"
        stream, duplex = client.request(authority, "/duplex", end=False)
        await asyncio.wait_for(duplex.first_data.wait(), timeout=10)
        assert bytes(duplex.body) == b"early:" and not duplex.done.is_set()
        client.body(stream, b"upload-before-eof", end=False)
        async with asyncio.timeout(10):
            while bytes(duplex.body) != b"early:upload-before-eof":
                if duplex.error or duplex.done.is_set():
                    raise AssertionError("duplex response ended before upload EOF")
                await asyncio.sleep(0.01)
        client.body(stream, b":tail", end=True)
        assert await response_complete(duplex) == b"early:upload-before-eof:tail"
        assert [v for k, v in duplex.headers if k == b"x-duplicate"] == [b"first", b"second"]
        assert [v for k, v in duplex.trailers if k == b"x-trailer"] == [b"first", b"second"]
        siblings = [client.request(authority, f"/sibling-{index}")[1] for index in range(32)]
        assert await asyncio.gather(*(response_complete(item) for item in siblings)) == [b"origin"] * 32
        held_stream, held = client.request(authority, "/hold", end=False)
        await asyncio.wait_for(held.first_data.wait(), timeout=10)
        assert bytes(held.body) == b"held"
        client.cancel(held_stream)
        assert await response_complete(client.request(authority, "/after-cancel")[1]) == b"origin"
        assert await response_complete(client.request(authority, "/large")[1]) == bytes(range(256)) * 1024
        settings = client.http.received_settings
        assert settings is not None
        assert settings.get(Setting.QPACK_MAX_TABLE_CAPACITY, 0) == 0
        assert settings.get(Setting.QPACK_BLOCKED_STREAMS, 0) == 0
    return {"downstreamALPN": "h3", "earlyDuplex": True, "parallelStreams": 32,
            "orderedFieldsAndTrailers": True, "cancelSibling": True, "largeBodyBytes": 262144}


async def rejected(port, authority, certificate, hostname):
    try:
        async with asyncio.timeout(15):
            async with connect("127.0.0.1", port, configuration=configuration(certificate, hostname),
                               create_protocol=Client) as client:
                response = client.request(authority, "/must-not-arrive")[1]
                await response.done.wait()
                if response.error:
                    return
                status = next((v for k, v in response.headers if k == b":status"), None)
                assert status is not None, "rejected route ended without an explicit stream error or HTTP status"
                assert int(status) >= 400, "unauthorized route produced successful HTTP"
    except ConnectionError:
        return


async def separated_fin(port, authority, certificate, peer):
    stats_file = Path(peer["stats"])
    release_file = Path(peer["releaseFin"])
    async with connect("127.0.0.1", port, configuration=configuration(certificate, "localhost"),
                       create_protocol=Client) as client:
        assert client.alpn == "h3"
        # Cross the spike's 128 initial bidirectional stream-credit boundary.
        for sequence in range(129):
            stream, response = client.request(authority, f"/split-fin/{sequence}", end=False)
            client.trailers(stream, sequence)
            async with asyncio.timeout(10):
                while sequence not in json.loads(stats_file.read_text())["splitRequestTrailers"]:
                    if response.error:
                        raise AssertionError(response.error)
                    await asyncio.sleep(0.01)
            # End-to-end trailer receipt proves the earlier HEADERS left the sender.
            client.finish(stream)
            await asyncio.wait_for(response.trailers_received.wait(), timeout=10)
            assert response.trailers == [(b"x-response-trailer", str(sequence).encode("ascii"))]
            temporary = release_file.with_suffix(".tmp")
            temporary.write_text(str(sequence))
            temporary.replace(release_file)
            assert await response_complete(response) == b"split-response"
            async with asyncio.timeout(10):
                while sequence not in json.loads(stats_file.read_text())["splitResponseFin"]:
                    await asyncio.sleep(0.01)
            del client.responses[stream]
        assert await response_complete(client.request(authority, "/after-split-fin")[1]) == b"origin"
    return {"separatedRequestAndResponseFIN": True, "completedExchanges": 129,
            "initialStreamCredit": 128, "sameConnectionSubsequentStream": True}


async def hostile_headers(port, authority, certificate, peer, upstream):
    async with connect("127.0.0.1", port, configuration=configuration(certificate, "localhost"),
                       create_protocol=Client) as client:
        if upstream:
            _, response = client.request(authority, "/oversized-response-headers")
        else:
            stream, response = client.raw_request()
            client._quic.send_stream_data(stream, oversized_headers_prefix(), end_stream=False)
            client.transmit()
        await asyncio.wait_for(response.done.wait(), timeout=10)
        assert response.error, "oversized incomplete HEADERS did not explicitly fail"
        assert not response.headers and not response.body, "oversized HEADERS reached HTTP delivery"
        if not upstream:
            assert client.connection_error == ErrorCode.H3_EXCESSIVE_LOAD, client.connection_error
    if upstream:
        async with asyncio.timeout(10):
            while ErrorCode.H3_EXCESSIVE_LOAD not in json.loads(Path(peer["stats"]).read_text())["connectionErrors"]:
                await asyncio.sleep(0.01)
    else:
        assert json.loads(Path(peer["stats"]).read_text())["requests"] == 0
    async with connect("127.0.0.1", port, configuration=configuration(certificate, "localhost"),
                       create_protocol=Client) as fresh:
        assert await response_complete(fresh.request(authority, "/after-oversized-headers")[1]) == b"origin"
    return {"direction": "response" if upstream else "request", "declaredEncodedBytes": ENCODED_HEADER_LIMIT + 1,
            "payloadBytesSent": 0, "sentFIN": False, "rejectedBeforePayload": True,
            "excessiveLoadCode": int(ErrorCode.H3_EXCESSIVE_LOAD), "freshConnectionProgress": True}


async def fragmented_progress(port, authority, certificate):
    async with connect("127.0.0.1", port, configuration=configuration(certificate, "localhost"),
                       create_protocol=Client) as client:
        stream, response = client.raw_request()
        count = await fragmented_headers(client, stream, [
            (b":method", b"GET"), (b":scheme", b"https"), (b":authority", authority.encode("ascii")),
            (b":path", b"/fragmented-headers"), (b"x-fragment", FRAGMENT_VALUE)
        ], end_stream=True)
        assert await response_complete(response) == b"fragmented"
        assert [value for name, value in response.headers if name == b"x-fragment"] == [FRAGMENT_VALUE]
        assert await response_complete(client.request(authority, "/after-fragmented-headers")[1]) == b"origin"
    return {"requestEncodedBytes": count, "decodedSyntheticValueBytes": len(FRAGMENT_VALUE),
            "fragmentBoundaries": ["after-type", "after-length", "mid-payload"],
            "acknowledgedBarriersEachDirection": 3, "sameConnectionSubsequentStream": True}


def retain_tail(source, destination):
    if not source.exists():
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    with source.open("rb") as stream:
        stream.seek(0, os.SEEK_END)
        stream.seek(max(0, stream.tell() - 65536))
        destination.write_bytes(stream.read(65536))


def retain_flow_trace(source, destination):
    if source.exists():
        assert source.stat().st_size <= 32 * 1024 * 1024, "independent qlog artifact bound exceeded"
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)


async def credit_stall(ready, authority, certificate, peer, artifacts, churn=False):
    logger = FlowLogger()
    config = configuration(certificate, "localhost")
    config.max_stream_data = STREAM_CREDIT
    config.quic_logger = logger
    try:
        async with connect("127.0.0.1", ready["port"], configuration=config, create_protocol=Client) as client:
            assert client.alpn == "h3"
            if churn:
                return await cancelled_credit_churn(client, authority, Path(ready["metricsPath"]), response_complete,
                                                    artifacts / "churn-samples.json")
            return await slow_receiver(client, authority, peer, Path(ready["metricsPath"]), response_complete,
                                       artifacts / "stall-samples.json")
    finally:
        logger.write(artifacts / "client.qlog")


async def storage_refusal(ready, authority, certificate, artifacts):
    async with AsyncExitStack() as stack:
        clients = []
        for _ in range(4):
            client = await stack.enter_async_context(connect(
                "127.0.0.1", ready["port"], configuration=configuration(certificate, "localhost"), create_protocol=Client
            ))
            assert client.alpn == "h3"
            clients.append(client)
        return await budget_refusal(clients, authority, Path(ready["metricsPath"]), response_complete,
                                    artifacts / "admission-samples.json")


async def run_case(binary, name, artifacts):
    with tempfile.TemporaryDirectory(prefix="swiftmitm-h3-") as temporary:
        directory = Path(temporary)
        ca_directory = directory / "ca"
        ca_directory.mkdir()
        certificate, key = create_identity(ca_directory)
        wrong_directory = directory / "wrong-ca"
        wrong_directory.mkdir()
        wrong_certificate, _ = create_identity(wrong_directory)
        origin_ready = directory / "origin" / "ready.json"
        environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
        with (directory / "origin.log").open("wb") as origin_log, (directory / "proxy.log").open("wb") as proxy_log:
            origin_arguments = [sys.executable, str(Path(__file__).with_name("peer.py")),
                                "--ready-file", str(origin_ready)]
            if name == "slow-receiver":
                origin_arguments.append("--flow-proof")
            origin = await asyncio.create_subprocess_exec(
                *origin_arguments,
                stdout=origin_log, stderr=asyncio.subprocess.STDOUT, env=environment
            )
            proxy = None
            try:
                peer = await readiness(origin_ready, origin)
                hostname = "wrong-host.invalid" if name == "upstream-hostname" else "localhost"
                authority = f"{hostname}:{peer['port']}"
                proxy_ready = directory / "proxy-ready.json"
                arguments = [str(binary), "serve", "--ca-cert", str(certificate), "--ca-key", str(key),
                             "--hostname", hostname, "--origin-address", "127.0.0.1", "--origin-port", str(peer["port"]),
                             "--origin-ca", str(wrong_certificate if name == "upstream-trust" else peer["certificate"]),
                             "--port", "0", "--ready-file", str(proxy_ready)]
                if name != "denied-route":
                    arguments.append("--allow-internal")
                proxy = await asyncio.create_subprocess_exec(
                    *arguments, stdout=proxy_log, stderr=asyncio.subprocess.STDOUT, env=environment
                )
                if name == "denied-route":
                    async with asyncio.timeout(20):
                        while not proxy_ready.exists() and proxy.returncode is None:
                            await asyncio.sleep(0.02)
                    if proxy.returncode is not None:
                        assert proxy.returncode == 2, f"denied route exited unexpectedly: {proxy.returncode}"
                        with (directory / "proxy.log").open("rb") as diagnostic:
                            assert b"HTTP3TransportProbe: denied-route" in diagnostic.read(65536).splitlines(), \
                                "startup failed without the typed route-denial diagnostic"
                    else:
                        ready = json.loads(proxy_ready.read_text())
                        await rejected(ready["port"], authority, certificate, hostname)
                else:
                    ready = await readiness(proxy_ready, proxy)
                    if name == "streaming":
                        report = await positive(ready["port"], authority, certificate)
                        async with asyncio.timeout(10):
                            while json.loads(Path(peer["stats"]).read_text())["cancelSignals"] == 0:
                                await asyncio.sleep(0.02)
                    elif name == "separated-fin":
                        report = await separated_fin(ready["port"], authority, certificate, peer)
                    elif name in ("oversized-request-headers", "oversized-response-headers"):
                        report = await hostile_headers(ready["port"], authority, certificate, peer,
                                                       upstream=name == "oversized-response-headers")
                    elif name == "fragmented-headers":
                        report = await fragmented_progress(ready["port"], authority, certificate)
                    elif name == "slow-receiver":
                        report = await credit_stall(ready, authority, certificate, peer, artifacts / name)
                    elif name == "cancelled-credit-churn":
                        report = await credit_stall(ready, authority, certificate, peer, artifacts / name, churn=True)
                    elif name == "storage-refusal":
                        report = await storage_refusal(ready, authority, certificate, artifacts / name)
                    elif name == "sni-policy":
                        report = await sni_policy(ready, authority, certificate, peer, artifacts / name,
                                                  Client, configuration, response_complete)
                    elif name == "goaway-drain":
                        report = await goaway_drain(ready, authority, certificate, peer, Client,
                                                   configuration, response_complete)
                    elif name == "changed-authority":
                        await rejected(ready["port"], f"other.invalid:{peer['port']}", certificate, hostname)
                    elif name == "downstream-trust":
                        await rejected(ready["port"], authority, wrong_certificate, hostname)
                    else:
                        await rejected(ready["port"], authority, certificate, hostname)
                await shutdown(proxy)
                if proxy_ready.exists():
                    assert proxy.returncode == 0, "ready proxy failed during the scenario or shutdown"
                stats = json.loads(Path(peer["stats"]).read_text())
                if name in ("sni-policy", "goaway-drain"):
                    final_metrics = read_metrics(Path(ready["metricsPath"]), stopped=True)
                    expected = 3 if name == "sni-policy" else 2
                    assert stats["requests"] == expected and stats["handshakes"] == expected, stats
                    assert stats["alpn"] == ["h3"] * expected, stats
                    assert final_metrics["peakConnectionStorageLifetimes"] > 0, final_metrics
                    return dict(report, finalMetrics=final_metrics, originRequests=stats["requests"])
                if name in ("slow-receiver", "cancelled-credit-churn", "storage-refusal"):
                    final_metrics = read_metrics(Path(ready["metricsPath"]), stopped=True)
                    if name == "slow-receiver":
                        assert stats["requests"] == 5, stats
                        assert stats["handshakes"] == 1 and stats["alpn"] == ["h3"], stats
                    elif name == "cancelled-credit-churn":
                        assert stats["requests"] == 65, stats
                        assert stats["handshakes"] == 1 and stats["alpn"] == ["h3"], stats
                        assert stats["cancelSignals"] >= 32, stats
                    else:
                        assert stats["handshakes"] == 4 and stats["alpn"] == ["h3"] * 4, stats
                    return dict(report, finalMetrics=final_metrics, originRequests=stats["requests"])
                if name == "streaming":
                    assert stats["alpn"] and set(stats["alpn"]) == {"h3"}
                    assert not stats["earlyDataAccepted"]
                    assert stats["cancelSignals"] > 0
                    assert stats["peerQpackCapacity"] == 0 and stats["peerQpackBlocked"] == 0
                    return dict(report, upstreamALPN=stats["alpn"], originRequests=stats["requests"])
                if name == "separated-fin":
                    assert stats["splitRequestTrailers"] == list(range(129)), stats
                    assert stats["splitResponseFin"] == list(range(129)), stats
                    assert stats["alpn"] and set(stats["alpn"]) == {"h3"}
                    assert stats["requests"] == 130, stats
                    return dict(report, upstreamALPN=stats["alpn"], originRequests=stats["requests"])
                if name in ("oversized-request-headers", "oversized-response-headers", "fragmented-headers"):
                    expected = 1 if name == "oversized-request-headers" else 2
                    assert stats["requests"] == expected, stats
                    assert stats["alpn"] and set(stats["alpn"]) == {"h3"}
                    if name == "fragmented-headers":
                        assert stats["fragmentedRequest"]
                        assert 16384 < stats["fragmentedResponseBytes"] <= ENCODED_HEADER_LIMIT
                        report["responseEncodedBytes"] = stats["fragmentedResponseBytes"]
                    return dict(report, upstreamALPN=stats["alpn"], originRequests=stats["requests"])
                assert stats["requests"] == 0, stats
                if name == "denied-route":
                    assert stats["datagrams"] == 0, stats
                return {"rejected": True, "originRequests": 0, "originDatagrams": stats["datagrams"]}
            finally:
                try:
                    if proxy is not None:
                        await shutdown(proxy)
                finally:
                    try:
                        await shutdown(origin)
                    finally:
                        retain_tail(directory / "origin.log", artifacts / name / "origin.log")
                        retain_tail(directory / "proxy.log", artifacts / name / "proxy.log")
                        retain_tail(origin_ready.parent / "stats.json", artifacts / name / "origin-stats.json")
                        retain_tail(directory / "proxy-ready.json.metrics.json", artifacts / name / "final-metrics.json")
                        retain_flow_trace(origin_ready.parent / "flow.qlog", artifacts / name / "origin.qlog")


async def main(arguments):
    results = {"peer": f"aioquic/{aioquic.__version__}", "checks": {}}
    artifacts = arguments.output.parent / "http3-peer-logs"
    for name in ("streaming", "changed-authority", "upstream-hostname", "upstream-trust",
                 "downstream-trust", "denied-route", "separated-fin", "oversized-request-headers",
                 "oversized-response-headers", "fragmented-headers", "slow-receiver",
                 "cancelled-credit-churn", "storage-refusal", "sni-policy", "goaway-drain"):
        try:
            async with asyncio.timeout(90):
                observation = await run_case(arguments.binary, name, artifacts)
            results["checks"][name] = {"result": "passed", "observation": observation}
        except (AssertionError, ConnectionError, RuntimeError, TimeoutError, OSError, KeyError, json.JSONDecodeError) as error:
            results["checks"][name] = {"result": "failed", "error": f"{type(error).__name__}: {error}"}
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(json.dumps(results))
    return 0 if all(item["result"] == "passed" for item in results["checks"].values()) else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    sys.exit(asyncio.run(main(parser.parse_args())))
