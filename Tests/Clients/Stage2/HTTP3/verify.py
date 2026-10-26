import argparse
import asyncio
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import signal
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

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from client_tls import create_identity


@dataclass
class Response:
    headers: list = field(default_factory=list)
    trailers: list = field(default_factory=list)
    body: bytearray = field(default_factory=bytearray)
    first_data: asyncio.Event = field(default_factory=asyncio.Event)
    done: asyncio.Event = field(default_factory=asyncio.Event)
    error: str | None = None


class Client(QuicConnectionProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.http = H3Connection(self._quic)
        self.responses = {}
        self.alpn = None

    def quic_event_received(self, event):
        if isinstance(event, HandshakeCompleted):
            self.alpn = event.alpn_protocol
            assert not event.early_data_accepted
        elif isinstance(event, ConnectionTerminated):
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
                else:
                    response.headers.extend(received.headers)
            elif isinstance(received, DataReceived):
                if len(response.body) + len(received.data) > 512 * 1024:
                    raise RuntimeError("response exceeds independent peer bound")
                response.body.extend(received.data)
                if received.data:
                    response.first_data.set()
            if received.stream_ended:
                response.done.set()

    def request(self, authority, path, end=True):
        if len(self.responses) >= 64:
            raise RuntimeError("independent client request bound exceeded")
        stream = self._quic.get_next_available_stream_id()
        response = Response()
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
                assert status is not None and int(status) >= 400, "unauthorized route produced successful HTTP"
    except ConnectionError:
        return


async def run_case(binary, name):
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
            origin = await asyncio.create_subprocess_exec(
                sys.executable, str(Path(__file__).with_name("peer.py")), "--ready-file", str(origin_ready),
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
                        assert proxy.returncode != 0, "denied route silently exited successfully"
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
                if name == "streaming":
                    assert stats["alpn"] and set(stats["alpn"]) == {"h3"}
                    assert not stats["earlyDataAccepted"]
                    assert stats["cancelSignals"] > 0
                    assert stats["peerQpackCapacity"] == 0 and stats["peerQpackBlocked"] == 0
                    return dict(report, upstreamALPN=stats["alpn"], originRequests=stats["requests"])
                assert stats["requests"] == 0, stats
                if name == "denied-route":
                    assert stats["datagrams"] == 0, stats
                return {"rejected": True, "originRequests": 0, "originDatagrams": stats["datagrams"]}
            finally:
                if proxy is not None:
                    await shutdown(proxy)
                await shutdown(origin)


async def main(arguments):
    results = {"peer": f"aioquic/{aioquic.__version__}", "checks": {}}
    for name in ("streaming", "changed-authority", "upstream-hostname", "upstream-trust",
                 "downstream-trust", "denied-route"):
        try:
            async with asyncio.timeout(90):
                observation = await run_case(arguments.binary, name)
            results["checks"][name] = {"result": "passed", "observation": observation}
        except (AssertionError, ConnectionError, RuntimeError, TimeoutError, OSError) as error:
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
