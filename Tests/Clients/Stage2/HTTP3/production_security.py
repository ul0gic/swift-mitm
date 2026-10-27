import argparse
import asyncio
from contextlib import AsyncExitStack
import json
import os
from pathlib import Path
import sys
import tempfile

from aioquic.asyncio.client import connect
from aioquic.buffer import encode_uint_var
from aioquic.h3.connection import ErrorCode, FrameType, encode_frame

from fidelity import field_hex, octet_headers
from peer import Origin, serve
from production_resources import read_resources
from verify import Client, configuration, readiness, response_complete, retain_tail, shutdown


SECURITY_CASES = (
    "cross-client-pooling", "mapped-route-isolation", "mapped-route-trust",
    "upstream-invalid-goaway", "upstream-increasing-goaway", "downstream-increasing-goaway",
    "downstream-goaway-drain", "unsupported-connect", "unsupported-webtransport",
)
CREDENTIALS = [(b"authorization", b"synthetic-bearer"), (b"proxy-authorization", b"synthetic-proxy"),
               (b"cookie", b"synthetic=cookie"), (b"x-private", b"synthetic-private")]


class SecurityOrigin(Origin):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.connection_id = self.stats.setdefault("securityConnections", 0)
        self.stats["securityConnections"] += 1
        self.stats.setdefault("securityRequests", [])

    def headers(self, event):
        fields = dict(event.headers)
        path = fields.get(b":path")
        if path is None:
            return super().headers(event)
        assert len(self.stats["securityRequests"]) < 64
        self.stats["securityRequests"].append({
            "connection": self.connection_id, "stream": event.stream_id,
            "path": path.decode("ascii"), "headers": field_hex(event.headers),
        })
        if path in (b"/security/invalid-goaway", b"/security/increasing-goaway"):
            self.stats["requests"] += 1
            self.stats["paths"].append(path.decode("ascii"))
            identifiers = (1,) if path == b"/security/invalid-goaway" else (4, 8)
            for identifier in identifiers:
                self._quic.send_stream_data(self.http._local_control_stream_id,
                                           encode_frame(FrameType.GOAWAY, encode_uint_var(identifier)))
                self.stats["goawayIDs"].append(identifier)
            return
        if path == b"/production/security-map":
            self.stats["requests"] += 1
            self.stats["paths"].append(path.decode("ascii"))
            self.http.send_headers(event.stream_id, [(b":status", b"200")])
            self.http.send_data(event.stream_id, b"mapped", end_stream=True)
            return
        super().headers(event)


def read_stats(peer):
    return json.loads(Path(peer["stats"]).read_text(encoding="utf-8"))


async def await_stats(peer, predicate):
    async with asyncio.timeout(10):
        while True:
            value = read_stats(peer)
            if predicate(value):
                return value
            await asyncio.sleep(0.05)


def send_headers(client, authority, path, credentials=()):
    stream, response = client.raw_request()
    fields = [(b":method", b"GET"), (b":scheme", b"https"),
              (b":authority", authority.encode("ascii")), (b":path", path.encode("ascii"))]
    client.http.send_headers(stream, fields + list(credentials), end_stream=True)
    client.transmit()
    return response


async def rejection(response):
    await asyncio.wait_for(response.done.wait(), timeout=15)
    assert response.error, "unsupported or unauthorized exchange completed without a transport rejection"
    assert not response.headers and not response.body, "rejected exchange exposed a fabricated response"
    assert response.error.startswith(("stream reset: ", "connection terminated: ")), response.error
    code = int(response.error.rsplit(": ", 1)[1])
    assert 0x101 <= code <= 0x3ff, f"expected protocol rejection, received {code}"
    return response.error


async def cross_client_pooling(ready, authority, certificate, peer):
    async with AsyncExitStack() as stack:
        clients = [await stack.enter_async_context(connect(
            "127.0.0.1", ready["port"], configuration=configuration(certificate, "localhost"), create_protocol=Client
        )) for _ in range(2)]
        for index, client in enumerate(clients):
            for request in range(2):
                path = f"/security/client-{index}/request-{request}"
                assert await response_complete(client.request(authority, path)[1]) == b"origin"
        stats = await await_stats(peer, lambda value: value["requests"] == 4)
        grouped = [{entry["connection"] for entry in stats["securityRequests"]
                    if entry["path"].startswith(f"/security/client-{index}/")} for index in range(2)]
        assert all(len(group) == 1 for group in grouped) and grouped[0].isdisjoint(grouped[1]), grouped
        assert stats["handshakes"] == 2, stats
    return {"downstreamClients": 2, "requestsPerClient": 2, "originConnections": [sorted(group) for group in grouped],
            "sameClientReuse": True, "crossClientReuse": False}


async def mapped_route(name, ready, authority, certificate, peer, mapped):
    async with connect("127.0.0.1", ready["port"], configuration=configuration(certificate, "localhost"),
                       create_protocol=Client) as client:
        stream, held = client.request(authority, "/duplex", end=False)
        await asyncio.wait_for(held.first_data.wait(), timeout=10)
        assert bytes(held.body) == b"early:" and not held.done.is_set()
        response = send_headers(client, authority, "/production/security-map", CREDENTIALS)
        if name == "mapped-route-trust":
            error = await rejection(response)
            assert read_stats(mapped)["requests"] == 0
        else:
            assert await response_complete(response) == b"mapped"
            mapped_stats = await await_stats(mapped, lambda value: value["requests"] == 1)
            record = mapped_stats["securityRequests"][0]
            fields = [(bytes.fromhex(key), bytes.fromhex(value)) for key, value in record["headers"]]
            assert (b":authority", f"127.0.0.1:{mapped['port']}".encode("ascii")) in fields, fields
            assert not {key for key, _ in fields}.intersection(key for key, _ in CREDENTIALS), fields
            error = None
        client.body(stream, b"fixed-route-survives", end=True)
        assert await response_complete(held) == b"early:fixed-route-survives"
        assert await response_complete(send_headers(client, authority, "/security/fixed-after-map", CREDENTIALS)) == b"origin"
        fixed = await await_stats(peer, lambda value: value["requests"] == 2)
        assert fixed["handshakes"] == 1 and fixed["paths"] == ["/duplex", "/security/fixed-after-map"], fixed
        fields = [(bytes.fromhex(key), bytes.fromhex(value)) for key, value in fixed["securityRequests"][-1]["headers"]]
        assert all(pair in fields for pair in CREDENTIALS if pair[0] != b"proxy-authorization"), fields
        assert (b":authority", authority.encode("ascii")) in fields, fields
    return {"mappedTrustRejected": error, "mappedRequests": read_stats(mapped)["requests"],
            "fixedRequests": 2, "unchangedSiblingCompleted": True, "fixedSessionRetained": True,
            "crossOriginCredentialsStripped": name == "mapped-route-isolation"}


async def goaway_case(name, ready, authority, certificate, peer):
    async with connect("127.0.0.1", ready["port"], configuration=configuration(certificate, "localhost"),
                       create_protocol=Client) as client:
        if name.startswith("upstream-"):
            path = "/security/invalid-goaway" if name == "upstream-invalid-goaway" else "/security/increasing-goaway"
            _, response = client.request(authority, path)
            await rejection(response)
            stats = await await_stats(peer, lambda value: int(ErrorCode.H3_ID_ERROR) in value["connectionErrors"])
            assert stats["requests"] == 1, stats
            return {"sentGOAWAYIDs": stats["goawayIDs"], "originObservedConnectionError": int(ErrorCode.H3_ID_ERROR),
                    "automaticRetry": False}
        stream, response = client.request(authority, "/duplex", end=False)
        await asyncio.wait_for(response.first_data.wait(), timeout=10)
        assert bytes(response.body) == b"early:" and not response.done.is_set()
        initial = read_resources(ready["resourcesPath"])["receivedGoaways"]
        identifiers = (0, 1) if name == "downstream-increasing-goaway" else (0,)
        for identifier in identifiers:
            client._quic.send_stream_data(client.http._local_control_stream_id,
                                         encode_frame(FrameType.GOAWAY, encode_uint_var(identifier)))
        client.transmit()
        if name == "downstream-increasing-goaway":
            async with asyncio.timeout(10):
                while client.connection_error is None:
                    await asyncio.sleep(0.05)
            assert client.connection_error == int(ErrorCode.H3_ID_ERROR), client.connection_error
            return {"sentGOAWAYIDs": list(identifiers), "downstreamObservedConnectionError": client.connection_error}
        async with asyncio.timeout(10):
            while read_resources(ready["resourcesPath"])["receivedGoaways"] < initial + 1:
                await asyncio.sleep(0.05)
        client.body(stream, b"after-client-goaway", end=True)
        assert await response_complete(response) == b"early:after-client-goaway"
        assert read_stats(peer)["requests"] == 1
    return {"sentClientPushID": 0, "admittedRequestCompleted": True,
            "originControlFrameMapping": "not established by this wire scenario"}


async def unsupported(name, ready, authority, certificate, peer):
    async with connect("127.0.0.1", ready["port"], configuration=configuration(certificate, "localhost"),
                       create_protocol=Client) as client:
        stream, response = client.raw_request()
        fields = [(b":method", b"CONNECT"), (b":authority", authority.encode("ascii"))]
        if name == "unsupported-webtransport":
            fields += [(b":scheme", b"https"), (b":path", b"/security/webtransport"), (b":protocol", b"webtransport")]
        client._quic.send_stream_data(stream, octet_headers(fields), end_stream=True)
        client.transmit()
        error = await rejection(response)
        assert read_stats(peer)["requests"] == 0 and read_stats(peer)["datagrams"] == 0
    return {"rejection": error, "originRequests": 0, "originDatagrams": 0}


async def run_security_case(binary, name, artifacts):
    assert name in SECURITY_CASES, name
    artifacts = artifacts / name
    artifacts.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="swiftmitm-h3-security-") as temporary:
        directory = Path(temporary)
        environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
        processes = []
        peers = []
        ready = None
        async with AsyncExitStack() as stack:
            try:
                for label in ("fixed", "mapped") if name.startswith("mapped-route") else ("fixed",):
                    log = stack.enter_context((directory / f"{label}.log").open("wb"))
                    ready_file = directory / label / "ready.json"
                    process = await asyncio.create_subprocess_exec(
                        sys.executable, str(Path(__file__)), "--ready-file", str(ready_file),
                        stdout=log, stderr=asyncio.subprocess.STDOUT, env=environment
                    )
                    processes.append(process)
                    peers.append(await readiness(ready_file, process))
                roots = Path(peers[0]["certificate"]).read_bytes()
                if len(peers) == 2 and name != "mapped-route-trust":
                    roots += Path(peers[1]["certificate"]).read_bytes()
                trust = directory / "trust.pem"
                trust.write_bytes(roots)
                authority = "localhost:443"
                config = {"authority": authority, "destinationAddress": "127.0.0.1", "destinationPort": peers[0]["port"],
                          "originCAPath": str(trust), "readyPath": str(directory / "host-ready.json"),
                          "allowInternal": True, "maximumConnections": 128, "maximumPendingHandshakes": 64,
                          "maximumRequestStreamsPerConnection": 128, "maximumRetainedBytes": 67108864,
                          "observe": name.startswith("unsupported-")}
                if len(peers) == 2:
                    config.update(controlPreset="security-route", mappedAuthority=f"127.0.0.1:{peers[1]['port']}")
                config_path = directory / "configuration.json"
                config_path.write_text(json.dumps(config), encoding="utf-8")
                log = stack.enter_context((directory / "host.log").open("wb"))
                host = await asyncio.create_subprocess_exec(str(binary), "--configuration", str(config_path),
                                                          stdout=log, stderr=asyncio.subprocess.STDOUT, env=environment)
                processes.append(host)
                ready = await readiness(Path(config["readyPath"]), host)
                certificate = Path(ready["certificate"])
                if name == "cross-client-pooling":
                    report = await cross_client_pooling(ready, authority, certificate, peers[0])
                elif name.startswith("mapped-route"):
                    report = await mapped_route(name, ready, authority, certificate, peers[0], peers[1])
                elif "goaway" in name:
                    report = await goaway_case(name, ready, authority, certificate, peers[0])
                else:
                    report = await unsupported(name, ready, authority, certificate, peers[0])
                await shutdown(host)
                assert host.returncode == 0, host.returncode
                report["finalResources"] = read_resources(ready["resourcesPath"], stopped=True)
                capture = json.loads(Path(ready["capturePath"]).read_text(encoding="utf-8"))
                for event in capture:
                    if event["kind"] == "requestHead":
                        assert event["authority"] == authority and event["provenance"] == "fixedRouteHTTP3", event
                        assert event["destinationAddress"] == "127.0.0.1", event
                        assert event["destinationPort"] == peers[0]["port"], event
                if name.startswith("unsupported-"):
                    for representation in ("original", "effective"):
                        path = Path(config["readyPath"] + f".{representation}.json")
                        observations = json.loads(path.read_text(encoding="utf-8"))
                        assert not any(event["kind"] in ("requestHead", "responseHead")
                                       for event in observations), observations
                for peer in peers:
                    stats = read_stats(peer)
                    assert not stats["earlyDataAccepted"], stats
                    assert set(stats["alpn"]) <= {"h3"}, stats
                return report
            finally:
                cleanup = await asyncio.gather(*(shutdown(process) for process in reversed(processes)),
                                               return_exceptions=True)
                for label in ("fixed", "mapped", "host"):
                    retain_tail(directory / f"{label}.log", artifacts / f"{label}.log")
                for index, peer in enumerate(peers):
                    retain_tail(Path(peer["stats"]), artifacts / f"origin-{index}-stats.json")
                if ready:
                    retain_tail(Path(ready["resourcesPath"]), artifacts / "resources.json")
                    retain_tail(Path(ready["capturePath"]), artifacts / "capture.json")
                    for representation in ("original", "effective"):
                        retain_tail(Path(config["readyPath"] + f".{representation}.json"),
                                    artifacts / f"{representation}.json")
                failures = [error for error in cleanup if isinstance(error, Exception)]
                if failures:
                    raise AssertionError("security peer cleanup failed") from failures[0]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ready-file", type=Path, required=True)
    parser.add_argument("--flow-proof", action="store_true")
    asyncio.run(serve(parser.parse_args(), origin_type=SecurityOrigin))
