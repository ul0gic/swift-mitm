import asyncio
from contextlib import contextmanager
import json
from pathlib import Path
import ssl

from aioquic import tls
from aioquic.asyncio.client import connect
from aioquic.buffer import Buffer

from flow import FlowLogger, frames, read_metrics


@contextmanager
def emitted_server_name(name):
    original = tls.push_client_hello
    serialized_names = []

    def push(buf, hello):
        # Change only the serialized SNI; certificate verification keeps its configured hostname.
        hello.server_name = name
        start = buf.tell()
        original(buf, hello)
        decoded = tls.pull_client_hello(Buffer(data=buf.data_slice(start, buf.tell())))
        serialized_names.append(decoded.server_name)

    tls.push_client_hello = push
    try:
        yield serialized_names
    finally:
        tls.push_client_hello = original


async def sni_policy(ready, authority, certificate, peer, artifacts, client_type, configuration, complete):
    logger = FlowLogger()
    config = configuration(certificate, "localhost")
    config.quic_logger = logger
    rejected = False
    try:
        with emitted_server_name("wrong-host.invalid") as names:
            try:
                async with asyncio.timeout(12):
                    async with connect("127.0.0.1", ready["port"], configuration=config,
                                       create_protocol=client_type):
                        raise AssertionError("wrong present SNI completed the handshake")
            except ConnectionError:
                rejected = True
        assert rejected and names and all(name == "wrong-host.invalid" for name in names), names
        remote_alerts = [frame for trace in logger._traces for frame in frames(trace, "received")
                         if frame["frame_type"] == "connection_close"]
        alert_codes = [frame["error_code"] for frame in remote_alerts]
        allowed_alerts = {0x100 + int(tls.AlertDescription.handshake_failure),
                          0x100 + int(tls.AlertDescription.unrecognized_name)}
        assert any(code in allowed_alerts for code in alert_codes), remote_alerts
        stats = json.loads(Path(peer["stats"]).read_text(encoding="utf-8"))
        assert stats["datagrams"] == 0 and stats["requests"] == 0, stats
    finally:
        logger.write(artifacts / "wrong-sni.qlog")
    accepted = []
    for name, path in (("LOCALHOST", "/case-sni"), (None, "/absent-sni"), ("localhost", "/after-sni-rejection")):
        config = configuration(certificate, "localhost")
        assert config.verify_mode == ssl.CERT_REQUIRED
        with emitted_server_name(name) as names:
            async with connect("127.0.0.1", ready["port"], configuration=config,
                               create_protocol=client_type) as client:
                assert client.alpn == "h3" and client._quic.tls._server_name == "localhost"
                assert await complete(client.request(authority, path)[1]) == b"origin"
            assert names and all(value == name for value in names), names
        accepted.append({"serializedSNI": name, "certificateVerificationName": "localhost"})
    return {"wrongPresentSNIRejectedDuringHandshake": True, "remoteTLSAlertCodes": alert_codes,
            "originDatagramsForRejectedSNI": 0, "accepted": accepted}


async def goaway_drain(ready, authority, certificate, peer, client_type, configuration, complete):
    metrics_path = Path(ready["metricsPath"])
    initial_events = read_metrics(metrics_path)["acceptedGoawayEvents"]
    async with connect("127.0.0.1", ready["port"], configuration=configuration(certificate, "localhost"),
                       create_protocol=client_type) as client:
        stream, existing = client.request(authority, "/goaway-drain", end=False)
        await asyncio.wait_for(existing.first_data.wait(), timeout=10)
        assert bytes(existing.body) == b"early:" and not existing.done.is_set()
        async with asyncio.timeout(10):
            while read_metrics(metrics_path)["acceptedGoawayEvents"] < initial_events + 3:
                await asyncio.sleep(0.12)
        assert read_metrics(metrics_path)["acceptedGoawayEvents"] == initial_events + 3
        rejected_stream, rejected = client.request(authority, "/must-not-follow-goaway")
        await asyncio.wait_for(rejected.done.wait(), timeout=10)
        assert rejected.error == "stream reset: 268", rejected.error
        assert not rejected.headers and not rejected.body
        assert client.connection_error is None, "GOAWAY ended an admitted exchange prematurely"
        stats = json.loads(Path(peer["stats"]).read_text(encoding="utf-8"))
        assert stats["requests"] == 1 and stats["goawayIDs"] == [8, 4, 4], stats
        client.body(stream, b"finish-after-goaway", end=True)
        assert await complete(existing) == b"early:finish-after-goaway"
    async with connect("127.0.0.1", ready["port"], configuration=configuration(certificate, "localhost"),
                       create_protocol=client_type) as fresh:
        assert await complete(fresh.request(authority, "/after-goaway-drain")[1]) == b"origin"
    return {"acceptedGOAWAYIDs": [8, 4, 4], "acceptedEventCount": 3,
            "rejectedLaterStream": rejected_stream, "rejectionCode": 268,
            "admittedExchangeCompleted": True, "freshConnectionProgress": True,
            "automaticRetry": False}
