import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile

from aioquic.asyncio.client import connect

from flow import FlowLogger, FrozenStreamCredit, SLOW_BYTES, SLOW_CHUNK, STREAM_CREDIT, frames, stream_offset
from peer import Origin, serve
from production_memory import zero
from production_resources import quiescent, read_resources
from verify import Client, configuration, readiness, response_complete, retain_flow_trace, retain_tail, shutdown


CASES = ("upstream-credit-stall",)


class UploadOrigin(Origin):
    def __init__(self, *args, **kwargs):
        self.upload_stream = None
        self.upload_gate = None
        self.upload_digest = hashlib.sha256()
        self.upload_bytes = 0
        super().__init__(*args, **kwargs)
        # aioquic1.7 asyncio/server.py:183-201 constructs this protocol before parsing the first datagram.
        self._quic._local_max_stream_data_bidi_remote = STREAM_CREDIT
        self.stats.update(uploadSamples=0, uploadPingsAcknowledged=0, uploadCreditReleased=False,
                          uploadReceivedOffset=0, uploadACKFramesSent=0, uploadCreditGrants=[],
                          uploadGateInterceptions=0, uploadFinished=False)

    def headers(self, event):
        path = dict(event.headers).get(b":path")
        if path != b"/stalled-upload":
            return super().headers(event)
        assert self.upload_stream is None and not event.stream_ended
        self.upload_stream = event.stream_id
        self.upload_gate = FrozenStreamCredit(self, event.stream_id)
        self.paths[event.stream_id] = path
        self.stats["uploadInitialACKFramesSent"] = sum(
            frame["frame_type"] == "ack" for frame in frames(self._quic._quic_logger, "sent")
        )
        self.stats["requests"] += 1
        self.stats["paths"].append(path.decode("ascii"))
        self.http.send_headers(event.stream_id, [(b":status", b"200")])
        self.http.send_data(event.stream_id, b"early:", end_stream=False)

    def data(self, event):
        if event.stream_id != self.upload_stream:
            return super().data(event)
        self.upload_bytes += len(event.data)
        assert self.upload_bytes <= SLOW_BYTES
        self.upload_digest.update(event.data)
        self.stats["uploadBytes"] += len(event.data)
        if event.stream_ended:
            assert self.upload_bytes == SLOW_BYTES
            digest = self.upload_digest.hexdigest()
            self.stats.update(uploadFinished=True, uploadSHA256=digest)
            self.http.send_data(event.stream_id, digest.encode("ascii"), end_stream=False)
            self.http.send_headers(event.stream_id, [(b"x-upload-bytes", str(self.upload_bytes).encode("ascii"))],
                                   end_stream=True)
            self.paths.pop(event.stream_id)

    async def sample_flow(self):
        while True:
            if self.upload_stream is not None:
                if self.release_file.exists() and not self.stats["uploadCreditReleased"]:
                    self.upload_gate.release()
                    self.stats["uploadCreditReleased"] = True
                await asyncio.wait_for(self.ping(), timeout=3)
                trace = self._quic._quic_logger
                self.stats["uploadPingsAcknowledged"] += 1
                self.stats["uploadSamples"] += 1
                self.stats["uploadReceivedOffset"] = stream_offset(trace, "received", self.upload_stream)
                self.stats["uploadACKFramesSent"] = sum(frame["frame_type"] == "ack" for frame in frames(trace, "sent"))
                self.stats["uploadCreditGrants"] = [frame["maximum"] for frame in frames(trace, "sent")
                                                    if frame["frame_type"] == "max_stream_data"
                                                    and frame["stream_id"] == self.upload_stream]
                self.stats["uploadGateInterceptions"] = self.upload_gate.intercepted
                parameters = [event["data"] for event in trace.to_dict()["events"]
                              if event["name"] == "transport:parameters_set" and event["data"]["owner"] == "local"]
                assert len(parameters) == 1
                self.stats["uploadInitialStreamCredit"] = parameters[0]["initial_max_stream_data_bidi_remote"]
                self.publish()
            await asyncio.sleep(0.1)


async def exercise(ready, authority, certificate, peer, evidence):
    path = ready["resourcesPath"]
    initial = await zero(path, 67108864)
    logger = FlowLogger()
    config = configuration(certificate, "localhost")
    config.quic_logger = logger
    samples = []
    try:
        async with connect("127.0.0.1", ready["port"], configuration=config, create_protocol=Client) as client:
            assert client.alpn == "h3"
            warmup, response = client.request(authority, "/upload-warmup")
            assert await response_complete(response) == b"origin"
            del client.responses[warmup]
            baseline = await quiescent(path)
            before = read_resources(path)
            stream, response = client.request(authority, "/stalled-upload", end=False)
            await asyncio.wait_for(response.first_data.wait(), timeout=10)
            assert bytes(response.body) == b"early:" and not response.done.is_set() and not response.error
            expected = hashlib.sha256()
            for offset in range(0, SLOW_BYTES, len(SLOW_CHUNK)):
                expected.update(SLOW_CHUNK)
                client.body(stream, SLOW_CHUNK, end=offset + len(SLOW_CHUNK) == SLOW_BYTES)
            trace = client._quic._quic_logger
            previous_offset = previous_sequence = stable_since = None
            stable_samples = siblings = 0
            loop = asyncio.get_running_loop()
            async with asyncio.timeout(15):
                while True:
                    assert not response.error and not response.done.is_set(), response.error
                    await asyncio.wait_for(client.ping(), timeout=3)
                    if siblings < 3:
                        sibling, reply = client.request(authority, f"/upload-stall-sibling/{siblings}")
                        assert await response_complete(reply) == b"origin"
                        del client.responses[sibling]
                        siblings += 1
                    stats = json.loads(Path(peer["stats"]).read_text(encoding="utf-8"))
                    usage = read_resources(path)
                    sent = stream_offset(trace, "sent", stream)
                    sequence = stats["uploadSamples"]
                    now = loop.time()
                    if sent != previous_offset:
                        stable_since, stable_samples = now, 0
                    elif sequence != previous_sequence:
                        stable_samples += 1
                    previous_offset, previous_sequence = sent, sequence
                    samples.append({"elapsed": now, "downstreamClientSentOffset": sent,
                                    "originWireReceivedOffset": stats["uploadReceivedOffset"],
                                    "originSample": sequence, "resources": usage})
                    assert len(samples) <= 128
                    if (stable_samples >= 5 and now - stable_since >= 0.6 and siblings == 3
                            and STREAM_CREDIT <= sent < SLOW_BYTES
                            and stats["uploadReceivedOffset"] == STREAM_CREDIT
                            and stats["uploadPingsAcknowledged"] >= 5 and usage["local"]["streams"] == 1):
                        break
                    await asyncio.sleep(0.12)
            assert stats["uploadInitialStreamCredit"] == STREAM_CREDIT
            assert not stats["uploadCreditReleased"] and stats["uploadGateInterceptions"] > 0
            assert not stats["uploadCreditGrants"] or max(stats["uploadCreditGrants"]) <= STREAM_CREDIT
            assert stats["uploadACKFramesSent"] > stats["uploadInitialACKFramesSent"] and stats["handshakes"] == 1
            assert any(frame["frame_type"] == "ack" for frame in frames(trace, "sent"))
            assert any(frame["frame_type"] == "ack" for frame in frames(trace, "received"))
            stable_seconds = loop.time() - stable_since
            stalled = usage
            Path(peer["releaseFin"]).write_text("release-upload", encoding="ascii")
            assert await response_complete(response) == b"early:" + expected.hexdigest().encode("ascii")
            assert response.trailers == [(b"x-upload-bytes", str(SLOW_BYTES).encode("ascii"))]
            async with asyncio.timeout(8):
                while True:
                    final_stats = json.loads(Path(peer["stats"]).read_text(encoding="utf-8"))
                    if final_stats["uploadCreditGrants"] and max(final_stats["uploadCreditGrants"]) > STREAM_CREDIT:
                        break
                    await asyncio.sleep(0.12)
            assert final_stats["uploadFinished"] and final_stats["uploadSHA256"] == expected.hexdigest()
            assert final_stats["uploadBytes"] == SLOW_BYTES
            assert await response_complete(client.request(authority, "/upload-after-resume")[1]) == b"origin"
            await quiescent(path, baseline)
            resumed = read_resources(path)
            assert client.connection_error is None
        closed = await zero(path, 67108864, resumed["sampleEndedNanoseconds"])
        return {"frozenOriginStreamCredit": STREAM_CREDIT, "originWireReceivedOffsetWhileFrozen": STREAM_CREDIT,
                "downstreamClientWireSentPlateau": sent, "stableSamples": stable_samples,
                "stableSeconds": stable_seconds, "earlyResponseBeforeUpload": True, "completedSiblings": siblings,
                "upstreamOriginPingACKs": stats["uploadPingsAcknowledged"], "resumedUploadBytes": SLOW_BYTES,
                "resumedSHA256": expected.hexdigest(), "sameConnectionBaselineRecovered": baseline,
                "exactZeroAfterDisconnect": True,
                "rssPhases": {"beforeConnections": initial["residentBytes"], "beforeUpload": before["residentBytes"],
                              "frozenOriginCredit": stalled["residentBytes"], "resumed": resumed["residentBytes"],
                              "closedQuiescent": closed["residentBytes"]},
                "nativeUpstreamSenderOffsets": "not exposed; origin received STREAM offsets are wire observations"}
    finally:
        evidence.parent.mkdir(parents=True, exist_ok=True)
        evidence.write_text(json.dumps(samples, indent=2), encoding="utf-8")
        logger.write(evidence.parent / "client.qlog")


async def run_case(binary, name, artifacts):
    assert name in CASES
    artifacts.mkdir(parents=True, exist_ok=True)
    (artifacts / "write-trace-scope.json").write_text(json.dumps({
        "environment": {"SWIFTMITM_HTTP3_WRITE_TRACE": "1"},
        "maximumRecords": 4096, "maximumBytes": 1048576,
        "absenceInterpretationRequires": ["schema SwiftMITMHTTP3.writeTrace.v1", "truncated=false",
                                          "encodingFailed=false", "summary records equals retained event count"],
        "scope": "Diagnostic observer allocation and timing costs included; no uninstrumented performance claim"
    }, indent=2), encoding="utf-8")
    with tempfile.TemporaryDirectory(prefix="swiftmitm-h3-upload-") as temporary:
        directory = Path(temporary)
        ready_path, origin_ready = directory / "host-ready.json", directory / "origin" / "ready.json"
        environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
        host = origin = None
        with (directory / "origin.log").open("wb") as origin_log, (directory / "host.log").open("wb") as host_log:
            try:
                origin = await asyncio.create_subprocess_exec(
                    sys.executable, str(Path(__file__)), "--ready-file", str(origin_ready), "--flow-proof",
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
                host = await asyncio.create_subprocess_exec(
                    str(binary), "--configuration", str(config_path), stdout=host_log,
                    stderr=asyncio.subprocess.STDOUT, env=dict(environment, SWIFTMITM_HTTP3_WRITE_TRACE="1")
                )
                ready = await readiness(ready_path, host)
                report = await exercise(ready, config["authority"], Path(ready["certificate"]), peer,
                                        artifacts / "upload-samples.json")
                await shutdown(host)
                assert host.returncode == 0, "public HTTP3 host failed or did not stop cleanly"
                report["finalResources"] = read_resources(ready["resourcesPath"], stopped=True)
                stats = json.loads(Path(peer["stats"]).read_text(encoding="utf-8"))
                assert stats["requests"] == 6 and stats["handshakes"] == 1, stats
                assert stats["alpn"] == ["h3"] and not stats["earlyDataAccepted"], stats
                assert stats["peerQpackCapacity"] == 0 and stats["peerQpackBlocked"] == 0, stats
                await shutdown(origin)
                assert origin.returncode == 0, "independent upload origin failed"
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
                                           (origin_ready.parent / "stats.json", "origin-stats.json")):
                        retain_tail(source, artifacts / target)
                    retain_flow_trace(origin_ready.parent / "flow.qlog", artifacts / "origin.qlog")
                    retain_flow_trace(Path(str(ready_path) + ".capture.json"), artifacts / "capture.json")
                    retain_flow_trace(Path(str(ready_path) + ".write-trace.jsonl"), artifacts / "write-trace.jsonl")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ready-file", type=Path, required=True)
    parser.add_argument("--flow-proof", action="store_true")
    asyncio.run(serve(parser.parse_args(), origin_type=UploadOrigin))
