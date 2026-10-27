import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import traceback

from aioquic.asyncio.client import connect

from flow import FlowLogger, FrozenStreamCredit, SLOW_CHUNK, STREAM_CREDIT, frames, stream_offset
from production_memory import observe, zero
from production_resources import quiescent, read_resources
from verify import Client, configuration, readiness, response_complete, retain_flow_trace, retain_tail, shutdown


DIAGNOSTIC_BYTES = 256 * 1024


class DiagnosticUnavailable(RuntimeError):
    pass


class CollectorProcessUnreaped(DiagnosticUnavailable):
    pass


async def collect(tool, pid, mode, output, *, command=None, byte_limit=32 * 1024 * 1024,
                  tool_name="malloc_history", timeout_seconds=5):
    output.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic_ns()
    process = await asyncio.create_subprocess_exec(
        str(tool), *(command if command is not None else (str(pid), mode)),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
    )
    size = 0
    record = {"mode": mode, "startedMonotonicNanoseconds": started, "complete": False,
              "timeoutSeconds": timeout_seconds, "processReaped": False}
    try:
        async with asyncio.timeout(timeout_seconds):
            with output.open("wb") as destination:
                while chunk := await process.stdout.read(65536):
                    remaining = byte_limit - size
                    destination.write(chunk[:remaining])
                    size += min(len(chunk), remaining)
                    if len(chunk) > remaining:
                        raise DiagnosticUnavailable(
                            f"{tool_name} output exceeded {byte_limit // (1024 * 1024)} MiB; retained prefix is incomplete")
                await process.wait()
                record["processReaped"] = True
        record["returnCode"] = process.returncode
        if process.returncode != 0 or size == 0:
            raise DiagnosticUnavailable(f"{tool_name} {mode}: exit={process.returncode}, outputBytes={size}")
        record["complete"] = True
    except TimeoutError as error:
        raise DiagnosticUnavailable(
            f"{tool_name} {mode} exceeded its {timeout_seconds}s collection deadline") from error
    finally:
        try:
            if process.returncode is None:
                process.kill()
            if not record["processReaped"]:
                await asyncio.wait_for(process.wait(), timeout=3)
                record["processReaped"] = True
        except TimeoutError as error:
            record["complete"] = False
            raise CollectorProcessUnreaped(f"{tool_name} {mode} exceeded its 3s kill/reap deadline") from error
        finally:
            record.update(returnCode=process.returncode, outputBytes=size,
                          endedMonotonicNanoseconds=time.monotonic_ns())
            output.with_suffix(".json").write_text(json.dumps(record, indent=2), encoding="utf-8")
    return record


async def capture_phase(tool, host, path, output, name, phases):
    assert host.returncode is None, "instrumented host exited before allocation sampling"
    record = {"name": name, "before": read_resources(path), "reports": []}
    phases.append(record)
    try:
        for mode, suffix in (("-allBySize", "live-by-size"), ("-callTree", "live-call-tree")):
            record["reports"].append(await collect(tool, host.pid, mode, output / f"{name}-{suffix}.txt"))
    finally:
        record["after"] = read_resources(path)
        (output / "phases.json").write_text(json.dumps(phases, indent=2), encoding="utf-8")


async def keep_alive(client):
    while True:
        await asyncio.wait_for(client.ping(), timeout=3)
        await asyncio.sleep(0.5)


async def frozen_phase(client, authority, peer, path):
    stream = client._quic.get_next_available_stream_id()
    gate = FrozenStreamCredit(client, stream)
    transferred = False
    start = len(client._quic._quic_logger._events)
    try:
        issued, response = client.request(authority, "/slow-receiver", stream_body=True)
        assert issued == stream
        await asyncio.wait_for(response.first_data.wait(), timeout=10)
        previous, stable_since, samples = None, None, []
        loop = asyncio.get_running_loop()
        async with asyncio.timeout(10):
            while True:
                assert not response.error and not response.done.is_set(), response.error
                await asyncio.wait_for(client.ping(), timeout=3)
                stats = json.loads(Path(peer["stats"]).read_text(encoding="utf-8"))
                offset = stats["slowResponseSentOffset"]
                now = loop.time()
                if previous != offset:
                    stable_since = now
                previous = offset
                sample = {"originWireSentOffset": offset, "originSample": stats["flowSamples"],
                          "clientWireReceivedOffset": stream_offset(client._quic._quic_logger, "received", stream),
                          "resources": read_resources(path)}
                samples.append(sample)
                assert len(samples) <= 96
                if (now - stable_since >= 0.6 and 0 < offset < DIAGNOSTIC_BYTES
                        and sample["clientWireReceivedOffset"] == STREAM_CREDIT
                        and sample["resources"]["local"]["streams"] == 1):
                    break
                await asyncio.sleep(0.12)
        sent = list(frames(client._quic._quic_logger, "sent", start))
        grants = [item["maximum"] for item in sent
                  if item["frame_type"] == "max_stream_data" and item["stream_id"] == stream]
        assert not grants or max(grants) <= STREAM_CREDIT, grants
        assert gate.intercepted > 0 and any(item["frame_type"] == "ack" for item in sent)
        assert await response_complete(client.request(authority, "/allocation-stall-sibling")[1]) == b"origin"
        transferred = True
        return gate, response, samples
    finally:
        if not transferred:
            gate.release()


async def workload(tool, host, ready, peer, output, phases):
    authority, path = "localhost:443", ready["resourcesPath"]
    await zero(path, 67108864)
    await capture_phase(tool, host, path, output, "baseline", phases)
    logger = FlowLogger()
    config = configuration(Path(ready["certificate"]), "localhost")
    config.max_stream_data = STREAM_CREDIT
    config.quic_logger = logger
    try:
        async with connect("127.0.0.1", ready["port"], configuration=config, create_protocol=Client) as client:
            assert client.alpn == "h3"
            keeper = asyncio.create_task(keep_alive(client))
            try:
                stream, response = client.request(authority, "/duplex", end=False)
                await asyncio.wait_for(response.first_data.wait(), timeout=10)
                assert bytes(response.body) == b"early:" and not response.done.is_set()
                await observe(path, 67108864, lambda value: value["local"]["streams"] == 1)
                await capture_phase(tool, host, path, output, "admitted", phases)
                client.body(stream, b"allocation-lifetime", end=True)
                assert await response_complete(response) == b"early:allocation-lifetime"
                await quiescent(path)
                gate, slow, samples = await frozen_phase(client, authority, peer, path)
                (output / "frozen-credit.json").write_text(json.dumps(samples, indent=2), encoding="utf-8")
                try:
                    await capture_phase(tool, host, path, output, "credit-frozen", phases)
                    assert not slow.done.is_set() and not slow.error
                finally:
                    gate.release()
                await asyncio.wait_for(slow.done.wait(), timeout=15)
                expected = hashlib.sha256()
                for _ in range(DIAGNOSTIC_BYTES // len(SLOW_CHUNK)):
                    expected.update(SLOW_CHUNK)
                assert not slow.error and slow.body_bytes == DIAGNOSTIC_BYTES and slow.digest.digest() == expected.digest()
                await quiescent(path)
                await capture_phase(tool, host, path, output, "resumed", phases)
                cancelled, pending = client.request(authority, "/duplex", end=False)
                await asyncio.wait_for(pending.first_data.wait(), timeout=10)
                assert bytes(pending.body) == b"early:" and not pending.done.is_set()
                client.cancel(cancelled)
                assert await response_complete(client.request(authority, "/allocation-cancel-survivor")[1]) == b"origin"
                await quiescent(path)
                await capture_phase(tool, host, path, output, "cancelled-with-live-connection", phases)
                assert client.connection_error is None
            finally:
                primary_error = sys.exception()
                keeper.cancel()
                try:
                    await keeper
                except asyncio.CancelledError:
                    pass
                except (TimeoutError, ConnectionError) as error:
                    failure = {"result": "failed", "reason": f"{type(error).__name__}: {error}",
                               "traceback": traceback.format_exc(limit=8),
                               "primaryErrorPreserved": primary_error is not None}
                    (output / "keep-alive-failure.json").write_text(json.dumps(failure, indent=2), encoding="utf-8")
                    if primary_error is None:
                        raise
        await zero(path, 67108864)
        await capture_phase(tool, host, path, output, "disconnected-quiescent", phases)
        await collect(tool, host.pid, "-allEvents", output / "allocation-free-events.txt")
        return {"resumedBytes": DIAGNOSTIC_BYTES, "resumedSHA256": expected.hexdigest(),
                "sameConnectionCancellationSurvivor": True, "zeroCurrentResourcesAfterDisconnect": True}
    finally:
        logger.write(output / "client.qlog")


async def run(arguments, report):
    with tempfile.TemporaryDirectory(prefix="swiftmitm-h3-allocations-") as temporary:
        directory = Path(temporary)
        ready_path, origin_ready = directory / "host-ready.json", directory / "origin" / "ready.json"
        environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
        for name in ("MallocStackLogging", "MallocStackLoggingNoCompact"):
            environment.pop(name, None)
        host_environment = dict(environment, MallocStackLoggingNoCompact="1", SWIFTMITM_HTTP3_WRITE_TRACE="1")
        host, origin = None, None
        artifacts = arguments.output.parent / "native-allocation-reports"
        artifacts.mkdir(parents=True, exist_ok=True)
        with (directory / "host.log").open("wb") as host_log, (directory / "origin.log").open("wb") as origin_log:
            try:
                origin = await asyncio.create_subprocess_exec(
                    sys.executable, str(Path(__file__).with_name("peer.py")), "--ready-file", str(origin_ready),
                    "--flow-proof", "--allocation-diagnostic", stdout=origin_log,
                    stderr=asyncio.subprocess.STDOUT, env=environment
                )
                peer = await readiness(origin_ready, origin)
                assert json.loads(Path(peer["stats"]).read_text(encoding="utf-8"))["allocationDiagnosticBytes"] == DIAGNOSTIC_BYTES
                config = {"authority": "localhost:443", "destinationAddress": "127.0.0.1",
                          "destinationPort": peer["port"], "originCAPath": peer["certificate"],
                          "readyPath": str(ready_path), "allowInternal": True,
                          "maximumConnections": 128, "maximumPendingHandshakes": 64,
                          "maximumRequestStreamsPerConnection": 128, "maximumRetainedBytes": 67108864}
                config_path = directory / "configuration.json"
                config_path.write_text(json.dumps(config), encoding="utf-8")
                host = await asyncio.create_subprocess_exec(
                    str(arguments.binary), "--configuration", str(config_path), stdout=host_log,
                    stderr=asyncio.subprocess.STDOUT, env=host_environment
                )
                ready = await readiness(ready_path, host)
                report["hostPID"] = host.pid
                report["protocolObservation"] = await workload(
                    arguments.malloc_history, host, ready, peer, artifacts, report["phases"]
                )
                await shutdown(host)
                assert host.returncode == 0, "instrumented public host did not stop cleanly"
                report["finalResources"] = read_resources(ready["resourcesPath"], stopped=True)
                stats = json.loads(Path(peer["stats"]).read_text(encoding="utf-8"))
                assert stats["requests"] == 5 and stats["handshakes"] == 1, stats
                assert stats["alpn"] == ["h3"] and not stats["earlyDataAccepted"], stats
                assert stats["peerQpackCapacity"] == stats["peerQpackBlocked"] == 0, stats
                report["origin"] = stats
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
                    retain_flow_trace(Path(str(ready_path) + ".capture.json"), artifacts / "capture.json")
                    retain_flow_trace(Path(str(ready_path) + ".write-trace.jsonl"), artifacts / "write-trace.jsonl")
                    retain_flow_trace(origin_ready.parent / "flow.qlog", artifacts / "origin.qlog")


async def main(arguments):
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    report = {"candidate": arguments.candidate, "surface": "public SwiftMITMHTTP3.HTTP3ProxyServer",
              "environment": {"MallocStackLoggingNoCompact": "1", "SWIFTMITM_HTTP3_WRITE_TRACE": "1"}, "phases": [],
              "writeTrace": {"artifact": "native-allocation-reports/write-trace.jsonl",
                             "absenceInterpretationRequires": ["schema SwiftMITMHTTP3.writeTrace.v1",
                                                               "truncated=false", "encodingFailed=false",
                                                               "summary records equals retained event count"],
                             "scope": "Observer and profiler costs included; not native-only allocation attribution"},
              "workload": {"responseBytes": DIAGNOSTIC_BYTES, "frozenStreamCreditBytes": STREAM_CREDIT,
                           "scope": "256 KiB attribution sampling; not full 8 MiB acceptance or native peak proof"},
              "collection": "unverified", "nativeComponentAttribution": "unverified: raw report interpretation required",
              "unsampledRSSPeak": "unverified", "C18Regression": "not run under allocation instrumentation"}
    code = 2
    try:
        for name, path in (("binary", arguments.binary), ("mallocHistory", arguments.malloc_history)):
            with path.open("rb") as source:
                digest = hashlib.file_digest(source, "sha256").hexdigest()
            report[name] = {"path": str(path.resolve()), "sha256": digest}
        async with asyncio.timeout(150):
            await run(arguments, report)
        report["collection"] = "captured"
        code = 0
    except TimeoutError as error:
        report["collection"] = "failed"
        report["reason"] = f"{type(error).__name__}: {error}"
        report["traceback"] = traceback.format_exc(limit=8)
        code = 1
    except (DiagnosticUnavailable, OSError) as error:
        report["reason"] = f"{type(error).__name__}: {error}"
        report["traceback"] = traceback.format_exc(limit=8)
    except (AssertionError, RuntimeError, KeyError, json.JSONDecodeError) as error:
        report["collection"] = "failed"
        report["reason"] = f"{type(error).__name__}: {error}"
        report["traceback"] = traceback.format_exc(limit=8)
        code = 1
    finally:
        arguments.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({"collection": report["collection"], "report": str(arguments.output)}))
    return code


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--malloc-history", type=Path, required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--output", type=Path, required=True)
    sys.exit(asyncio.run(main(parser.parse_args())))
