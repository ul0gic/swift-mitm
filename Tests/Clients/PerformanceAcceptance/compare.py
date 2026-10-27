import argparse
import hashlib
import json
from pathlib import Path
import statistics
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from client_tls import create_identity


PAYLOAD_BYTES = 256 * 1024 * 1024
THROUGHPUT_MEASURED_RESPONSES = 16
BASELINE_REVISION = "dc36969d94c9bbbfb2b7f8546b10ec441a4c3a13"
BASELINE_PATCH_SHA256 = "ba9b0596392cb69e086897d37f1c99f2b1e4fce5bc104cb8df1ded17edbb3d08"
BASELINE_TYPED_WRITES_PATCH_SHA256 = "67098af5a8696f332c46a1cfaec05746728511698c743984b6bd520659b5b56c"
ORDER = (("baseline", "candidate"), ("candidate", "baseline"), ("baseline", "candidate"),
         ("candidate", "baseline"), ("baseline", "candidate"))


class MeasurementUnavailable(RuntimeError):
    pass


def expected_digest(count):
    digest = hashlib.sha256()
    chunk = b"A" * 16384
    for _ in range(count // len(chunk)):
        digest.update(chunk)
    return digest.hexdigest()


def observe(path, protocol, workload):
    assert path.stat().st_size <= 2 * 1024 * 1024, "benchmark report exceeds expected bound"
    raw = json.loads(path.read_text(encoding="utf-8"))
    assert raw["protocolName"] == raw["downstreamALPN"] == protocol, raw
    assert raw["workload"] == workload and raw["upstreamALPN"], raw
    assert set(raw["upstreamALPN"]) == {protocol}, raw["upstreamALPN"]
    count = 100 if workload == "latency" else THROUGHPUT_MEASURED_RESPONSES
    paths = [f"/{workload}/{index}" for index in range(count + 1)]
    assert raw["originPaths"] == paths, raw["originPaths"]
    assert [item["path"] for item in raw["exchanges"]] == paths
    assert len(raw["captures"]) == count + 1
    captures = {item["path"]: item for item in raw["captures"]}
    assert len(captures) == count + 1 and len({item["requestID"] for item in captures.values()}) == count + 1
    expected_bytes = 0 if workload == "latency" else PAYLOAD_BYTES
    digest = expected_digest(expected_bytes)
    protocol_version = "HTTP/2" if protocol == "h2" else "HTTP/1.1"
    intervals = []
    response_intervals = []
    for index, exchange in enumerate(raw["exchanges"]):
        capture = captures[exchange["path"]]
        assert capture["protocolName"] == protocol_version and capture["status"] == 200, capture
        assert (capture["requestEnds"], capture["responseHeads"], capture["responseEnds"]) == (1, 1, 1), capture
        assert capture["responseBytes"] == exchange["bytes"] == expected_bytes, (capture, exchange)
        assert exchange["sha256"] == digest, exchange
        if exchange["end"] <= exchange["start"] or capture["headTime"] <= exchange["start"]:
            raise MeasurementUnavailable("missing or nonpositive monotonic interval")
        assert capture["headTime"] <= exchange["end"], "capture follows response completion"
        if index:
            intervals.append(capture["headTime"] - exchange["start"])
            response_intervals.append(exchange["end"] - exchange["start"])
    if workload == "latency":
        value = statistics.median(intervals)
    else:
        value = count * PAYLOAD_BYTES / 1048576 / (sum(response_intervals) / 1e9)
    return {"value": value, "unit": "nanoseconds" if workload == "latency" else "MiB/s",
            "warmupVerified": True, "firstEventIntervalsNanoseconds": intervals,
            "measuredResponseCount": count, "measuredResponseBytes": count * expected_bytes,
            "responseIntervalsNanoseconds": response_intervals,
            "correctness": "passed", "rawReport": str(path)}


def verdict(samples, workload):
    baseline = [samples[index]["baseline"]["value"] for index in range(5)]
    candidate = [samples[index]["candidate"]["value"] for index in range(5)]
    paired = [100 * (c / b - 1) if workload == "latency" else 100 * (1 - c / b)
              for b, c in zip(baseline, candidate)]
    spread = {name: 100 * (max(values) - min(values)) / statistics.median(values)
              for name, values in (("baseline", baseline), ("candidate", candidate))}
    regression = statistics.median(paired)
    result = "inconclusive" if max(spread.values()) > 10 else (
        "failed" if regression > 10 else "passed")
    return {"status": result, "pairedRegressionPercent": paired, "medianRegressionPercent": regression,
            "relativeSpreadPercent": spread, "meetsFivePercentTarget": result == "passed" and regression <= 5}


def run_cell(arguments, identity, protocol, workload, report, deadline):
    cell = {"protocol": protocol, "workload": workload, "samples": [], "status": "unverified"}
    report["cells"].append(cell)
    for pair, order in enumerate(ORDER):
        samples = {}
        cell["samples"].append(samples)
        for variant in order:
            stem = f"{protocol.replace('/', '-')}-{workload}-{pair + 1}-{variant}"
            output = arguments.output.parent / "performance-runs" / f"{stem}.json"
            output.parent.mkdir(parents=True, exist_ok=True)
            execution = {"pair": pair + 1, "variant": variant, "startedMonotonicNanoseconds": time.monotonic_ns()}
            report["executionOrder"].append(execution)
            try:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise MeasurementUnavailable("600-second measurement budget exhausted")
                binary = getattr(arguments, variant)
                with output.with_suffix(".log").open("wb") as log:
                    result = subprocess.run([str(binary), protocol, workload, str(identity), str(output)],
                                            stdout=log, stderr=subprocess.STDOUT, timeout=min(150, remaining), check=False)
                execution["returnCode"] = result.returncode
                if result.returncode:
                    raise RuntimeError(f"{stem} exited {result.returncode}")
                samples[variant] = observe(output, protocol, workload)
            except (OSError, subprocess.TimeoutExpired, MeasurementUnavailable) as error:
                cell.update(status="unverified", reason=f"{type(error).__name__}: {error}")
                return
            except (AssertionError, RuntimeError, KeyError, ValueError, json.JSONDecodeError) as error:
                cell.update(status="failed", reason=f"{type(error).__name__}: {error}")
                return
            finally:
                execution["endedMonotonicNanoseconds"] = time.monotonic_ns()
                arguments.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    cell.update(verdict(cell["samples"], workload))


def git_tree_identity(value):
    if len(value) != 40 or any(character not in "0123456789abcdef" for character in value):
        raise argparse.ArgumentTypeError("patched baseline tree must be a full lowercase Git SHA1 tree identity")
    return value


def main(arguments):
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    report = {"baselineRevision": arguments.baseline_revision, "candidateRevision": arguments.candidate_revision,
              "comparison": "candidate versus reconstructed dc36969 + status-map and typed-write compilation repairs",
              "baselineReconstruction": {
                  "patchSHA256": arguments.baseline_patch_sha256,
                  "typedWritesPatchSHA256": arguments.baseline_typed_writes_patch_sha256,
                  "patchedGitTree": arguments.baseline_patched_tree,
                  "scope": "status-name macro rows 497 and 505; three NIOAny wrapper removals in "
                           "ExplicitHTTPProxyHandler; measured workload requires 200 and uses explicitConnect",
                  "untouchedBaselinePerformance": "unavailable: original revision does not compile"
              },
              "benchmarkRevision": arguments.benchmark_revision, "configuration": "release",
              "samplingMethod": "C18.fixed-throughput-batch.v2",
              "throughputSampling": {"measuredResponses": THROUGHPUT_MEASURED_RESPONSES,
                                     "warmupResponses": 1, "responseBytes": PAYLOAD_BYTES,
                                     "aggregation": "totalBytes/sumResponseDurations"},
              "optionalStage2Features": "absent", "captureBodyLimit": 0,
              "cells": [], "executionOrder": [], "status": "unverified"}
    for variant in ("baseline", "candidate"):
        binary = getattr(arguments, variant).resolve(strict=True)
        setattr(arguments, variant, binary)
        with binary.open("rb") as source:
            report[variant + "BinarySHA256"] = hashlib.file_digest(source, "sha256").hexdigest()
    with tempfile.TemporaryDirectory(prefix="swiftmitm-performance-") as directory:
        identity = Path(directory)
        for name in ("proxy", "origin"):
            target = identity / name
            target.mkdir()
            certificate, _ = create_identity(target)
            report[name + "CertificatePEMSHA256"] = hashlib.sha256(certificate.read_bytes()).hexdigest()
        deadline = time.monotonic() + 600
        report["measurementBudgetSeconds"] = 600
        for protocol in ("http/1.1", "h2"):
            for workload in ("latency", "throughput"):
                run_cell(arguments, identity, protocol, workload, report, deadline)
    report["status"] = "passed" if all(cell["status"] == "passed" for cell in report["cells"]) else "incomplete"
    arguments.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({"status": report["status"], "comparison": report["comparison"], "cells": [
        {key: cell[key] for key in ("protocol", "workload", "status")} for cell in report["cells"]]}))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--baseline-revision", choices=[BASELINE_REVISION], required=True)
    parser.add_argument("--baseline-patch-sha256", choices=[BASELINE_PATCH_SHA256], required=True)
    parser.add_argument("--baseline-typed-writes-patch-sha256",
                        choices=[BASELINE_TYPED_WRITES_PATCH_SHA256], required=True)
    parser.add_argument("--baseline-patched-tree", type=git_tree_identity, required=True)
    parser.add_argument("--candidate-revision", required=True)
    parser.add_argument("--benchmark-revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    sys.exit(main(parser.parse_args()))
