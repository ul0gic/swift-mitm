import argparse
import hashlib
import json
from pathlib import Path
import statistics
import sys
import tempfile
import time

from compare import (PAYLOAD_BYTES, THROUGHPUT_MEASURED_RESPONSES, comparison_values,
                     create_identity, git_tree_identity, run_cell)


def cpu_delta(before, after):
    for snapshot in (before, after):
        if snapshot.get("errorNumber") is not None:
            raise ValueError(f"getrusage unavailable: errno {snapshot['errorNumber']}")
        assert snapshot["startedNanoseconds"] <= snapshot["endedNanoseconds"]
    assert before["endedNanoseconds"] <= after["startedNanoseconds"]
    wall = after["startedNanoseconds"] - before["endedNanoseconds"]
    assert wall > 0
    user = ((after["userSeconds"] - before["userSeconds"]) * 1000000
            + after["userMicroseconds"] - before["userMicroseconds"])
    system = ((after["systemSeconds"] - before["systemSeconds"]) * 1000000
              + after["systemMicroseconds"] - before["systemMicroseconds"])
    assert user >= 0 and system >= 0
    return {"wallNanoseconds": wall, "userMicroseconds": user, "systemMicroseconds": system,
            "processCPUPerWall": (user + system) * 1000 / wall,
            "voluntaryContextSwitches": after["voluntaryContextSwitches"] - before["voluntaryContextSwitches"],
            "involuntaryContextSwitches": after["involuntaryContextSwitches"] - before["involuntaryContextSwitches"]}


def observe_cpu(sample, workload):
    raw = json.loads(Path(sample["rawReport"]).read_text(encoding="utf-8"))
    observations = raw["processObservations"]
    before, after = observations["phaseBefore"], observations["phaseAfter"]
    exchanges = raw["exchanges"][1:]
    assert before["endedNanoseconds"] <= exchanges[0]["start"]
    assert after["startedNanoseconds"] >= exchanges[-1]["end"]
    result = {"phase": cpu_delta(before, after), "responses": [], "status": "captured"}
    responses = observations["responses"]
    assert len(responses) == (THROUGHPUT_MEASURED_RESPONSES if workload == "throughput" else 0)
    for exchange, observation in zip(exchanges, responses):
        assert observation["path"] == exchange["path"]
        assert observation["before"]["endedNanoseconds"] <= exchange["start"]
        assert observation["after"]["startedNanoseconds"] >= exchange["end"]
        result["responses"].append({"path": exchange["path"],
                                    **cpu_delta(observation["before"], observation["after"])})
    return result


def interpret(cell):
    for pair in cell["samples"]:
        for sample in pair.values():
            try:
                sample["cpuObservation"] = observe_cpu(sample, cell["workload"])
            except (OSError, AssertionError, KeyError, TypeError, ValueError) as error:
                sample["cpuObservation"] = {"status": "unverified", "reason": f"{type(error).__name__}: {error}"}
    if cell["status"] != "captured":
        return
    paired, spread = comparison_values(cell["samples"], cell["workload"], ("A", "B"))
    cell["repeatability"] = {"pairedTemporalDifferencePercent": paired,
                             "medianTemporalDifferencePercent": statistics.median(paired),
                             "relativeSpreadPercent": spread,
                             "exceedsExistingTenPercentSpread": max(spread.values()) > 10,
                             "differenceSign": "positive means B slower latency or lower throughput than A",
                             "scope": "temporal A/B control; not a revision regression verdict"}
    if any(sample["cpuObservation"]["status"] != "captured"
           for pair in cell["samples"] for sample in pair.values()):
        cell["status"] = "unverified"


def main(arguments):
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    binary = arguments.binary.resolve(strict=True)
    hashes = {}
    for label in ("A", "B"):
        with binary.open("rb") as source:
            hashes[label] = hashlib.file_digest(source, "sha256").hexdigest()
    assert hashes["A"] == hashes["B"], "A/A executable hashes differ before measurement"
    arguments.baseline = arguments.candidate = binary
    report = {"mode": "identical-binary-instrumented-control", "status": "unverified",
              "acceptance": "not evaluated; diagnostic cannot satisfy C18",
              "productRevisions": {label: arguments.product_revision for label in ("A", "B")},
              "benchmarkRevision": arguments.benchmark_revision, "binarySHA256": hashes,
              "configuration": "release", "measurementBudgetSeconds": 600, "subprocessBudgetSeconds": 150,
              "samplingMethod": "C18.fixed-throughput-batch.v2 plus process-boundary observations",
              "throughputSampling": {"measuredResponses": THROUGHPUT_MEASURED_RESPONSES,
                                     "warmupResponses": 1, "responseBytes": PAYLOAD_BYTES,
                                     "aggregation": "totalBytes/sumResponseDurations"},
              "latencyMeasuredRequests": 100, "captureBodyLimit": 0,
              "observerScope": "getrusage(RUSAGE_SELF) outside request intervals; phase excludes warmup; "
                               "throughput phase includes inter-response setup and observation work; "
                               "CPU covers all process threads, CPU/wall may exceed one; context switches "
                               "are implementation-defined diagnostics; snapshot brackets retain observer cost",
              "cells": [], "executionOrder": []}
    with tempfile.TemporaryDirectory(prefix="swiftmitm-aa-control-") as directory:
        identity = Path(directory)
        for name in ("proxy", "origin"):
            target = identity / name
            target.mkdir()
            certificate, _ = create_identity(target)
            report[name + "CertificatePEMSHA256"] = hashlib.sha256(certificate.read_bytes()).hexdigest()
        deadline = time.monotonic() + 600
        for protocol in ("http/1.1", "h2"):
            for workload in ("latency", "throughput"):
                run_cell(arguments, identity, protocol, workload, report, deadline, diagnostic=True)
                interpret(report["cells"][-1])
                arguments.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    report["status"] = "captured" if all(cell["status"] == "captured" for cell in report["cells"]) else "incomplete"
    arguments.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({"status": report["status"], "acceptance": report["acceptance"]}))
    return 0 if report["status"] == "captured" else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--product-revision", type=git_tree_identity, required=True)
    parser.add_argument("--benchmark-revision", type=git_tree_identity, required=True)
    parser.add_argument("--output", type=Path, required=True)
    sys.exit(main(parser.parse_args()))
