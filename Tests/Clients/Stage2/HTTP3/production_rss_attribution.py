import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import sys
import time
import traceback

from production_allocations import CollectorProcessUnreaped, DiagnosticUnavailable, collect
from production_maximum_cycles import BYTE_LIMIT, run_case
from production_memory import zero
from production_resources import CURRENT, read_resources


CHECKPOINTS = (1, 5, 15, 20)


def capture_metadata(path):
    captured = Path(path).read_bytes()
    events = json.loads(captured)
    assert isinstance(events, list) and len(events) <= 20000
    header_bytes = sum(
        len(field["name"].encode("utf-8")) + len(field["value"].encode("utf-8"))
        + len(field.get("rawValueBytes") or [])
        for event in events for field in event.get("headers", [])
    )
    assert header_bytes <= 8 * 1024 * 1024
    return {"observedMonotonicNanoseconds": time.monotonic_ns(), "eventCount": len(events),
            "logicalHeaderBytes": header_bytes, "encodedFileBytes": len(captured),
            "encodedSHA256": hashlib.sha256(captured).hexdigest(),
            "scope": "recorder counters and encoded bytes, not heap or RSS attribution"}


async def checkpoint(arguments, report, host, ready, number, cycle):
    if number not in CHECKPOINTS:
        return
    assert host.returncode is None, "instrumented host exited before maximum-cycle sampling"
    record = {"cycle": number, "candidate": arguments.candidate, "hostPID": host.pid,
              "progress": cycle, "reports": []}
    report["checkpoints"].append(record)
    output = arguments.output.parent / "maximum-rss-reports"
    try:
        record["before"] = await zero(ready["resourcesPath"], BYTE_LIMIT,
                                     cycle["phaseSamples"]["closedQuiescent"]["sampleEndedNanoseconds"])
        record["captureBefore"] = capture_metadata(ready["capturePath"])
        for tool, mode, name, command, limit, seconds in (
            (arguments.vmmap, "-summary", "vmmap", ("-summary", str(host.pid)), 1024 * 1024, 5),
            (arguments.malloc_history, "-allBySize", "malloc_history", None, 32 * 1024 * 1024, 20),
        ):
            target = output / f"cycle-{number:02d}-{name}.txt"
            tool_report = {"tool": name, "artifact": target.name, "complete": False}
            record["reports"].append(tool_report)
            try:
                tool_report.update(await collect(tool, host.pid, mode, target, command=command,
                                                 byte_limit=limit, tool_name=name, timeout_seconds=seconds))
            except CollectorProcessUnreaped:
                raise
            except DiagnosticUnavailable as error:
                evidence = json.loads(target.with_suffix(".json").read_text(encoding="utf-8"))
                tool_report.update(evidence)
                tool_report["reason"] = f"{type(error).__name__}: {error}"
                tool_report["complete"] = False
                if evidence["processReaped"] is not True or type(evidence["returnCode"]) is not int:
                    raise CollectorProcessUnreaped(f"{name} has no confirmed child exit in its sidecar") from error
        record["captureAfter"] = capture_metadata(ready["capturePath"])
        observed = read_resources(ready["resourcesPath"], byte_limit=BYTE_LIMIT)
        record["after"] = await zero(ready["resourcesPath"], BYTE_LIMIT, observed["sampleEndedNanoseconds"])
        assert all(record["after"][key] == 0 for key in CURRENT)
        assert all(value == 0 for value in record["after"]["local"].values())
        record["complete"] = all(item["complete"] for item in record["reports"])
    finally:
        record.setdefault("complete", False)
        (output / "checkpoints.json").write_text(json.dumps(report["checkpoints"], indent=2), encoding="utf-8")


async def main(arguments):
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    artifacts = arguments.output.parent / "maximum-rss-reports"
    artifacts.mkdir(parents=True, exist_ok=True)
    report = {
        "candidate": arguments.candidate, "surface": "public SwiftMITMHTTP3.HTTP3ProxyServer",
        "environment": {"MallocStackLogging": "1", "MallocStackLoggingNoCompact": "unset",
                        "SWIFTMITM_HTTP3_WRITE_TRACE": "unset"},
        "checkpoints": [], "requestedCycles": 20, "requestedCheckpoints": list(CHECKPOINTS),
        "collection": "unverified", "workload": "unverified",
        "C18UninstrumentedAcceptance": "not established by this instrumented run; original assertion still executes",
        "attribution": "unverified: complete symbolicated reports require interpretation",
        "unsampledPeakAndFreedAllocationHistory": "unverified; compact logging records current live allocations",
        "observerCosts": "whole host includes unchanged recorder and allocation instrumentation; no modeled subtraction",
        "bounds": {"runSeconds": 390, "peerProcessSeconds": 420, "allocationToolSeconds": 20, "vmToolSeconds": 5,
                   "killReapSeconds": 3, "allocationReportBytes": 32 * 1024 * 1024,
                   "vmReportBytes": 1024 * 1024},
    }
    code = 2
    try:
        for name, path in (("binary", arguments.binary), ("mallocHistory", arguments.malloc_history),
                           ("vmmap", arguments.vmmap)):
            with path.open("rb") as source:
                digest = hashlib.file_digest(source, "sha256").hexdigest()
            report[name] = {"path": str(path.resolve()), "sha256": digest}
        environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1", MallocStackLogging="1")
        environment.pop("MallocStackLoggingNoCompact", None)
        environment.pop("SWIFTMITM_HTTP3_WRITE_TRACE", None)

        async def observe(host, ready, number, cycle):
            await checkpoint(arguments, report, host, ready, number, cycle)

        async with asyncio.timeout(390):
            report["protocolObservation"] = await run_case(
                arguments.binary, "memory-maximum-cycles", artifacts, checkpoint=observe,
                host_environment=environment, peer_seconds=420)
        report["workload"] = "passed under instrumentation"
        code = 0
    except TimeoutError as error:
        report["workload"] = "failed"
        report["reason"] = f"{type(error).__name__}: {error}"
        report["traceback"] = traceback.format_exc(limit=8)
        code = 1
    except (DiagnosticUnavailable, OSError) as error:
        report["reason"] = f"{type(error).__name__}: {error}"
        report["traceback"] = traceback.format_exc(limit=8)
    except (AssertionError, RuntimeError, KeyError, json.JSONDecodeError) as error:
        report["workload"] = "failed"
        report["reason"] = f"{type(error).__name__}: {error}"
        report["traceback"] = traceback.format_exc(limit=8)
        code = 1
    finally:
        if ([item["cycle"] for item in report["checkpoints"]] == list(CHECKPOINTS)
                and all(item["complete"] and len(item["reports"]) == 2 for item in report["checkpoints"])):
            report["collection"] = "captured"
        elif code == 0:
            code = 2
        arguments.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({"collection": report["collection"], "workload": report["workload"],
                      "report": str(arguments.output)}))
    return code


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--malloc-history", type=Path, required=True)
    parser.add_argument("--vmmap", type=Path, required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--output", type=Path, required=True)
    sys.exit(asyncio.run(main(parser.parse_args())))
