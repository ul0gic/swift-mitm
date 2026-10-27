import asyncio
import json
import os
from pathlib import Path
import statistics
import sys
import tempfile

from production_memory import maximum_storage, zero
from production_resources import CURRENT, read_resources, refusals
from verify import readiness, retain_flow_trace, retain_tail, shutdown


CASES = ("memory-maximum-cycles",)
BYTE_LIMIT = 268435456
CYCLES = 20


async def exercise(ready, peer, artifacts):
    artifacts.mkdir(parents=True, exist_ok=True)
    path = ready["resourcesPath"]
    initial = await zero(path, BYTE_LIMIT)
    cycles, rss = [], []
    previous_end = initial["sampleEndedNanoseconds"]
    total_admitted = 0
    loop = asyncio.get_running_loop()
    started = loop.time()
    try:
        for index in range(CYCLES):
            cycle_started = loop.time()
            phases_path = artifacts / f"cycle-{index:02d}" / "memory-samples.json"
            report = await maximum_storage(ready, ready["authority"], Path(ready["certificate"]), phases_path)
            phases = json.loads(phases_path.read_text(encoding="utf-8"))
            peak, closed = phases["admittedCeiling"], phases["closedQuiescent"]
            assert closed["sampleStartedNanoseconds"] > previous_end, closed
            assert closed["sampleStartedNanoseconds"] > phases["resumed"]["sampleEndedNanoseconds"], phases
            assert all(closed[key] == 0 for key in CURRENT), closed
            assert all(count == 0 for count in closed["local"].values()), closed
            admitted = report["admittedRequests"]
            assert admitted > 0 and peak["local"]["streams"] == admitted, report
            assert peak["local"]["retainedBytes"] <= BYTE_LIMIT, peak
            assert refusals(peak, "local", "retainedBytes") > refusals(phases["baseline"], "local", "retainedBytes")
            assert report["survivorCompleted"] and report["newRequestAfterRecovery"], report
            assert report["exactZeroCurrentResourcesAfterDisconnect"], report
            total_admitted += admitted
            previous_end = closed["sampleEndedNanoseconds"]
            rss.append(closed["residentBytes"])
            stats = json.loads(Path(peer["stats"]).read_text(encoding="utf-8"))
            assert stats["requests"] == total_admitted + (index + 1) * 9, stats
            assert stats["handshakes"] == (index + 1) * 8, stats
            assert stats["alpn"] == ["h3"] * stats["handshakes"] and not stats["earlyDataAccepted"], stats
            assert stats["peerQpackCapacity"] == stats["peerQpackBlocked"] == 0, stats
            assert stats["uploadBytes"] == (index + 1) * len(b"survives-capacity-refusal"), stats
            assert stats["cancelSignals"] >= total_admitted - (index + 1), stats
            capture = json.loads(Path(ready["capturePath"]).read_text(encoding="utf-8"))
            assert len(capture) <= 20000, "host capture recorder exceeded its unchanged event cap"
            cycles.append({
                "cycle": index, "elapsedSeconds": loop.time() - cycle_started,
                "offeredConcurrentRequests": admitted + 1, "admittedConcurrentRequests": admitted,
                "bindingConstraint": "local retainedBytes", "observedRefusal": report["refusal"],
                "phaseSamples": phases, "originRequestsAfterCycle": stats["requests"],
                "originHandshakesAfterCycle": stats["handshakes"],
                "retainedHostCaptureEvents": len(capture),
            })
            (artifacts / "maximum-cycle-progress.json").write_text(json.dumps(cycles, indent=2), encoding="utf-8")
        first, last = statistics.median(rss[:5]), statistics.median(rss[-5:])
        allowance = max(16 * 1024 * 1024, first * 0.1)
        assert last <= first + allowance, {"firstFiveMedian": first, "lastFiveMedian": last, "allowance": allowance}
        return {
            "cycles": CYCLES, "elapsedSeconds": loop.time() - started,
            "configuredRetainedBytes": BYTE_LIMIT,
            "configuredCountCeilings": {"connections": 1024, "pendingHandshakes": 256,
                                        "requestStreamsPerConnection": 1024},
            "admittedRequestsPerCycle": [item["admittedConcurrentRequests"] for item in cycles],
            "offeredRequestsPerCycle": [item["offeredConcurrentRequests"] for item in cycles],
            "bindingConstraintEveryCycle": "local retainedBytes",
            "countCeilingsSimultaneouslyReached": False,
            "completedResumedRequests": CYCLES, "completedRecoveryRequests": CYCLES,
            "cancelledRequests": total_admitted - CYCLES,
            "exactZeroCurrentResourcesAfterEveryDisconnect": True,
            "firstFiveQuiescentRSSMedian": first, "lastFiveQuiescentRSSMedian": last,
            "permittedMedianGrowthBytes": allowance, "quiescentRSSBytes": rss,
            "maximumObservedRSSBytes": max(
                sample["residentBytes"] for item in cycles for sample in item["phaseSamples"].values()),
            "measurement": {
                "process": "HTTP3ProductionHost self task_info MACH_TASK_BASIC_INFO resident_size",
                "includes": "native transport, library and unchanged bounded host capture recorder",
                "captureRecorder": "at most20000 event summaries and8MiB header storage; no payload retention",
                "nativeAllocationAttributionAndUnsampledPeak": "unverified",
                "shared1GiBCeiling": "not exercised by this local256MiB case",
            },
        }
    finally:
        (artifacts / "maximum-cycle-progress.json").write_text(json.dumps(cycles, indent=2), encoding="utf-8")


async def run_case(binary, name, artifacts):
    assert name in CASES
    artifacts.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="swiftmitm-h3-maximum-cycles-") as temporary:
        directory = Path(temporary)
        ready_path, origin_ready = directory / "host-ready.json", directory / "origin" / "ready.json"
        environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
        host, origin = None, None
        with (directory / "origin.log").open("wb") as origin_log, (directory / "host.log").open("wb") as host_log:
            try:
                origin = await asyncio.create_subprocess_exec(
                    sys.executable, str(Path(__file__).with_name("peer.py")), "--ready-file", str(origin_ready),
                    stdout=origin_log, stderr=asyncio.subprocess.STDOUT, env=environment)
                peer = await readiness(origin_ready, origin)
                config = {"authority": "localhost:443", "destinationAddress": "127.0.0.1",
                          "destinationPort": peer["port"], "originCAPath": peer["certificate"],
                          "readyPath": str(ready_path), "allowInternal": True,
                          "maximumConnections": 1024, "maximumPendingHandshakes": 256,
                          "maximumRequestStreamsPerConnection": 1024, "maximumRetainedBytes": BYTE_LIMIT}
                config_path = directory / "configuration.json"
                config_path.write_text(json.dumps(config), encoding="utf-8")
                host = await asyncio.create_subprocess_exec(
                    str(binary), "--configuration", str(config_path), stdout=host_log,
                    stderr=asyncio.subprocess.STDOUT, env=environment)
                ready = await readiness(ready_path, host)
                report = await exercise(ready, peer, artifacts)
                await shutdown(host)
                assert host.returncode == 0, "maximum-cycle public host did not stop cleanly"
                report["finalResources"] = read_resources(ready["resourcesPath"], stopped=True, byte_limit=BYTE_LIMIT)
                capture = json.loads(Path(ready["capturePath"]).read_text(encoding="utf-8"))
                assert len(capture) <= 20000, "host capture recorder exceeded its unchanged event cap"
                report["retainedHostCaptureEvents"] = len(capture)
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
                                           (origin_ready.parent / "stats.json", "origin-stats.json"),
                                           (Path(str(ready_path) + ".resources.json"), "resources.json")):
                        retain_tail(source, artifacts / target)
                    retain_flow_trace(Path(str(ready_path) + ".capture.json"), artifacts / "capture.json")
