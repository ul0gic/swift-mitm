import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
import json
import os
from pathlib import Path
import statistics
import sys
import tempfile

from aioquic.asyncio.client import connect

from production_flow import storage_ceiling
from production_resources import CURRENT, current, quiescent, read_resources
from verify import Client, configuration, readiness, response_complete, retain_flow_trace, retain_tail, shutdown


CASES = ("memory-cancel-cycles", "memory-maximum-storage")


def sample(path, byte_limit):
    value = read_resources(path, byte_limit=byte_limit)
    assert isinstance(value["residentBytes"], int) and value["residentBytes"] > 0, value
    return value


async def observe(path, byte_limit, predicate, after=0, diagnostics=None):
    async with asyncio.timeout(8):
        while True:
            value = sample(path, byte_limit)
            if diagnostics is not None:
                diagnostics("ownership-observation", resources=value,
                            newerThanBarrier=value["sampleStartedNanoseconds"] > after)
            if value["sampleStartedNanoseconds"] > after and predicate(value):
                return value
            await asyncio.sleep(0.05)


async def zero(path, byte_limit, after=0, diagnostics=None):
    first = await observe(path, byte_limit, lambda value: (
        all(value[name] == 0 for name in CURRENT) and all(count == 0 for count in value["local"].values())
    ), after, diagnostics)
    if diagnostics is not None:
        diagnostics("zero-first-observed", resources=first)
    await quiescent(path, current(first), byte_limit=byte_limit)
    if diagnostics is not None:
        diagnostics("zero-quiescence-completed")
    return await observe(path, byte_limit, lambda value: (
        all(value[name] == 0 for name in CURRENT) and all(count == 0 for count in value["local"].values())
    ), first["sampleEndedNanoseconds"], diagnostics)


async def cancel_cycles(ready, authority, certificate, evidence):
    path, byte_limit = ready["resourcesPath"], 67108864
    samples = [{"phase": "baseline", "resources": await zero(path, byte_limit)}]
    quiescent_rss = []
    try:
        for cycle in range(20):
            async with connect("127.0.0.1", ready["port"], configuration=configuration(certificate, "localhost"),
                               create_protocol=Client) as client:
                assert client.alpn == "h3"
                held = []
                for _ in range(32):
                    stream, response = client.request(authority, "/duplex", end=False)
                    await asyncio.wait_for(response.first_data.wait(), timeout=8)
                    assert not response.error and not response.done.is_set() and bytes(response.body) == b"early:"
                    held.append((stream, response))
                admitted = await observe(path, byte_limit, lambda value: value["local"]["streams"] == 32)
                samples.append({"cycle": cycle, "phase": "admitted-stalled-uploads", "resources": admitted})
                survivor_stream, survivor = held[0]
                client.body(survivor_stream, b"resumed-memory-cycle", end=True)
                assert await response_complete(survivor) == b"early:resumed-memory-cycle"
                for stream, _ in held[1:]:
                    client.cancel(stream)
                recovered = await observe(path, byte_limit, lambda value: (
                    value["local"]["streams"] == 0 and value["forwardingReservedBytes"] == 0
                ), admitted["sampleEndedNanoseconds"])
                samples.append({"cycle": cycle, "phase": "resumed-and-cancelled", "resources": recovered})
                assert client.connection_error is None
                assert await response_complete(client.request(authority, "/memory-survivor")[1]) == b"origin"
            closed = await zero(path, byte_limit, recovered["sampleEndedNanoseconds"])
            samples.append({"cycle": cycle, "phase": "closed-quiescent", "resources": closed})
            quiescent_rss.append(closed["residentBytes"])
        first, last = statistics.median(quiescent_rss[:5]), statistics.median(quiescent_rss[-5:])
        allowance = max(16 * 1024 * 1024, first * 0.1)
        assert last <= first + allowance, {"firstFiveMedian": first, "lastFiveMedian": last, "allowance": allowance}
        return {"cycles": 20, "simultaneousRequestsPerCycle": 32, "cancelledRequests": 620,
                "completedResumedRequests": 20, "completedSurvivorRequests": 20,
                "exactZeroCurrentResourcesAfterEveryDisconnect": True,
                "firstFiveQuiescentRSSMedian": first, "lastFiveQuiescentRSSMedian": last,
                "permittedMedianGrowthBytes": allowance, "quiescentRSSBytes": quiescent_rss,
                "maximumObservedRSSBytes": max(item["resources"]["residentBytes"] for item in samples)}
    finally:
        evidence.parent.mkdir(parents=True, exist_ok=True)
        evidence.write_text(json.dumps(samples, indent=2), encoding="utf-8")


async def maximum_storage(ready, authority, certificate, evidence):
    path, byte_limit = ready["resourcesPath"], 268435456
    evidence.parent.mkdir(parents=True, exist_ok=True)
    phases = {}
    phase = "baseline"
    loop = asyncio.get_running_loop()
    started = loop.time()
    records = 0
    with evidence.with_name("ownership-progress.jsonl").open("w", encoding="utf-8") as progress:
        def record(event, **fields):
            nonlocal records
            assert records < 1024, "maximum-storage diagnostic record bound exceeded"
            progress.write(json.dumps({"event": event, "phase": phase,
                                       "elapsedSeconds": loop.time() - started, **fields}) + "\n")
            progress.flush()
            records += 1

        @asynccontextmanager
        async def recorded_client(index):
            diagnostics = {}
            context = connect("127.0.0.1", ready["port"],
                              configuration=configuration(certificate, "localhost"),
                              create_protocol=lambda *args, **kwargs: Client(
                                  *args, diagnostics=diagnostics, **kwargs))
            client = await context.__aenter__()
            try:
                record("client-handshake-completed", clientIndex=index)
                yield client
            finally:
                body_error = sys.exception()
                completed = False
                try:
                    record("client-close-initiated", clientIndex=index, diagnostics=diagnostics)
                finally:
                    error = sys.exception()
                    diagnostic_error = error if error is not body_error else None
                    try:
                        await context.__aexit__(None, None, None)
                        completed = True
                    finally:
                        close_error = None if completed else sys.exception()
                        record("client-context-exited", clientIndex=index, closeCompleted=completed,
                               bodyErrorType=type(body_error).__name__ if body_error is not None else None,
                               closeErrorType=type(close_error).__name__ if close_error is not None else None,
                               closeDiagnosticErrorType=(type(diagnostic_error).__name__
                                                         if diagnostic_error is not None else None),
                               diagnostics=diagnostics)

        try:
            baseline = await zero(path, byte_limit, diagnostics=record)
            phases["baseline"] = baseline
            phase = "connecting-clients"
            async with AsyncExitStack() as stack:
                clients = []
                try:
                    for index in range(8):
                        clients.append(await stack.enter_async_context(recorded_client(index)))
                        assert clients[-1].alpn == "h3"
                    phase = "storage-ceiling"
                    record("phase-started")
                    report = await storage_ceiling(clients, authority, path, response_complete,
                                                   evidence.parent / "capacity-samples.json", byte_limit)
                    phases["admittedCeiling"] = report["atRefusal"]
                    resumed = sample(path, byte_limit)
                    phases["resumed"] = resumed
                    phase = "closing-clients"
                    record("phase-started", resources=resumed)
                finally:
                    for client in clients:
                        client.close()
                    record("all-client-closes-initiated", clientCount=len(clients))
            phase = "awaiting-zero-after-client-close"
            record("phase-started")
            final = await zero(path, byte_limit, resumed["sampleEndedNanoseconds"], diagnostics=record)
            phases["closedQuiescent"] = final
            phase = "complete"
        finally:
            error = sys.exception()
            record("scenario-exit", errorType=type(error).__name__ if error is not None else None)
            evidence.write_text(json.dumps(phases, indent=2), encoding="utf-8")
    assert all(isinstance(value["residentBytes"], int) and value["residentBytes"] > 0 for value in phases.values())
    return dict(report, rssPhases={name: value["residentBytes"] for name, value in phases.items()},
                exactZeroCurrentResourcesAfterDisconnect=True,
                countCeilings={"connections": 1024, "pendingHandshakes": 256, "requestStreamsPerConnection": 1024},
                countCeilingsSimultaneouslyReached=False)


async def run_case(binary, name, artifacts):
    assert name in CASES
    with tempfile.TemporaryDirectory(prefix="swiftmitm-h3-memory-") as temporary:
        directory = Path(temporary)
        ready_path, origin_ready = directory / "host-ready.json", directory / "origin" / "ready.json"
        environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
        host, origin = None, None
        with (directory / "origin.log").open("wb") as origin_log, (directory / "host.log").open("wb") as host_log:
            try:
                origin = await asyncio.create_subprocess_exec(
                    sys.executable, str(Path(__file__).with_name("peer.py")), "--ready-file", str(origin_ready),
                    stdout=origin_log, stderr=asyncio.subprocess.STDOUT, env=environment
                )
                peer = await readiness(origin_ready, origin)
                maximum = name == "memory-maximum-storage"
                byte_limit = 268435456 if maximum else 67108864
                config = {"authority": "localhost:443", "destinationAddress": "127.0.0.1",
                          "destinationPort": peer["port"], "originCAPath": peer["certificate"],
                          "readyPath": str(ready_path), "allowInternal": True,
                          "maximumConnections": 1024 if maximum else 128,
                          "maximumPendingHandshakes": 256 if maximum else 64,
                          "maximumRequestStreamsPerConnection": 1024 if maximum else 128,
                          "maximumRetainedBytes": byte_limit}
                config_path = directory / "configuration.json"
                config_path.write_text(json.dumps(config), encoding="utf-8")
                host = await asyncio.create_subprocess_exec(
                    str(binary), "--configuration", str(config_path), stdout=host_log,
                    stderr=asyncio.subprocess.STDOUT, env=environment
                )
                ready = await readiness(ready_path, host)
                evidence = artifacts / name / "memory-samples.json"
                exercise = maximum_storage if maximum else cancel_cycles
                report = await exercise(ready, config["authority"], Path(ready["certificate"]), evidence)
                await shutdown(host)
                assert host.returncode == 0, "public HTTP3 host failed or did not stop cleanly"
                report["finalResources"] = read_resources(ready["resourcesPath"], stopped=True, byte_limit=byte_limit)
                stats = json.loads(Path(peer["stats"]).read_text(encoding="utf-8"))
                assert stats["alpn"] and set(stats["alpn"]) == {"h3"} and not stats["earlyDataAccepted"], stats
                assert stats["peerQpackCapacity"] == 0 and stats["peerQpackBlocked"] == 0, stats
                if not maximum:
                    assert stats["requests"] == 660 and stats["handshakes"] == 20, stats
                    assert stats["cancelSignals"] >= 620, stats
                report["originRequests"] = stats["requests"]
                report["measurement"] = {
                    "process": "HTTP3ProductionHost self task_info MACH_TASK_BASIC_INFO resident_size",
                    "includes": "native transport, library and bounded host capture recorder; peer Python is another process",
                    "captureRecorder": "at most20000 event summaries and8MiB header storage; no payload retention",
                    "componentAttribution": "unverified; RSS is not allocated-object attribution",
                    "unsampledPeak": "unverified; maximumObservedRSSBytes is sampled only",
                    "shared1GiBCeiling": "not exercised by these transport-local256MiB scenarios",
                }
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
                        retain_tail(source, artifacts / name / target)
                    retain_flow_trace(Path(str(ready_path) + ".capture.json"), artifacts / name / "capture.json")
