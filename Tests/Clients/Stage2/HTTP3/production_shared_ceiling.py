import asyncio
from contextlib import AsyncExitStack
import json
import os
from pathlib import Path
import sys
import tempfile

from aioquic.asyncio.client import connect

from production_resources import CURRENT, current, quiescent, read_resources, refusals
from verify import Client, configuration, readiness, response_complete, retain_flow_trace, retain_tail, shutdown


CASES = ("shared-maximum-storage",)
LOCAL_BYTES = 268435456
SHARED_BYTES = 1073741824
CONNECTIONS_PER_SERVER = 8


def snapshot(ready, stopped=False):
    shared = json.loads(Path(ready["sharedPath"]).read_text(encoding="utf-8"))
    assert shared["phase"] == ("stopped" if stopped else "running"), shared
    assert 0 < shared["residentBytes"] and shared["sampleStartedNanoseconds"] <= shared["sampleEndedNanoseconds"]
    assert 0 <= shared["shared"]["retainedBytes"] <= SHARED_BYTES, shared
    local = [read_resources(node["resourcesPath"], stopped=stopped, byte_limit=LOCAL_BYTES)
             for node in ready["servers"]]
    coherent = all(shared["sampleStartedNanoseconds"] <= value["sampleStartedNanoseconds"]
                   <= value["sampleEndedNanoseconds"] <= shared["sampleEndedNanoseconds"] for value in local)
    return {"shared": shared, "servers": local, "coherentSamplingInterval": coherent}


async def observe(ready, predicate):
    async with asyncio.timeout(10):
        while True:
            value = snapshot(ready)
            if value["coherentSamplingInterval"] and predicate(value):
                return value
            await asyncio.sleep(0.05)


def zero(value):
    return (all(count == 0 for count in value["shared"]["shared"].values())
            and all(all(node[key] == 0 for key in CURRENT)
                    and all(count == 0 for count in node["local"].values()) for node in value["servers"]))


async def keep_alive(clients):
    while True:
        await asyncio.wait_for(asyncio.gather(*(client.ping() for _, client in clients)), timeout=3)
        await asyncio.sleep(0.5)


async def exercise(ready, peer, artifacts):
    assert len(ready["servers"]) == 5, ready
    initial = await observe(ready, zero)
    certificate, authority = Path(ready["certificate"]), ready["authority"]
    clients, held, counts = [], [], [0] * 5
    phases = {"initial": initial}
    progress_path = artifacts / "shared-capacity-progress.jsonl"
    artifacts.mkdir(parents=True, exist_ok=True)
    try:
        async with AsyncExitStack() as stack:
            keeper = asyncio.create_task(keep_alive(clients))
            try:
                for connection in range(CONNECTIONS_PER_SERVER):
                    for index, node in enumerate(ready["servers"]):
                        client = await stack.enter_async_context(connect(
                            "127.0.0.1", node["port"], configuration=configuration(certificate, "localhost"),
                            create_protocol=Client))
                        assert client.alpn == "h3"
                        clients.append((index, client))
                        stream, response = client.request(authority, f"/shared-warmup/{index}/{connection}")
                        assert await response_complete(response) == b"origin"
                        del client.responses[stream]
                baselines = [await quiescent(node["resourcesPath"], byte_limit=LOCAL_BYTES)
                             for node in ready["servers"]]
                baseline = await observe(ready, lambda value: all(
                    current(node) == expected for node, expected in zip(value["servers"], baselines)))
                assert baseline["shared"]["shared"]["streams"] == 0, baseline
                phases["preload"] = baseline
                rejected = None
                with progress_path.open("w", encoding="utf-8") as progress:
                    for attempt in range(len(clients) * 48):
                        index, client = clients[attempt % len(clients)]
                        stream, response = client.request(authority, "/duplex", end=False)
                        await asyncio.wait_for(response.first_data.wait(), timeout=8)
                        if response.error:
                            assert response.error.startswith("stream reset:") and client.connection_error is None, response.error
                            rejected = {"server": index, "stream": stream, "error": response.error}
                            break
                        assert bytes(response.body) == b"early:" and not response.done.is_set()
                        held.append((index, client, stream, response))
                        counts[index] += 1
                        if (attempt + 1) % len(clients) == 0:
                            progress.write(json.dumps({"admittedPerServer": list(counts), "sample": snapshot(ready)}) + "\n")
                            progress.flush()
                assert rejected is not None and all(counts) and len(held) >= 256, (rejected, counts)
                peak = await observe(ready, lambda value: (
                    value["shared"]["shared"]["streams"] == len(held)
                    and [node["local"]["streams"] for node in value["servers"]] == counts
                    and sum(refusals(node, "shared", "retainedBytes") for node in value["servers"]) > 0))
                phases["sharedRefusal"] = peak
                assert peak["shared"]["shared"]["connections"] == len(clients) * 2, peak
                assert peak["shared"]["shared"]["retainedBytes"] == sum(
                    node["local"]["retainedBytes"] for node in peak["servers"]), peak
                for node in peak["servers"]:
                    assert node["local"]["connections"] == CONNECTIONS_PER_SERVER * 2, node
                    assert node["local"]["retainedBytes"] < LOCAL_BYTES, node
                    assert all(item["scope"] == "shared" and item["resource"] == "retainedBytes"
                               for item in node["refusals"] if item["count"]), node
                assert all(client.connection_error is None for _, client in clients)
                completed_servers = set()
                for index, client, stream, response in held:
                    if index not in completed_servers:
                        client.body(stream, b"shared-ceiling-survivor", end=True)
                        assert await response_complete(response) == b"early:shared-ceiling-survivor"
                        completed_servers.add(index)
                    else:
                        client.cancel(stream)
                assert completed_servers == set(range(5))
                for node, expected in zip(ready["servers"], baselines):
                    await quiescent(node["resourcesPath"], expected, byte_limit=LOCAL_BYTES)
                phases["recoveredWithLiveConnections"] = await observe(ready, lambda value: (
                    value["shared"]["shared"] == baseline["shared"]["shared"]
                    and all(current(node) == expected for node, expected in zip(value["servers"], baselines))))
                for index, client in clients[:5]:
                    assert await response_complete(client.request(authority, f"/shared-recovered/{index}")[1]) == b"origin"
                assert not keeper.done(), "a required live connection stopped responding to PING"
            finally:
                keeper.cancel()
                for _, client in clients:
                    client.close()
                try:
                    await keeper
                except asyncio.CancelledError:
                    pass
        phases["disconnectedQuiescent"] = await observe(ready, zero)
        stats = json.loads(Path(peer["stats"]).read_text(encoding="utf-8"))
        assert stats["requests"] == len(clients) + len(held) + 5, stats
        assert stats["handshakes"] == len(clients) and stats["alpn"] == ["h3"] * len(clients), stats
        assert not stats["earlyDataAccepted"] and stats["peerQpackCapacity"] == stats["peerQpackBlocked"] == 0, stats
        assert stats["uploadBytes"] == 5 * len(b"shared-ceiling-survivor"), stats
        assert stats["cancelSignals"] >= len(held) - 5, stats
        return {"localRetainedByteCeiling": LOCAL_BYTES, "sharedRetainedByteCeiling": SHARED_BYTES,
                "servers": 5, "downstreamConnections": len(clients), "admittedRequests": len(held),
                "admittedPerServer": counts, "typedSharedRefusal": rejected,
                "retainedBytesAtRefusal": peak["shared"]["shared"]["retainedBytes"],
                "unusedSharedBytesAtRefusal": SHARED_BYTES - peak["shared"]["shared"]["retainedBytes"],
                "independentSurvivors": 5, "exactSharedAndLocalBaselineRecovery": True,
                "allCurrentOwnershipZeroAfterDisconnect": True,
                "residentBytesByPhase": {name: value["shared"]["residentBytes"] for name, value in phases.items()},
                "originRequests": stats["requests"],
                "measurementBoundary": "real admitted traffic reservations; RSS includes five bounded host recorders",
                "unsampledRSSPeakAndNativeAllocationAttribution": "unverified",
                "simultaneousCountCeilings": "not claimed; shared byte limit is the binding constraint"}
    finally:
        (artifacts / "shared-capacity-phases.json").write_text(json.dumps(phases, indent=2), encoding="utf-8")


async def run_case(binary, name, artifacts):
    assert name in CASES
    artifacts.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="swiftmitm-h3-shared-capacity-") as temporary:
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
                          "maximumRequestStreamsPerConnection": 1024, "maximumRetainedBytes": LOCAL_BYTES,
                          "sharedRetainedBytes": SHARED_BYTES}
                config_path = directory / "configuration.json"
                config_path.write_text(json.dumps(config), encoding="utf-8")
                host = await asyncio.create_subprocess_exec(
                    str(binary), "--shared-ceiling", str(config_path), stdout=host_log,
                    stderr=asyncio.subprocess.STDOUT, env=environment)
                ready = await readiness(ready_path, host)
                report = await exercise(ready, peer, artifacts)
                await shutdown(host)
                assert host.returncode == 0, "multi-server public host did not stop cleanly"
                final = snapshot(ready, stopped=True)
                assert final["coherentSamplingInterval"] and zero(final), final
                report["finalResources"] = final
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
                                           (origin_ready.parent / "stats.json", "origin-stats.json")):
                        retain_tail(source, artifacts / target)
                    for source in directory.glob("host-ready.json.*.json"):
                        retain_flow_trace(source, artifacts / source.name)
