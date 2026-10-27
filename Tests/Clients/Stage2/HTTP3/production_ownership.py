import asyncio
from contextlib import AsyncExitStack
import json
import os
from pathlib import Path
import sys
import tempfile

from aioquic.asyncio.client import connect

from production_controls import command as control_command, wait_json
from production_lifecycle import command as lifecycle_command
from production_resources import CURRENT, quiescent, read_resources, refusals
from verify import Client, configuration, readiness, response_complete, retain_flow_trace, retain_tail, shutdown


CASES = ("shared-byte-refusal", "orphan-stop-restart")


async def ownership_snapshot(prefix, predicate):
    return await wait_json(prefix + ".embedding.json", predicate)


async def shared_refusal(ready, authority, certificate):
    prefix = ready["prefix"]
    path = ready["resourcesPath"]
    async with connect("127.0.0.1", ready["port"], configuration=configuration(certificate, "localhost"),
                       create_protocol=Client) as client:
        warmup, response = client.request(authority, "/shared-warmup")
        assert await response_complete(response) == b"origin"
        del client.responses[warmup]
        baseline = await quiescent(path)
        shared_baseline = await ownership_snapshot(prefix, lambda value: value["shared"]["streams"] == 0)
        admitted = []
        for _ in range(32):
            stream, response = client.request(authority, "/duplex", end=False)
            await asyncio.wait_for(response.first_data.wait(), timeout=8)
            if response.error:
                assert response.error.startswith("stream reset:") and client.connection_error is None, response.error
                break
            assert bytes(response.body) == b"early:" and not response.done.is_set()
            admitted.append((stream, response))
        else:
            raise AssertionError("shared8MiB byte ceiling was not exercised")
        assert admitted
        await wait_json(path, lambda value: refusals(value, "shared", "retainedBytes") > 0)
        rejected = read_resources(path)
        shared = await ownership_snapshot(prefix, lambda value: value["shared"]["streams"] == len(admitted))
        assert shared["shared"]["retainedBytes"] <= 8388608, shared
        assert rejected["local"]["retainedBytes"] < 67108864, rejected
        assert refusals(rejected, "local", "retainedBytes") == 0, rejected
        survivor_stream, survivor = admitted[0]
        client.body(survivor_stream, b"survives-shared-refusal", end=True)
        assert await response_complete(survivor) == b"early:survives-shared-refusal"
        for stream, _ in admitted[1:]:
            client.cancel(stream)
        recovered = await quiescent(path, baseline)
        shared_recovered = await ownership_snapshot(prefix, lambda value: value["shared"] == shared_baseline["shared"])
        assert await response_complete(client.request(authority, "/after-shared-recovery")[1]) == b"origin"
        await quiescent(path, baseline)
        return {"sharedCeiling": 8388608, "localCeiling": 67108864, "admitted": len(admitted),
                "typedSharedRefusal": rejected["refusals"], "localRollbackBaseline": recovered,
                "sharedRecoveryBaseline": shared_recovered["shared"], "activeSiblingSurvived": True}


async def orphan_cycles(ready, authority, certificate, peer, evidence):
    prefix = ready["prefix"]
    port = ready["port"]
    cycles = []
    try:
        for sequence in range(20):
            async with connect("127.0.0.1", port, configuration=configuration(certificate, "localhost"),
                               create_protocol=Client) as client:
                stream, response = client.request(authority, "/production/paused")
                await wait_json(prefix + ".control-events.json", lambda value: value["callbacksEntered"] == sequence + 1)
                assert not response.done.is_set()
                client.cancel(stream)
                await ownership_snapshot(prefix, lambda value: value["control"]["orphanedCallbacks"] == 1)
            stopped = await lifecycle_command(prefix, "stop", f"orphan-stop-{sequence}")
            assert stopped["result"] == "success" and stopped["abandonedCallbacks"] == 1, stopped
            usage = json.loads(Path(prefix + f".lifecycle-orphan-stop-{sequence}-after.json").read_text())
            snapshot = await ownership_snapshot(prefix, lambda value: (
                value["sampleStartedNanoseconds"] > usage["sampleEndedNanoseconds"]
                and value.get("stopReport", {}).get("abandonedCallbacks") == 1))
            assert snapshot["sampleStartedNanoseconds"] <= snapshot["sampleEndedNanoseconds"], snapshot
            assert usage["phase"] == "stopped" and all(usage[name] == 0 for name in CURRENT), usage
            assert usage["local"]["connections"] == usage["local"]["streams"] == 0, usage
            retained = stopped["retainedCallbackBytes"]
            assert retained > 0
            assert usage["local"]["retainedBytes"] == snapshot["shared"]["retainedBytes"] == retained, (usage, snapshot)
            assert snapshot["control"] == {"pausedExchanges": 0, "callbackTasks": 1, "orphanedCallbacks": 1,
                                            "retainedBytes": retained, "taskSlots": 1}, snapshot
            restarted = await lifecycle_command(prefix, "start", f"orphan-start-{sequence}")
            assert restarted["result"] == "success", restarted
            port = restarted["port"]
            async with connect("127.0.0.1", port, configuration=configuration(certificate, "localhost"),
                               create_protocol=Client) as client:
                _, rejected = client.request(authority, "/production/paused")
                await asyncio.wait_for(rejected.done.wait(), timeout=8)
                assert rejected.error and rejected.error.startswith("stream reset:"), rejected.error
                assert client.connection_error is None
                assert await response_complete(client.request(authority, "/after-orphan-restart")[1]) == b"origin"
            await control_command(prefix, f"release-{sequence}", "releaseCallback")
            await ownership_snapshot(prefix, lambda value: all(count == 0 for count in value["control"].values()))
            stale = await control_command(prefix, f"stale-resolve-{sequence}", "resolve")
            assert stale["resolutions"] == ["alreadyCompleted"], stale
            recovered = await quiescent(ready["resourcesPath"])
            assert all(recovered[name] == 0 for name in CURRENT), recovered
            assert all(count == 0 for count in recovered["local"].values()), recovered
            await ownership_snapshot(prefix, lambda value: all(count == 0 for count in value["shared"].values()))
            stats = json.loads(Path(peer["stats"]).read_text())
            assert stats["requests"] == sequence + 1, stats
            assert all(path == "/after-orphan-restart" for path in stats["paths"]), stats
            cycles.append({"sequence": sequence, "stopReport": stopped, "retained": snapshot,
                           "postRelease": recovered, "lateResolution": stale})
        return {"cycles": 20, "noncooperativeCallbackRetainedUntilExit": True,
                "sameBindingRestartRetainedCharges": True, "taskAdmissionRemainedCapped": True,
                "lateResultsDidNotWrite": True, "allOwnedCountersRecovered": True,
                "rss": "not measured by this scenario"}
    finally:
        evidence.parent.mkdir(parents=True, exist_ok=True)
        evidence.write_text(json.dumps(cycles, indent=2), encoding="utf-8")


async def run_case(binary, name, artifacts):
    with tempfile.TemporaryDirectory(prefix="swiftmitm-h3-ownership-") as temporary:
        directory = Path(temporary)
        ready_path = directory / "host-ready.json"
        origin_ready = directory / "origin" / "ready.json"
        environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
        async with AsyncExitStack() as stack:
            logs = {label: stack.enter_context((directory / f"{label}.log").open("wb")) for label in ("origin", "host")}
            origin = await asyncio.create_subprocess_exec(
                sys.executable, str(Path(__file__).with_name("peer.py")), "--ready-file", str(origin_ready),
                stdout=logs["origin"], stderr=asyncio.subprocess.STDOUT, env=environment)
            host = None
            try:
                peer = await readiness(origin_ready, origin)
                authority = "localhost:443"
                config = {"authority": authority, "destinationAddress": "127.0.0.1", "destinationPort": peer["port"],
                          "originCAPath": peer["certificate"], "readyPath": str(ready_path), "allowInternal": True,
                          "maximumConnections": 128, "maximumPendingHandshakes": 64,
                          "maximumRequestStreamsPerConnection": 128, "maximumRetainedBytes": 67108864,
                          "sharedRetainedBytes": 8388608 if name == "shared-byte-refusal" else 67108864}
                if name == "orphan-stop-restart":
                    config.update(controlPreset="orphan", callerOwnedGroup=True)
                path = directory / "configuration.json"
                path.write_text(json.dumps(config), encoding="utf-8")
                host = await asyncio.create_subprocess_exec(str(binary), "--configuration", str(path),
                                                          stdout=logs["host"], stderr=asyncio.subprocess.STDOUT, env=environment)
                ready = await readiness(ready_path, host)
                certificate = Path(ready["certificate"])
                if name == "shared-byte-refusal":
                    report = await shared_refusal(ready, authority, certificate)
                else:
                    report = await orphan_cycles(ready, authority, certificate, peer, artifacts / "orphan-cycles.json")
                await shutdown(host)
                assert host.returncode == 0, host.returncode
                report["finalResources"] = read_resources(ready["resourcesPath"], stopped=True)
                embedding = json.loads(Path(str(ready_path) + ".embedding.json").read_text())
                assert all(count == 0 for count in embedding["shared"].values()), embedding
                return report
            finally:
                try:
                    if host is not None:
                        await shutdown(host)
                finally:
                    await shutdown(origin)
                    for label in ("origin", "host"):
                        retain_tail(directory / f"{label}.log", artifacts / f"{label}.log")
                    retain_tail(origin_ready.parent / "stats.json", artifacts / "origin-stats.json")
                    for suffix in ("resources", "embedding", "control-events", "capture"):
                        retain_flow_trace(Path(str(ready_path) + f".{suffix}.json"), artifacts / f"{suffix}.json")
