import asyncio
import json
from pathlib import Path


CURRENT = (
    "pendingHandshakes", "nativeListenerConnections", "nativeListenerPendingHandshakes",
    "activeConnectionStorageLifetimes", "nativeStreamReservedBytes", "parserReservedBytes",
    "forwardingReservedBytes", "sampledNativeTrackedStreams",
)


def read_resources(path, stopped=False, byte_limit=67108864):
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    assert value["schema"] == "SwiftMITMHTTP3.public.resourceUsage.v1", value
    assert value["phase"] == ("stopped" if stopped else "running"), value
    assert value["sampleStartedNanoseconds"] <= value["sampleEndedNanoseconds"], value
    assert all(value[name] >= 0 for name in CURRENT), value
    assert all(count >= 0 for count in value["local"].values()), value
    assert value["local"]["retainedBytes"] <= byte_limit, value
    assert value["local"]["replayAttempts"] == 0, value
    assert value["peakForwardedPayloadBytesPerStream"] <= 65536, value
    assert value["peakObservedRawReadCapacity"] <= 16384, value
    assert value["peakSampledNativePendingBytesPerStream"] <= 65536, value
    assert value["peakSampledNativeStorageTicketsPerStream"] <= 256, value
    assert value["nativeOutputSampleComplete"], "production native sample coverage incomplete"
    if stopped:
        assert all(value[name] == 0 for name in CURRENT), value
        assert all(count == 0 for count in value["local"].values()), value
    return value


def current(value):
    return dict({name: value[name] for name in CURRENT}, local=value["local"])


async def quiescent(path, expected=None, byte_limit=67108864):
    previous = None
    stable_since = None
    loop = asyncio.get_running_loop()
    try:
        async with asyncio.timeout(8):
            while True:
                value = read_resources(path, byte_limit=byte_limit)
                observed = current(value)
                now = loop.time()
                if (observed != previous or value["forwardingReservedBytes"] != 0
                        or value["local"]["streams"] != 0 or (expected is not None and observed != expected)):
                    stable_since = now
                elif now - stable_since >= 0.3:
                    return observed
                previous = observed
                await asyncio.sleep(0.12)
    except TimeoutError as error:
        raise AssertionError(f"public resource quiescence: observed={previous}, expected={expected}") from error


def refusals(value, scope, resource):
    return sum(item["count"] for item in value["refusals"]
               if item["scope"] == scope and item["resource"] == resource)


async def stream_limit(client, authority, resources_path, complete):
    warmup, response = client.request(authority, "/limit-warmup")
    assert await complete(response) == b"origin"
    del client.responses[warmup]
    baseline = await quiescent(resources_path)
    admitted = []
    for _ in range(2):
        stream, response = client.request(authority, "/duplex", end=False)
        await asyncio.wait_for(response.first_data.wait(), timeout=8)
        assert not response.error and bytes(response.body) == b"early:"
        admitted.append((stream, response))
    _, rejected = client.request(authority, "/must-not-exceed-stream-limit")
    await asyncio.wait_for(rejected.done.wait(), timeout=8)
    assert rejected.error and rejected.error.startswith("stream reset:"), rejected.error
    assert client.connection_error is None
    async with asyncio.timeout(5):
        while refusals(read_resources(resources_path), "local", "requestStreams") == 0:
            await asyncio.sleep(0.12)
    usage = read_resources(resources_path)
    assert usage["local"]["streams"] == 2, usage
    for stream, response in admitted:
        client.body(stream, b"survives-limit", end=True)
        assert await complete(response) == b"early:survives-limit"
    await quiescent(resources_path, baseline)
    assert await complete(client.request(authority, "/after-stream-limit")[1]) == b"origin"
    await quiescent(resources_path, baseline)
    return {"configuredRequestLimit": 2, "admitted": 2, "refused": True,
            "admittedSiblingsCompleted": True, "exactBaselineRecovered": baseline}


async def production_goaway(ready, authority, certificate, peer, client_type, configuration, complete, connect):
    path = ready["resourcesPath"]
    before = read_resources(path)["receivedGoaways"]
    async with connect("127.0.0.1", ready["port"], configuration=configuration(certificate, "localhost"),
                       create_protocol=client_type) as client:
        stream, response = client.request(authority, "/goaway-drain", end=False)
        await asyncio.wait_for(response.first_data.wait(), timeout=10)
        assert bytes(response.body) == b"early:" and not response.done.is_set()
        async with asyncio.timeout(10):
            while read_resources(path)["receivedGoaways"] < before + 3:
                await asyncio.sleep(0.12)
        assert read_resources(path)["receivedGoaways"] == before + 3
        draining = read_resources(path)
        assert await complete(client.request(authority, "/fresh-after-goaway")[1]) == b"origin"
        assert not response.done.is_set() and client.connection_error is None
        async with asyncio.timeout(8):
            while True:
                replacement = read_resources(path)
                if (replacement["local"]["connections"] == draining["local"]["connections"] + 1
                        and replacement["activeConnectionStorageLifetimes"]
                        == draining["activeConnectionStorageLifetimes"] + 1):
                    break
                await asyncio.sleep(0.12)
        stats = json.loads(Path(peer["stats"]).read_text())
        records = stats["goawayRequests"]
        assert stats["requests"] == 2 and stats["goawayIDs"] == [8, 4, 4], stats
        assert stats["paths"] == ["/goaway-drain", "/fresh-after-goaway"], stats
        assert [record["path"] for record in records] == stats["paths"], records
        assert len({record["connection"] for record in records}) == 2, records
        assert all(record["stream"] >= 0 and record["stream"] % 4 == 0 for record in records), records
        assert stats["handshakes"] == 2 and stats["alpn"] == ["h3", "h3"], stats
        client.body(stream, b"finish-after-goaway", end=True)
        assert await complete(response) == b"early:finish-after-goaway"
    async with connect("127.0.0.1", ready["port"], configuration=configuration(certificate, "localhost"),
                       create_protocol=client_type) as client:
        assert await complete(client.request(authority, "/after-goaway-drain")[1]) == b"origin"
    stats = json.loads(Path(peer["stats"]).read_text())
    records = stats["goawayRequests"]
    assert stats["requests"] == 3 and stats["handshakes"] == 3 and stats["alpn"] == ["h3"] * 3, stats
    assert stats["paths"] == ["/goaway-drain", "/fresh-after-goaway", "/after-goaway-drain"], stats
    assert [record["path"] for record in records] == stats["paths"], records
    assert len({record["connection"] for record in records}) == 3, records
    assert all(record["stream"] >= 0 and record["stream"] % 4 == 0 for record in records), records
    assert stats["uploadBytes"] == len(b"finish-after-goaway") and stats["goawayIDs"] == [8, 4, 4], stats
    return {"acceptedGOAWAYIDs": [8, 4, 4], "receivedGoaways": 3,
            "admittedExchangeCompleted": True, "freshConnectionProgress": True, "automaticRetry": False,
            "originRequestRecords": records, "originHandshakes": stats["handshakes"],
            "drainingLocalConnections": draining["local"]["connections"],
            "replacementLocalConnections": replacement["local"]["connections"]}
