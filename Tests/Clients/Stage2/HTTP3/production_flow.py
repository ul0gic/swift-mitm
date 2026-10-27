import asyncio
import hashlib
import json
from pathlib import Path

from flow import FrozenStreamCredit, SLOW_BYTES, SLOW_CHUNK, STREAM_CREDIT, frames, stream_offset
from production_resources import quiescent, read_resources, refusals


async def credit_stall(client, authority, peer, path, complete, evidence):
    trace = client._quic._quic_logger
    parameters = [event["data"] for event in trace.to_dict()["events"]
                  if event["name"] == "transport:parameters_set" and event["data"]["owner"] == "local"]
    assert len(parameters) == 1 and parameters[0]["initial_max_stream_data_bidi_local"] == STREAM_CREDIT
    stream = client._quic.get_next_available_stream_id()
    gate = FrozenStreamCredit(client, stream)
    start = len(trace._events)
    samples = []
    try:
        issued, response = client.request(authority, "/slow-receiver", stream_body=True)
        assert issued == stream
        await asyncio.wait_for(response.first_data.wait(), timeout=10)
        previous_offset = None
        previous_sequence = None
        stable_since = None
        stable_samples = 0
        siblings = 0
        loop = asyncio.get_running_loop()
        async with asyncio.timeout(12):
            while True:
                assert not response.error and not response.done.is_set(), response.error
                await asyncio.wait_for(client.ping(), timeout=3)
                if siblings < 3:
                    sibling_stream, sibling = client.request(authority, f"/during-credit-stall/{siblings}")
                    assert await complete(sibling) == b"origin"
                    del client.responses[sibling_stream]
                    siblings += 1
                stats = json.loads(Path(peer["stats"]).read_text(encoding="utf-8"))
                usage = read_resources(path)
                offset, sequence = stats["slowResponseSentOffset"], stats["flowSamples"]
                now = loop.time()
                if offset != previous_offset:
                    stable_since, stable_samples = now, 0
                elif sequence != previous_sequence:
                    stable_samples += 1
                previous_offset, previous_sequence = offset, sequence
                samples.append({"elapsed": now, "originSentOffset": offset, "originSample": sequence,
                                "clientReceivedOffset": stream_offset(trace, "received", stream), "resources": usage})
                assert len(samples) <= 128
                if (stable_samples >= 5 and now - stable_since >= 0.6 and siblings == 3
                        and 0 < offset < SLOW_BYTES
                        and samples[-1]["clientReceivedOffset"] == STREAM_CREDIT
                        and usage["local"]["streams"] == 1 and usage["sampledNativeTrackedStreams"] > 0):
                    break
                await asyncio.sleep(0.12)
        frozen_end = len(trace._events)
        sent, received = list(frames(trace, "sent", start, frozen_end)), list(frames(trace, "received", start, frozen_end))
        grants = [frame["maximum"] for frame in sent
                  if frame["frame_type"] == "max_stream_data" and frame["stream_id"] == stream]
        assert not grants or max(grants) <= STREAM_CREDIT, grants
        assert any(frame["frame_type"] == "ack" for frame in sent)
        assert any(frame["frame_type"] == "ack" for frame in received)
        assert any(frame["frame_type"] == "ping" for frame in sent)
        assert gate.intercepted > 0 and 0 < len(response.body) < STREAM_CREDIT
        assert stats["handshakes"] == 1, stats
        stalled_seconds = loop.time() - stable_since
        gate.release()
        await asyncio.wait_for(response.done.wait(), timeout=15)
        assert not response.error and (b":status", b"200") in response.headers, response.error
        expected = hashlib.sha256()
        for _ in range(SLOW_BYTES // len(SLOW_CHUNK)):
            expected.update(SLOW_CHUNK)
        assert response.body_bytes == SLOW_BYTES and response.digest.digest() == expected.digest()
        resumed_grants = [frame["maximum"] for frame in frames(trace, "sent", frozen_end)
                          if frame["frame_type"] == "max_stream_data" and frame["stream_id"] == stream]
        assert resumed_grants and max(resumed_grants) > STREAM_CREDIT, resumed_grants
        assert await complete(client.request(authority, "/after-credit-resume")[1]) == b"origin"
        return {"frozenStreamCredit": STREAM_CREDIT, "originWireSentPlateau": offset,
                "stableSamples": stable_samples, "stableSeconds": stalled_seconds, "completedSiblings": siblings,
                "pingACKProgress": True, "resumedBytes": SLOW_BYTES, "resumedSHA256": expected.hexdigest(),
                "nativeCurrentPendingAndTicketSums": "not exposed by public snapshot"}
    finally:
        gate.release()
        evidence.parent.mkdir(parents=True, exist_ok=True)
        evidence.write_text(json.dumps(samples, indent=2), encoding="utf-8")


async def cancel_churn(client, authority, path, complete, evidence):
    trace = client._quic._quic_logger
    warmup, response = client.request(authority, "/churn-warmup")
    assert await complete(response) == b"origin"
    del client.responses[warmup]
    baseline = await quiescent(path)
    cycles = []
    try:
        for sequence in range(32):
            stream = client._quic.get_next_available_stream_id()
            gate = FrozenStreamCredit(client, stream)
            start = len(trace._events)
            try:
                issued, response = client.request(authority, "/large", end=False)
                assert issued == stream
                await asyncio.wait_for(response.first_data.wait(), timeout=8)
                async with asyncio.timeout(8):
                    while True:
                        assert not response.error and not response.done.is_set(), response.error
                        await asyncio.wait_for(client.ping(), timeout=3)
                        usage = read_resources(path)
                        if (stream_offset(trace, "received", stream) == STREAM_CREDIT
                                and usage["local"]["streams"] == 1
                                and usage["local"]["retainedBytes"] > baseline["local"]["retainedBytes"]
                                and usage["sampledNativeTrackedStreams"] > baseline["sampledNativeTrackedStreams"]):
                            break
                        await asyncio.sleep(0.12)
                grants = [frame["maximum"] for frame in frames(trace, "sent", start)
                          if frame["frame_type"] == "max_stream_data" and frame["stream_id"] == stream]
                assert not grants or max(grants) <= STREAM_CREDIT, grants
                assert gate.intercepted > 0
                client.cancel(stream)
            finally:
                gate.release()
            sibling_stream, sibling = client.request(authority, f"/churn-sibling/{sequence}")
            assert await complete(sibling) == b"origin"
            del client.responses[sibling_stream]
            recovered = await quiescent(path, baseline)
            cycles.append({"sequence": sequence, "retainedBeforeCancel": usage, "recovered": recovered})
        return {"cancelledCreditBlockedStreams": 32, "completedSiblings": 32,
                "exactBaselineRecoveredEachCycle": True, "baseline": baseline,
                "nativeCurrentPendingAndTicketSums": "not exposed by public snapshot"}
    finally:
        evidence.parent.mkdir(parents=True, exist_ok=True)
        evidence.write_text(json.dumps(cycles, indent=2), encoding="utf-8")


async def storage_ceiling(clients, authority, path, complete, evidence, byte_limit):
    for index, client in enumerate(clients):
        stream, response = client.request(authority, f"/budget-warmup/{index}")
        assert await complete(response) == b"origin"
        del client.responses[stream]
    baseline = await quiescent(path, byte_limit=byte_limit)
    initial = refusals(read_resources(path, byte_limit=byte_limit), "local", "retainedBytes")
    admitted, samples = [], []
    refused = None
    try:
        for index in range(len(clients) * 48):
            client = clients[index % len(clients)]
            stream, response = client.request(authority, "/duplex", end=False)
            await asyncio.wait_for(response.first_data.wait(), timeout=8)
            if response.error:
                assert client.connection_error is None, "byte refusal terminated admitted sibling connection"
                assert response.error.startswith("stream reset:"), response.error
                async with asyncio.timeout(5):
                    while refusals(read_resources(path, byte_limit=byte_limit), "local", "retainedBytes") == initial:
                        await asyncio.sleep(0.12)
                refused = {"stream": stream, "error": response.error}
                break
            assert bytes(response.body) == b"early:" and not response.done.is_set()
            admitted.append((client, stream, response))
            samples.append(read_resources(path, byte_limit=byte_limit))
        assert refused is not None and admitted, "configured byte ceiling was not reached"
        peak = read_resources(path, byte_limit=byte_limit)
        assert peak["local"]["streams"] == len(admitted), peak
        assert peak["local"]["connections"] == 2 * len(clients), peak
        client, stream, survivor = admitted[0]
        client.body(stream, b"survives-capacity-refusal", end=True)
        assert await complete(survivor) == b"early:survives-capacity-refusal"
        del client.responses[stream]
        for client, stream, _ in admitted[1:]:
            client.cancel(stream)
        await quiescent(path, baseline, byte_limit=byte_limit)
        assert await complete(clients[0].request(authority, "/after-budget-recovery")[1]) == b"origin"
        await quiescent(path, baseline, byte_limit=byte_limit)
        return {"configuredRetainedBytes": byte_limit, "downstreamConnections": len(clients),
                "combinedNativeConnections": peak["local"]["connections"], "admittedRequests": len(admitted),
                "refusal": refused, "atRefusal": peak, "exactBaselineRecovered": baseline,
                "survivorCompleted": True, "newRequestAfterRecovery": True}
    finally:
        evidence.parent.mkdir(parents=True, exist_ok=True)
        evidence.write_text(json.dumps(samples, indent=2), encoding="utf-8")
