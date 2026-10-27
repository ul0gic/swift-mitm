import asyncio
import hashlib
import json
from pathlib import Path

from aioquic.quic.logger import QuicLogger, QuicLoggerTrace


STREAM_CREDIT = 16384
SLOW_CHUNK = bytes(range(256)) * 64
# Native FlowControl.swift allows a 4 MiB receive high-water mark, beyond initial stream credit.
SLOW_BYTES = 8 * 1024 * 1024
CURRENT_METRICS = (
    "sharedRetainedBytes", "parserReservedBytes", "nativeStreamReservedBytes", "queuedCopyBytes",
    "sampledNativePendingBytes", "sampledNativeRetainedBytes", "sampledNativeStorageTickets",
    "sampledNativeTrackedStreams",
    "activeConnectionStorageLifetimes",
)


class FlowTrace(QuicLoggerTrace):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.sent_offsets = {}

    def log_event(self, *, category, event, data):
        if len(self._events) >= 65536:
            raise RuntimeError("independent flow trace event bound exceeded")
        if category == "transport" and event == "packet_sent":
            for frame in data.get("frames", []):
                if frame["frame_type"] == "stream":
                    stream = frame["stream_id"]
                    self.sent_offsets[stream] = max(self.sent_offsets.get(stream, 0),
                                                    frame["offset"] + frame["length"])
        super().log_event(category=category, event=event, data=data)


class FlowLogger(QuicLogger):
    def start_trace(self, is_client, odcid):
        if len(self._traces) >= 4:
            raise RuntimeError("independent flow trace connection bound exceeded")
        trace = FlowTrace(is_client=is_client, odcid=odcid)
        self._traces.append(trace)
        return trace

    def write(self, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_dict()), encoding="utf-8")


class FrozenStreamCredit:
    def __init__(self, client, stream):
        self.client = client
        self.stream = stream
        self.original = client._quic._write_stream_limits
        self.intercepted = 0
        client._quic._write_stream_limits = self.write_limits

    def write_limits(self, builder, space, stream):
        if stream.stream_id == self.stream:
            # aioquic 1.7.0 auto-grows credit here; skip only this stream's grant.
            assert stream.max_stream_data_local == STREAM_CREDIT, stream.max_stream_data_local
            self.intercepted += 1
            return
        self.original(builder, space, stream)

    def release(self):
        self.client._quic._write_stream_limits = self.original
        self.client.transmit()


def frames(trace, direction, start=0, end=None):
    for event in trace.to_dict()["events"][start:end]:
        if event["name"] == f"transport:packet_{direction}":
            yield from event["data"]["frames"]


def stream_offset(trace, direction, stream):
    return max((frame["offset"] + frame["length"] for frame in frames(trace, direction)
                if frame["frame_type"] == "stream" and frame["stream_id"] == stream), default=0)


def read_metrics(path, stopped=False):
    value = json.loads(path.read_text(encoding="utf-8"))
    required = set(CURRENT_METRICS) | {"version", "phase", "peakSharedRetainedBytes",
                                     "peakForwardedPayloadBytesPerStream", "peakObservedRawReadCapacity",
                                     "peakSampledNativePendingBytesPerStream", "peakSampledNativeStorageTicketsPerStream"}
    assert required <= value.keys(), f"missing metrics: {required - value.keys()}"
    assert value["version"] == 1 and value["phase"] == ("stopped" if stopped else "running"), value
    assert all(value[name] >= 0 for name in CURRENT_METRICS), value
    assert value["sharedRetainedBytes"] <= 67108864, value
    assert value["peakSharedRetainedBytes"] <= 67108864, value
    assert value["peakForwardedPayloadBytesPerStream"] <= 65536, value
    assert value["peakObservedRawReadCapacity"] <= 16384, value
    assert value["peakSampledNativePendingBytesPerStream"] <= 65536, value
    assert value["peakSampledNativeStorageTicketsPerStream"] <= 256, value
    assert value["sampledNativePendingBytes"] <= value["sampledNativeTrackedStreams"] * 65536, value
    assert value["sampledNativeStorageTickets"] <= value["sampledNativeTrackedStreams"] * 256, value
    if stopped:
        assert all(value[name] == 0 for name in CURRENT_METRICS), value
    return value


async def quiescent_metrics(path, expected=None):
    previous = None
    stable_since = None
    loop = asyncio.get_running_loop()
    try:
        async with asyncio.timeout(8):
            while True:
                value = read_metrics(path)
                current = {name: value[name] for name in CURRENT_METRICS}
                quiet = all(value[name] == 0 for name in (
                    "queuedCopyBytes", "sampledNativePendingBytes", "sampledNativeRetainedBytes", "sampledNativeStorageTickets"
                ))
                now = loop.time()
                if not quiet or current != previous or (expected is not None and current != expected):
                    stable_since = now
                elif now - stable_since >= 0.3:
                    return current
                previous = current
                await asyncio.sleep(0.12)
    except TimeoutError as error:
        raise AssertionError(f"native quiescence deadline: observed={previous}, expected={expected}") from error


async def cancelled_credit_churn(client, authority, metrics_path, complete, evidence):
    trace = client._quic._quic_logger
    warmup, response = client.request(authority, "/churn-warmup")
    assert await complete(response) == b"origin"
    del client.responses[warmup]
    baseline = await quiescent_metrics(metrics_path)
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
                        retained = read_metrics(metrics_path)
                        if (stream_offset(trace, "received", stream) == STREAM_CREDIT
                                and retained["sampledNativePendingBytes"] > 0
                                and retained["sampledNativeRetainedBytes"] > 0
                                and retained["sampledNativeStorageTickets"] > 0):
                            break
                        await asyncio.sleep(0.12)
                grants = [frame["maximum"] for frame in frames(trace, "sent", start)
                          if frame["frame_type"] == "max_stream_data" and frame["stream_id"] == stream]
                assert not grants or max(grants) <= STREAM_CREDIT, grants
                client.cancel(stream)
            finally:
                gate.release()
            sibling_stream, sibling = client.request(authority, f"/churn-sibling/{sequence}")
            assert await complete(sibling) == b"origin"
            del client.responses[sibling_stream]
            recovered = await quiescent_metrics(metrics_path, expected=baseline)
            cycles.append({"sequence": sequence, "retainedBeforeCancellation": retained,
                           "recoveredCurrentMetrics": recovered})
        return {"cancelledCreditBlockedStreams": 32, "completedSiblings": 32,
                "baselineRestoredAfterEveryCycle": True, "baselineCurrentMetrics": baseline,
                "accountingScope": "reserved storage and sampled native records; excludes RSS and derived Frames"}
    finally:
        evidence.parent.mkdir(parents=True, exist_ok=True)
        evidence.write_text(json.dumps(cycles, indent=2), encoding="utf-8")


async def budget_refusal(clients, authority, metrics_path, complete, evidence):
    for index, client in enumerate(clients):
        stream, response = client.request(authority, f"/budget-warmup/{index}")
        assert await complete(response) == b"origin"
        del client.responses[stream]
    baseline = await quiescent_metrics(metrics_path)
    initial = read_metrics(metrics_path)
    assert "storageAdmissionRefusals" in initial, "probe does not identify storage admission refusals"
    initial_refusals = initial["storageAdmissionRefusals"]
    admitted = []
    samples = []
    refused = None
    try:
        for index in range(128):
            client = clients[index % len(clients)]
            stream, response = client.request(authority, "/duplex", end=False)
            await asyncio.wait_for(response.first_data.wait(), timeout=8)
            if response.error:
                assert client.connection_error is None, "budget exhaustion closed admitted siblings' connection"
                assert response.error.startswith("stream reset:"), response.error
                async with asyncio.timeout(5):
                    while read_metrics(metrics_path)["storageAdmissionRefusals"] == initial_refusals:
                        await asyncio.sleep(0.12)
                refused = {"stream": stream, "error": response.error}
                break
            assert bytes(response.body) == b"early:" and not response.done.is_set()
            admitted.append((client, stream, response))
            samples.append(read_metrics(metrics_path))
        assert refused is not None, "128 held exchanges did not exercise the default storage admission limit"
        assert admitted, "no work admitted before storage refusal"
        refusal_metrics = read_metrics(metrics_path)
        survivor_client, survivor_stream, survivor = admitted[0]
        survivor_client.body(survivor_stream, b"survives-capacity-refusal", end=True)
        assert await complete(survivor) == b"early:survives-capacity-refusal"
        del survivor_client.responses[survivor_stream]
        for client, stream, _ in admitted[1:]:
            client.cancel(stream)
        await quiescent_metrics(metrics_path, expected=baseline)
        recovery = clients[0].request(authority, "/after-budget-recovery")[1]
        assert await complete(recovery) == b"origin"
        await quiescent_metrics(metrics_path, expected=baseline)
        return {"connections": len(clients), "heldExchangesBeforeRefusal": len(admitted),
                "streamRefusal": refused, "storageAdmissionRefusals": refusal_metrics["storageAdmissionRefusals"],
                "peakSharedRetainedBytes": refusal_metrics["peakSharedRetainedBytes"],
                "sharedLimit": 67108864, "activeExchangeSurvivedRefusal": True,
                "baselineRestoredAfterCancellation": True, "requestAfterRecovery": True,
                "baselineCurrentMetrics": baseline}
    finally:
        evidence.parent.mkdir(parents=True, exist_ok=True)
        evidence.write_text(json.dumps(samples, indent=2), encoding="utf-8")


async def slow_receiver(client, authority, peer, metrics_path, complete, evidence):
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
                metrics = read_metrics(metrics_path)
                offset = stats["slowResponseSentOffset"]
                sequence = stats["flowSamples"]
                now = loop.time()
                if offset != previous_offset:
                    stable_since = now
                    stable_samples = 0
                elif sequence != previous_sequence:
                    stable_samples += 1
                previous_offset, previous_sequence = offset, sequence
                samples.append({"elapsed": now, "originSentOffset": offset, "originSample": sequence,
                                "clientReceivedOffset": stream_offset(trace, "received", stream),
                                "metrics": metrics})
                assert len(samples) <= 128, "independent metrics sample bound exceeded"
                if (stable_samples >= 5 and now - stable_since >= 0.6 and siblings == 3
                        and 0 < offset < SLOW_BYTES
                        and samples[-1]["clientReceivedOffset"] == STREAM_CREDIT
                        and metrics["sampledNativePendingBytes"] > 0
                        and metrics["sampledNativeRetainedBytes"] > 0
                        and metrics["sampledNativeStorageTickets"] > 0):
                    break
                await asyncio.sleep(0.12)
        frozen_end = len(trace._events)
        sent = list(frames(trace, "sent", start, frozen_end))
        received = list(frames(trace, "received", start, frozen_end))
        grants = [frame["maximum"] for frame in sent
                  if frame["frame_type"] == "max_stream_data" and frame["stream_id"] == stream]
        assert not grants or max(grants) <= STREAM_CREDIT, grants
        assert any(frame["frame_type"] == "ack" for frame in sent)
        assert any(frame["frame_type"] == "ack" for frame in received)
        assert any(frame["frame_type"] == "ping" for frame in sent)
        assert gate.intercepted > 0
        assert 0 < len(response.body) < STREAM_CREDIT
        assert stats["handshakes"] == 1, "sibling requests bypassed the blocked origin connection"
        stalled_seconds = loop.time() - stable_since
        gate.release()
        await asyncio.wait_for(response.done.wait(), timeout=15)
        assert not response.error, response.error
        assert (b":status", b"200") in response.headers, response.headers
        expected = hashlib.sha256()
        for _ in range(SLOW_BYTES // len(SLOW_CHUNK)):
            expected.update(SLOW_CHUNK)
        assert response.body_bytes == SLOW_BYTES, response.body_bytes
        assert response.digest.digest() == expected.digest()
        grants_after = [frame["maximum"] for frame in frames(trace, "sent", frozen_end)
                        if frame["frame_type"] == "max_stream_data" and frame["stream_id"] == stream]
        assert grants_after and max(grants_after) > STREAM_CREDIT, grants_after
        assert await complete(client.request(authority, "/after-credit-resume")[1]) == b"origin"
        return {"initialStreamCredit": STREAM_CREDIT, "frozenReceivedOffset": STREAM_CREDIT,
                "frozenMaximumStreamDataGrants": grants, "originSentOffsetDuringStall": offset,
                "stableOriginSamples": stable_samples, "stableSeconds": stalled_seconds,
                "pingACKProgress": True, "completedSiblingsDuringStall": siblings,
                "resumedResponseBytes": SLOW_BYTES, "resumedSHA256": expected.hexdigest(),
                "resumedMaximumStreamData": max(grants_after),
                "accountingScope": "reserved storage and sampled native records; excludes RSS and derived Frames"}
    finally:
        gate.release()
        evidence.parent.mkdir(parents=True, exist_ok=True)
        evidence.write_text(json.dumps(samples, indent=2), encoding="utf-8")
