import asyncio
import hashlib
import json
from pathlib import Path

from aioquic.asyncio.client import connect

from fidelity import field_hex, octet_headers
from production_origin import ORIGINAL_FIELDS
from verify import Client, configuration, response_complete


CONTROL_CASES = ("control-headers", "control-body", "control-synthetic", "control-breakpoint", "control-delay")
APPENDED_FIELDS = [(b"x-text", b"\xc3\xbf"), (b"x-insert", b"\x80\xff"),
                   (b"x-replaced", b"\xc3\xbf"), (b"x-marker", b"edited")]
EDITED_FIELDS = [(name, value) for name, value in ORIGINAL_FIELDS if name != b"x-replaced"] + APPENDED_FIELDS


def host_configuration(name):
    return {"controlPreset": name.removeprefix("control-"), "observe": True,
            "sharedRetainedBytes": 67108864}


async def wait_json(path, predicate):
    async with asyncio.timeout(8):
        while True:
            if Path(path).exists():
                value = json.loads(Path(path).read_text(encoding="utf-8"))
                if predicate(value):
                    return value
            await asyncio.sleep(0.05)


async def command(prefix, identifier, action):
    path = Path(prefix + ".control-command.json")
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps({"id": identifier, "action": action}), encoding="utf-8")
    temporary.replace(path)
    return await wait_json(prefix + ".control-ack.json", lambda value: value["id"] == identifier)


def request(client, authority, path, fields, body=None):
    stream, response = client.raw_request()
    headers = [(b":method", b"GET" if body is None else b"POST"), (b":scheme", b"https"),
               (b":authority", authority.encode("ascii")), (b":path", path.encode("ascii"))] + fields
    client._quic.send_stream_data(stream, octet_headers(headers), end_stream=body is None)
    if body is not None:
        client.http.send_data(stream, body, end_stream=True)
    client.transmit()
    return stream, response


async def exercise_control(name, ready, authority, certificate, peer):
    async with connect("127.0.0.1", ready["port"], configuration=configuration(certificate, "localhost"),
                       create_protocol=Client) as client:
        assert client.alpn == "h3"
        if name == "control-headers":
            _, response = request(client, authority, "/production/headers", ORIGINAL_FIELDS)
            assert await response_complete(response) == b"origin"
            assert response.headers == [(b":status", b"200")] + EDITED_FIELDS, field_hex(response.headers)
            stats = await wait_json(peer["stats"], lambda value: len(value.get("productionRequests", [])) == 1)
            observed = [[n, v] for n, v in stats["productionRequests"][0]["headers"]
                        if not bytes.fromhex(n).startswith(b":")]
            assert observed == field_hex(EDITED_FIELDS), observed
            return {"wireFields": field_hex(EDITED_FIELDS), "rawFFDistinctFromTextFF": True}
        if name == "control-body":
            return await body_edits(client, authority, peer)
        if name == "control-synthetic":
            _, response = client.request(authority, "/production/respond")
            await asyncio.wait_for(response.done.wait(), timeout=8)
            assert not response.error and (b":status", b"201") in response.headers, response.error
            assert (b"x-synthetic", b"yes") in response.headers and bytes(response.body) == b"synthetic"
            assert json.loads(Path(peer["stats"]).read_text())["requests"] == 0
            return {"status": 201, "syntheticBody": "synthetic", "originRequests": 0}
        if name == "control-breakpoint":
            _, response = client.request(authority, "/production/paused")
            await wait_json(ready["prefix"] + ".control-events.json",
                            lambda value: any(record["kind"] == "breakpoint" for record in value["records"]))
            assert not response.done.is_set()
            assert json.loads(Path(peer["stats"]).read_text())["requests"] == 0
            ack = await command(ready["prefix"], "resume-1", "resolve")
            assert ack["resolutions"] == ["resumed"], ack
            assert await response_complete(response) == b"origin"
            stats = await wait_json(peer["stats"], lambda value: len(value.get("productionRequests", [])) == 1)
            assert [b"x-resumed".hex(), b"yes".hex()] in stats["productionRequests"][0]["headers"]
            return {"pausedBeforeOrigin": True, "publicHandleEditedResume": True}
        assert name == "control-delay", name
        start = asyncio.get_running_loop().time()
        _, delayed = client.request(authority, "/production/delayed")
        assert await response_complete(client.request(authority, "/production/sibling")[1]) == b"origin"
        assert not delayed.done.is_set(), "delayed stream ended before its independent sibling"
        assert await response_complete(delayed) == b"origin"
        elapsed = asyncio.get_running_loop().time() - start
        assert elapsed >= 1.0, elapsed
        return {"configuredDelayMilliseconds": 1000, "elapsedSeconds": elapsed, "siblingBeforeDelayed": True}


async def body_edits(client, authority, peer):
    _, accepted = request(client, authority, "/production/replace-request", [], b"aaaa")
    assert await response_complete(accepted) == b"edited"
    for body, fields in ((b"aaaaa", []), (b"aaaa", [(b"cache-control", b"no-transform")])):
        _, rejected = request(client, authority, "/production/replace-request", fields, body)
        await asyncio.wait_for(rejected.done.wait(), timeout=8)
        assert rejected.error and rejected.error.startswith("stream reset:"), rejected.error
        assert client.connection_error is None
    _, response = client.request(authority, "/production/replace-response")
    assert await response_complete(response) == b"replacement"
    assert (b"content-length", b"11") in response.headers, response.headers
    assert all(name != b"content-md5" for name, _ in response.headers + response.trailers)
    assert response.trailers == [(b"x-trailer", b"keep")], response.trailers
    stats = await wait_json(peer["stats"], lambda value: len(value.get("productionRequests", [])) == 2)
    assert [record["path"] for record in stats["productionRequests"]] == [
        "/production/replace-request", "/production/replace-response"]
    assert stats["productionRequests"][0]["body"] == b"edited".hex(), stats
    return {"acceptedInputBytes": 4, "rejectedInputBytes": 5, "noTransformRejected": True,
            "responseReplacement": "replacement", "staleMetadataRemoved": True, "trailerPreserved": True}


def final_control_oracles(name, ready):
    prefix = ready["prefix"]
    embedding = json.loads(Path(prefix + ".embedding.json").read_text())
    assert all(value == 0 for value in embedding["control"].values()), embedding
    assert all(value == 0 for value in embedding["shared"].values()), embedding
    assert embedding["stopReport"] == {"abandonedCallbacks": 0, "retainedCallbackBytes": 0}, embedding
    originals = json.loads(Path(prefix + ".original.json").read_text())
    effective = json.loads(Path(prefix + ".effective.json").read_text())
    bodies = json.loads(Path(prefix + ".bodies.json").read_text())
    if name == "control-headers":
        for events, expected in ((originals, ORIGINAL_FIELDS), (effective, EDITED_FIELDS)):
            heads = [event for event in events if event["kind"] in ("requestHead", "responseHead")]
            assert len(heads) == 2, heads
            for head in heads:
                fields = [(field["name"].encode("ascii"), bytes(field["rawValueBytes"]))
                          for field in head["headers"] if field["name"].startswith("x-")]
                assert fields == expected, fields
        assert originals[0]["requestID"] == effective[0]["requestID"]
    if name == "control-body":
        request_head = next(event for event in originals if event["kind"] == "requestHead"
                            and event["path"] == "/production/replace-request")
        identifier = request_head["requestID"]
        for representation, body in (("original", b"aaaa"), ("effective", b"edited")):
            observed = bodies[f"{identifier}:request:{representation}"]
            assert observed == {"byteCount": len(body), "sha256": hashlib.sha256(body).hexdigest(), "unavailable": False}, observed
    if name == "control-synthetic":
        assert not any(event["kind"] == "responseHead" for event in originals), originals
        assert any(event["kind"] == "responseHead" and event["status"] == 201 for event in effective), effective
    return {"originalEffectiveObserved": True, "sharedAndControlReleased": True}
