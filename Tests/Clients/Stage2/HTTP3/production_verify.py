import argparse
import asyncio
from contextlib import AsyncExitStack
import ipaddress
import json
import os
from pathlib import Path
import sys
import tempfile
import traceback

import aioquic
from aioquic.asyncio.client import connect
from cryptography import x509

from verify import (
    Client, configuration, create_identity, fragmented_progress, hostile_headers, octet_fidelity,
    positive, readiness, rejected, response_complete, retain_flow_trace, retain_tail, separated_fin, shutdown,
)
from lifecycle import sni_policy
from fidelity import OCTET_FIELDS
from flow import FlowLogger, STREAM_CREDIT
from production_flow import cancel_churn, credit_stall, storage_ceiling
from production_controls import CONTROL_CASES, exercise_control, final_control_oracles, host_configuration
from production_lifecycle import CASES as LIFECYCLE_CASES, run_case as run_lifecycle_case
from production_security import SECURITY_CASES, run_security_case
from production_memory import CASES as MEMORY_CASES, run_case as run_memory_case
from production_maximum_cycles import CASES as MAXIMUM_CYCLE_CASES, run_case as run_maximum_cycle_case
from production_ownership import CASES as OWNERSHIP_CASES, run_case as run_ownership_case
from production_migration import CASES as MIGRATION_CASES, run_case as run_migration_case
from production_upload import CASES as UPLOAD_CASES, run_case as run_upload_case
from production_early_data import CASES as EARLY_DATA_CASES, run_case as run_early_data_case
from production_deadlines import CASES as DEADLINE_CASES, run_case as run_deadline_case
from production_shared_ceiling import CASES as SHARED_CEILING_CASES, run_case as run_shared_ceiling_case
from production_resources import production_goaway, read_resources, stream_limit


CASES = (
    "streaming", "ip-san", "changed-authority", "upstream-hostname", "upstream-trust", "downstream-trust",
    "denied-route", "separated-fin", "oversized-request-headers", "oversized-response-headers",
    "fragmented-headers", "sni-policy", "goaway-drain", "octet-fidelity", "request-stream-limit",
    "slow-receiver", "cancelled-credit-churn", "storage-refusal", "maximum-storage-refusal",
) + CONTROL_CASES + LIFECYCLE_CASES + SECURITY_CASES + MEMORY_CASES + OWNERSHIP_CASES + MIGRATION_CASES + UPLOAD_CASES + EARLY_DATA_CASES + DEADLINE_CASES + SHARED_CEILING_CASES + MAXIMUM_CYCLE_CASES


async def exercise(name, ready, authority, certificate, peer, artifacts):
    if name in CONTROL_CASES:
        return await exercise_control(name, ready, authority, certificate, peer)
    if name in ("slow-receiver", "cancelled-credit-churn"):
        return await exercise_flow(name, ready, authority, certificate, peer, artifacts)
    if name in ("storage-refusal", "maximum-storage-refusal"):
        return await exercise_storage(name, ready, authority, certificate, artifacts)
    if name == "streaming":
        return await positive(ready["port"], authority, certificate, diagnostics_directory=artifacts)
    if name == "separated-fin":
        return await separated_fin(ready["port"], authority, certificate, peer)
    if name in ("oversized-request-headers", "oversized-response-headers"):
        return await hostile_headers(ready["port"], authority, certificate, peer,
                                     upstream=name == "oversized-response-headers")
    if name == "fragmented-headers":
        return await fragmented_progress(ready["port"], authority, certificate)
    if name == "sni-policy":
        return await sni_policy(ready, authority, certificate, peer, artifacts,
                                Client, configuration, response_complete)
    if name == "goaway-drain":
        return await production_goaway(ready, authority, certificate, peer, Client,
                                       configuration, response_complete, connect)
    if name == "octet-fidelity":
        return await octet_fidelity(ready, authority, certificate, peer, Client, configuration, response_complete)
    hostname = "127.0.0.1" if name == "ip-san" else "localhost"
    async with connect("127.0.0.1", ready["port"], configuration=configuration(certificate, hostname),
                       create_protocol=Client) as client:
        assert client.alpn == "h3"
        if name == "request-stream-limit":
            return await stream_limit(client, authority, ready["resourcesPath"], response_complete)
        assert name == "ip-san", name
        certificate = client._quic.tls._peer_certificate
        names = certificate.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
        assert ipaddress.ip_address("127.0.0.1") in names.get_values_for_type(x509.IPAddress)
        assert await response_complete(client.request(authority, "/ip-san")[1]) == b"origin"
    return {"verifiedIPIdentity": "127.0.0.1", "downstreamIPSAN": True, "downstreamALPN": "h3"}


async def exercise_flow(name, ready, authority, certificate, peer, artifacts):
    logger = FlowLogger()
    config = configuration(certificate, "localhost")
    config.max_stream_data = STREAM_CREDIT
    config.quic_logger = logger
    try:
        async with connect("127.0.0.1", ready["port"], configuration=config, create_protocol=Client) as client:
            assert client.alpn == "h3"
            if name == "cancelled-credit-churn":
                return await cancel_churn(client, authority, ready["resourcesPath"], response_complete,
                                          artifacts / "churn-samples.json")
            return await credit_stall(client, authority, peer, ready["resourcesPath"], response_complete,
                                      artifacts / "stall-samples.json")
    finally:
        logger.write(artifacts / "client.qlog")


async def exercise_storage(name, ready, authority, certificate, artifacts):
    maximum = name == "maximum-storage-refusal"
    async with AsyncExitStack() as stack:
        clients = []
        for _ in range(8 if maximum else 4):
            client = await stack.enter_async_context(connect(
                "127.0.0.1", ready["port"], configuration=configuration(certificate, "localhost"), create_protocol=Client))
            assert client.alpn == "h3"
            clients.append(client)
        return await storage_ceiling(clients, authority, ready["resourcesPath"], response_complete,
                                     artifacts / "admission-samples.json", 268435456 if maximum else 67108864)


def capture_oracle(path, authority, peer, name):
    events = json.loads(Path(path).read_text(encoding="utf-8"))
    assert not any(event["kind"] == "unexpectedNonHTTPEvent" for event in events), events
    heads = [event for event in events if event["kind"] == "requestHead"]
    for head in heads:
        assert head["version"] == "HTTP/3" and head["provenance"] == "fixedRouteHTTP3", head
        assert head["authority"] == authority, head
        assert head["destinationAddress"] == "127.0.0.1" and head["destinationPort"] == peer["port"], head
    for event in events:
        if event["kind"] == "responseHead":
            assert event["version"] == "HTTP/3", event
    if name == "streaming":
        selected = next(head for head in heads if head["path"] == "/duplex")
        exchange = [event for event in events if event.get("requestID") == selected["requestID"]]
        for kind in ("requestHead", "requestEnd", "responseHead", "responseEnd"):
            assert sum(event["kind"] == kind for event in exchange) == 1, exchange
        assert not any(event["kind"] == "streamError" for event in exchange), exchange
    if name == "octet-fidelity":
        selected = next(head for head in heads if head["path"] == "/octet-fidelity")
        exchange = [event for event in events if event.get("requestID") == selected["requestID"]]
        field_events = [event for event in exchange if event["kind"] in (
            "requestHead", "requestTrailers", "responseHead", "responseTrailers")]
        assert len(field_events) == 5, field_events
        for event in field_events:
            observed = [(field["name"].encode("ascii"), bytes(field["rawValueBytes"]))
                        for field in event["headers"] if field["name"] in ("x-octet", "x-huffman")]
            assert observed == OCTET_FIELDS, observed
    return {"requestHeads": len(heads), "events": len(events), "publicHTTP3Provenance": bool(heads)}


async def run_case(binary, name, artifacts):
    with tempfile.TemporaryDirectory(prefix="swiftmitm-production-h3-") as temporary:
        directory = Path(temporary)
        wrong_directory = directory / "wrong-ca"
        wrong_directory.mkdir()
        wrong_certificate, _ = create_identity(wrong_directory)
        origin_ready = directory / "origin" / "ready.json"
        ready_path = directory / "host-ready.json"
        environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
        environment["SWIFTMITM_HTTP3_WRITE_TRACE"] = "1" if name == "streaming" else "0"
        with (directory / "origin.log").open("wb") as origin_log, (directory / "host.log").open("wb") as host_log:
            script = "production_origin.py" if name in CONTROL_CASES else "peer.py"
            origin_arguments = [sys.executable, str(Path(__file__).with_name(script)), "--ready-file", str(origin_ready)]
            if name == "slow-receiver":
                origin_arguments.append("--flow-proof")
            origin = await asyncio.create_subprocess_exec(
                *origin_arguments,
                stdout=origin_log, stderr=asyncio.subprocess.STDOUT, env=environment
            )
            host = None
            try:
                peer = await readiness(origin_ready, origin)
                hostname = "wrong-host.invalid" if name == "upstream-hostname" else (
                    "127.0.0.1" if name == "ip-san" else "localhost")
                authority = f"{hostname}:443"
                config = {"authority": authority, "destinationAddress": "127.0.0.1", "destinationPort": peer["port"],
                          "originCAPath": str(wrong_certificate if name == "upstream-trust" else peer["certificate"]),
                          "readyPath": str(ready_path), "allowInternal": name != "denied-route",
                          "maximumConnections": 128, "maximumPendingHandshakes": 64,
                          "maximumRequestStreamsPerConnection": 2 if name == "request-stream-limit" else 128,
                          "maximumRetainedBytes": 67108864}
                if name == "maximum-storage-refusal":
                    config.update(maximumConnections=1024, maximumPendingHandshakes=256,
                                  maximumRequestStreamsPerConnection=1024, maximumRetainedBytes=268435456)
                if name in CONTROL_CASES:
                    config.update(host_configuration(name))
                config_path = directory / "configuration.json"
                config_path.write_text(json.dumps(config), encoding="utf-8")
                host = await asyncio.create_subprocess_exec(
                    str(binary), "--configuration", str(config_path), stdout=host_log,
                    stderr=asyncio.subprocess.STDOUT, env=environment
                )
                if name == "denied-route":
                    await asyncio.wait_for(host.wait(), timeout=20)
                    assert host.returncode == 2 and not ready_path.exists(), host.returncode
                    assert b"HTTP3ProductionHost: egressDenied" in (directory / "host.log").read_bytes().splitlines()
                    report = {"typedStartupRejection": "egressDenied"}
                else:
                    ready = await readiness(ready_path, host)
                    certificate = Path(ready["certificate"])
                    if name in ("changed-authority", "upstream-hostname", "upstream-trust", "downstream-trust"):
                        await rejected(ready["port"], "other.invalid:443" if name == "changed-authority" else authority,
                                       wrong_certificate if name == "downstream-trust" else certificate, hostname)
                        report = {"rejected": True}
                    else:
                        report = await exercise(name, ready, authority, certificate, peer, artifacts / name)
                    await shutdown(host)
                    assert host.returncode == 0, "production host failed during the scenario or stop"
                    report["finalResources"] = read_resources(ready["resourcesPath"], stopped=True,
                                                              byte_limit=config["maximumRetainedBytes"])
                    report["capture"] = capture_oracle(ready["capturePath"], authority, peer, name)
                    if name in CONTROL_CASES:
                        report["controlOracles"] = final_control_oracles(name, ready)
                stats = json.loads(Path(peer["stats"]).read_text(encoding="utf-8"))
                if name in ("changed-authority", "upstream-hostname", "upstream-trust", "downstream-trust", "denied-route"):
                    assert stats["requests"] == 0, stats
                    if name == "denied-route":
                        assert stats["datagrams"] == 0, stats
                elif name == "control-synthetic":
                    assert stats["requests"] == 0 and stats["datagrams"] == 0, stats
                else:
                    assert stats["alpn"] and set(stats["alpn"]) == {"h3"}, stats
                    assert not stats["earlyDataAccepted"], stats
                    assert stats["peerQpackCapacity"] == 0 and stats["peerQpackBlocked"] == 0, stats
                    if name in ("streaming", "cancelled-credit-churn"):
                        assert stats["cancelSignals"] > 0, stats
                    if name == "cancelled-credit-churn":
                        assert stats["cancelSignals"] >= 32 and stats["handshakes"] == 1, stats
                    if name == "slow-receiver":
                        assert stats["handshakes"] == 1, stats
                    if name in ("storage-refusal", "maximum-storage-refusal"):
                        expected_connections = 8 if name == "maximum-storage-refusal" else 4
                        assert stats["handshakes"] == expected_connections, stats
                    if name == "separated-fin":
                        assert stats["splitRequestTrailers"] == list(range(129)), stats
                        assert stats["splitResponseFin"] == list(range(129)), stats
                    expected = {"ip-san": 1, "oversized-request-headers": 1, "oversized-response-headers": 2,
                                "fragmented-headers": 2, "sni-policy": 3, "goaway-drain": 3,
                                "octet-fidelity": 2, "request-stream-limit": 4, "separated-fin": 130,
                                "slow-receiver": 5, "cancelled-credit-churn": 65}
                    if name in expected:
                        assert stats["requests"] == expected[name], stats
                return dict(report, originRequests=stats["requests"], upstreamALPN=stats["alpn"],
                            logicalAuthority=authority, dialPort=peer["port"])
            finally:
                try:
                    if host is not None:
                        await shutdown(host)
                finally:
                    try:
                        await shutdown(origin)
                    finally:
                        for source, target in ((directory / "host.log", "host.log"),
                                               (directory / "origin.log", "origin.log"),
                                               (Path(str(ready_path) + ".resources.json"), "resources.json"),
                                               (origin_ready.parent / "stats.json", "origin-stats.json")):
                            retain_tail(source, artifacts / name / target)
                        retain_flow_trace(Path(str(ready_path) + ".capture.json"), artifacts / name / "capture.json")
                        retain_flow_trace(Path(str(ready_path) + ".write-trace.jsonl"),
                                          artifacts / name / "write-trace.jsonl")
                        retain_flow_trace(origin_ready.parent / "flow.qlog", artifacts / name / "origin.qlog")
                        for suffix in ("original", "effective", "bodies", "embedding", "control-events"):
                            retain_flow_trace(Path(str(ready_path) + f".{suffix}.json"), artifacts / name / f"{suffix}.json")


async def main(arguments):
    results = {"surface": "public SwiftMITMHTTP3.HTTP3ProxyServer", "peer": f"aioquic/{aioquic.__version__}", "checks": {}}
    artifacts = arguments.output.parent / "production-http3-peer-logs"
    for name in arguments.case or CASES:
        try:
            timeout = 150 if name in ("memory-cancel-cycles", "orphan-stop-restart", "shared-maximum-storage") else 90
            if name == "memory-maximum-cycles":
                timeout = 240
            async with asyncio.timeout(timeout):
                if name in LIFECYCLE_CASES:
                    observation = await run_lifecycle_case(arguments.binary, name, artifacts / name)
                elif name in SECURITY_CASES:
                    observation = await run_security_case(arguments.binary, name, artifacts / name)
                elif name in MEMORY_CASES:
                    observation = await run_memory_case(arguments.binary, name, artifacts / name)
                elif name in MAXIMUM_CYCLE_CASES:
                    observation = await run_maximum_cycle_case(arguments.binary, name, artifacts / name)
                elif name in OWNERSHIP_CASES:
                    observation = await run_ownership_case(arguments.binary, name, artifacts / name)
                elif name in MIGRATION_CASES:
                    observation = await run_migration_case(arguments.binary, name, artifacts / name)
                elif name in UPLOAD_CASES:
                    observation = await run_upload_case(arguments.binary, name, artifacts / name)
                elif name in EARLY_DATA_CASES:
                    observation = await run_early_data_case(arguments.binary, name, artifacts / name)
                elif name in DEADLINE_CASES:
                    observation = await run_deadline_case(arguments.binary, name, artifacts / name)
                elif name in SHARED_CEILING_CASES:
                    observation = await run_shared_ceiling_case(arguments.binary, name, artifacts / name)
                else:
                    observation = await run_case(arguments.binary, name, artifacts)
            results["checks"][name] = {"result": "passed", "observation": observation}
        except (AssertionError, ConnectionError, RuntimeError, TimeoutError, OSError, KeyError, json.JSONDecodeError,
                ExceptionGroup) as error:
            results["checks"][name] = {"result": "failed", "error": f"{type(error).__name__}: {error}",
                                      "traceback": traceback.format_exc(limit=8)}
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(json.dumps(results))
    return 0 if all(item["result"] == "passed" for item in results["checks"].values()) else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--case", action="append", choices=CASES)
    sys.exit(asyncio.run(main(parser.parse_args())))
