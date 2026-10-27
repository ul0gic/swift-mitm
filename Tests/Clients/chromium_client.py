import argparse
import hashlib
import json
from pathlib import Path
import ssl
import sys
import tempfile
from urllib.parse import urlsplit

from playwright.sync_api import Error as PlaywrightError, sync_playwright


def certificate_facts(certificates, expected_fingerprint):
    fingerprints = [
        hashlib.sha256(ssl.PEM_cert_to_DER_cert(certificate)).hexdigest()
        for certificate in certificates[:8]
    ]
    root_present = expected_fingerprint in fingerprints
    return {
        "chain_count": len(certificates),
        "chain_fingerprints": fingerprints,
        "expected_root_present": root_present if root_present or len(certificates) <= 8 else None,
        "chain_truncated": len(certificates) > 8,
    }


def report_tls_diagnostic(path, expected_ca, hostname):
    try:
        with path.open("rb") as stream:
            content = stream.read(8 * 1024 * 1024 + 1)
        if len(content) > 8 * 1024 * 1024:
            print("chromium: tls-diagnostic=log-read-limit", file=sys.stderr, flush=True)
            return
        log = json.loads(content)
        expected = hashlib.sha256(ssl.PEM_cert_to_DER_cert(expected_ca.read_text())).hexdigest()
        constants = log["constants"]
        event_type = constants["logEventTypes"]["CERT_VERIFY_PROC"]
        phases = constants["logEventPhase"]
        sources = {
            event["source"]["id"]
            for event in log["events"]
            if event["type"] == event_type and event.get("params", {}).get("host") == hostname
        }
        count = 0
        for event in log["events"]:
            if event["type"] != event_type or event["source"]["id"] not in sources:
                continue
            params = event.get("params", {})
            if event["phase"] == phases["PHASE_BEGIN"]:
                facts = certificate_facts(params["certificates"], expected)
                facts["phase"] = "presented-chain"
            elif event["phase"] == phases["PHASE_END"]:
                facts = certificate_facts(params["verified_cert"]["certificates"], expected)
                facts.update(phase="verification", net_error=params.get("net_error", 0),
                             cert_status=params["cert_status"])
            else:
                continue
            print("chromium: tls-diagnostic=" + json.dumps(facts), file=sys.stderr, flush=True)
            count += 1
            if count == 4:
                break
        if count == 0:
            print("chromium: tls-diagnostic=no-target-verification", file=sys.stderr, flush=True)
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"chromium: tls-diagnostic-unavailable={type(error).__name__}", file=sys.stderr, flush=True)


parser = argparse.ArgumentParser()
parser.add_argument("--executable", required=True)
parser.add_argument("--proxy", required=True)
parser.add_argument("--url", required=True)
parser.add_argument("--expected-ca", required=True, type=Path)
arguments = parser.parse_args()
print("chromium: driver-start", file=sys.stderr, flush=True)
directory = tempfile.TemporaryDirectory(prefix="swiftmitm-chromium-")
netlog = Path(directory.name) / "netlog.json"
try:
    with sync_playwright() as playwright:
        print("chromium: browser-launch", file=sys.stderr, flush=True)
        browser = playwright.chromium.launch(
            executable_path=arguments.executable,
            headless=True,
            proxy={"server": arguments.proxy, "bypass": "<-loopback>"},
            args=[f"--log-net-log={netlog}", "--net-log-max-size-mb=4"],
        )
        try:
            context = browser.new_context(ignore_https_errors=False)
            page = context.new_page()
            print("chromium: navigate", file=sys.stderr, flush=True)
            page.goto(arguments.url, wait_until="domcontentloaded", timeout=15000)
            print("chromium: application-barriers", file=sys.stderr, flush=True)
            page.wait_for_function("window.__swiftmitmDone === true", timeout=15000)
            assert page.locator("body").inner_text() == "SwiftMITM browser passed"
            print(json.dumps({"chromium": browser.version, "sse": True, "websocket": True, "trusted": True}))
            print("chromium: context-close", file=sys.stderr, flush=True)
            context.close()
        finally:
            original_error = sys.exception()
            print("chromium: browser-close", file=sys.stderr, flush=True)
            try:
                browser.close()
            except PlaywrightError as error:
                if original_error is None:
                    raise
                print(f"chromium: close-failed={type(error).__name__}", file=sys.stderr, flush=True)
        print("chromium: driver-close", file=sys.stderr, flush=True)
finally:
    original_error = sys.exception()
    report_tls_diagnostic(netlog, arguments.expected_ca, urlsplit(arguments.url).hostname)
    try:
        directory.cleanup()
    except OSError as error:
        if original_error is None:
            raise
        print(f"chromium: diagnostic-cleanup-failed={type(error).__name__}", file=sys.stderr, flush=True)
print("chromium: stopped", file=sys.stderr, flush=True)
