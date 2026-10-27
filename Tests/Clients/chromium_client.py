import argparse
import json
import sys

from playwright.sync_api import sync_playwright

parser = argparse.ArgumentParser()
parser.add_argument("--executable", required=True)
parser.add_argument("--proxy", required=True)
parser.add_argument("--url", required=True)
arguments = parser.parse_args()
print("chromium: driver-start", file=sys.stderr, flush=True)
with sync_playwright() as playwright:
    print("chromium: browser-launch", file=sys.stderr, flush=True)
    browser = playwright.chromium.launch(
        executable_path=arguments.executable,
        headless=True,
        proxy={"server": arguments.proxy, "bypass": "<-loopback>"},
    )
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
    print("chromium: browser-close", file=sys.stderr, flush=True)
    browser.close()
    print("chromium: driver-close", file=sys.stderr, flush=True)
print("chromium: stopped", file=sys.stderr, flush=True)
