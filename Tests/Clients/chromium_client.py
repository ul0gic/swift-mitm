import argparse
import json

from playwright.sync_api import sync_playwright

parser = argparse.ArgumentParser()
parser.add_argument("--executable", required=True)
parser.add_argument("--proxy", required=True)
parser.add_argument("--url", required=True)
arguments = parser.parse_args()
with sync_playwright() as playwright:
    browser = playwright.chromium.launch(
        executable_path=arguments.executable,
        headless=True,
        proxy={"server": arguments.proxy, "bypass": "<-loopback>"},
    )
    context = browser.new_context(ignore_https_errors=False)
    page = context.new_page()
    page.goto(arguments.url, wait_until="domcontentloaded", timeout=15000)
    page.wait_for_function("window.__swiftmitmDone === true", timeout=15000)
    assert page.locator("body").inner_text() == "SwiftMITM browser passed"
    print(json.dumps({"chromium": browser.version, "sse": True, "websocket": True, "trusted": True}))
    context.close()
    browser.close()
