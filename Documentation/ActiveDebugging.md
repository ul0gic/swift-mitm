# Pause and edit an HTTP request

`InterceptionLab` demonstrates a public `HTTPControlRuntime` breakpoint for requests whose target is exactly `/debug` with no query. It keeps the proxy passive for other requests. When the breakpoint arrives, the console consumer resolves it with either the original request or one configured header edit. This example uses a command-line decision so no input thread can hold shutdown open.

## Start the proxy with an edit

Use [Run the local interception lab](LocalLab.md) to generate isolated CA credentials and start repository-owned peers. The commands below assume your own origin is already running.

Use a matching P-256 interception CA certificate and private key. From `Examples/InterceptionLab`:

```sh
swift run InterceptionLab serve ca-cert.pem ca-key.pem --edit-header X-Lab=reviewed
```

The listener binds to `127.0.0.1:8080`. It does not install trust or change system proxy settings. The client must trust `ca-cert.pem`; upstream certificate verification stays enabled. The `--edit-header` option applies only after `/debug` reaches its breakpoint; the held request resumes with the edited decision. Without that option, the request resumes unchanged. The hold is visible as `paused ...`; the edited request then continues through the normal forwarding path.

Issue a request from another terminal:

```sh
curl --noproxy '' --proxy http://127.0.0.1:8080 --cacert ca-cert.pem https://localhost:9443/debug
```

This command expects an HTTPS origin you control at `localhost:9443` with a `/debug` endpoint. For an internal origin whose certificate chains to `origin-ca.pem`, start the proxy with `--allow-internal --origin-ca origin-ca.pem`. Internal egress is denied by default. Additional roots supplement system trust; verification is never disabled.

The example removes any existing field with the configured name and appends the supplied value. To permit an internal origin, add `--allow-internal --origin-ca origin-ca.pem`; internal destinations are denied by default, and the opt-in applies to internal destinations generally rather than only `localhost`. It does not expose body replacement, routing, arbitrary scripts or a GUI. If the configuration rejects the edit, it rejects the held request. A client disconnect, shutdown or deadline can also finish a hold before the decision is applied; a completed handle cannot be resolved twice.

The control runtime uses `HTTPControlLimits` defaults: 256 rules, 256 KiB configuration, 16 callback tasks, 1 MiB per control body and 16 MiB retained control data. Replay uses `ReplayConfiguration` defaults of eight concurrent attempts and a 1 MiB request body. These bounds can be changed by an embedding application through the public configuration values.

The explicit proxy's outer CONNECT handshake bypasses HTTP control evaluation. Within an intercepted connection, edits, breakpoints and faults on protocol-upgrade handshakes are unsupported; matching unsupported actions fail rather than modifying upgraded traffic. Unmatched WebSocket traffic retains its existing forwarding and observation behavior. A WebSocket message observation is not an HTTP-body edit capability.

## Configuration lifetime

A control runtime belongs to one active proxy server. Stopping and restarting a server does not erase retained callback charges; noncooperative callbacks remain charged until they exit. A concurrent server cannot share the same runtime.

TLS HTTP/1 tunnels opened with an empty rule snapshot keep their raw forwarding path for that connection. Adding rules affects new tunnels; existing controlled HTTP/1 connections and explicit HTTP requests select snapshots per message, and HTTP/2 selects them per new stream. Response rules use the admitted request snapshot.

## Replay and redacted HAR export

Replay one explicit GET through the library's egress and upstream TLS policy:

```sh
swift run InterceptionLab replay ca-cert.pem ca-key.pem https://localhost:9443/resource replay.har --allow-internal --origin-ca origin-ca.pem
```

The command expects an HTTPS origin you control at `localhost:9443`; the flags explicitly permit internal egress and trust `origin-ca.pem` in addition to system roots. Without `replay.har`, it reports response metadata only. With a path, it overwrites that path with a streamed HAR 1.2 document through the public `HARExporter` API when the replay result includes measured send/wait/receive timings and a known decoded response content size. Timings are in milliseconds and come from monotonic transport boundaries; the example does not infer them from HTTP head timestamps. Synthetic responses, incomplete uploads, overlapping exchanges and unknown decoded content sizes produce an explicit export error. The sample omits bodies, includes the replay request and response heads, and applies the exporter's built-in credential-header and query-name redaction. Review the file before sharing it because application-specific secrets may use other header or query names.

The replay is a new GET with no copied headers or body. Its `ReplaySource` identifies a new source request ID and the `.original` representation; `ReplayResult` returns the attempt's separate request ID. It strips credentials by construction, uses one explicit destination, and does not follow redirects or retry. The sample briefly starts its loopback listener on an automatically selected port because `ProxyServer.replay` uses the server's event loop; it does not print or expose that port. Upstream TLS verification remains enabled.

This is consumer behavior, not acceptance evidence. Replay and export interoperability remain unverified until the final candidate's GitHub Actions checks complete.

## Admission overload

When the optional shared stream limit rejects a raw HTTP/1 request, the proxy closes that overloaded connection and reports its unfinished pipelined captures as failed. It cannot reset one raw HTTP/1 exchange independently. Other connections remain active; HTTP/2 stream rejection is scoped to the rejected stream.
