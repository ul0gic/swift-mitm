# Pause and edit an HTTP request

`InterceptionLab` demonstrates a public `HTTPControlRuntime` breakpoint for requests whose target is exactly `/debug` with no query. Other requests continue without pausing. When the breakpoint arrives, the console consumer resumes the original request or applies one configured header edit.

## Start the proxy with an edit

Use [Run the local interception lab](LocalLab.md) to generate isolated CA credentials and start repository-owned peers. The commands below assume your own origin is already running.

The example expects an HTTPS origin at `localhost:9443` with a `/debug` endpoint. Put the matching P-256 interception CA certificate and private key in `$HOME/.swiftmitm/` and the origin's CA certificate in `$HOME/.swiftmitm/origin-ca.pem`. Start the proxy from `Examples/InterceptionLab`.

```sh
swift run InterceptionLab serve "$HOME/.swiftmitm/ca-cert.pem" "$HOME/.swiftmitm/ca-key.pem" \
  --allow-internal --origin-ca "$HOME/.swiftmitm/origin-ca.pem" --edit-header X-Lab=reviewed
```

The listener binds to `127.0.0.1:8080`. The command permits internal destinations for this proxy process and adds the origin CA to its trust roots. At the breakpoint the console prints `paused GET /debug` and resumes the request with the configured `X-Lab` header. Without `--edit-header`, the request resumes unchanged.

Issue a request from another terminal:

```sh
curl --noproxy '' --proxy http://127.0.0.1:8080 --cacert "$HOME/.swiftmitm/ca-cert.pem" https://localhost:9443/debug
```

Curl trusts the interception CA for this request. The example replaces any existing field with the configured name. If the configuration rejects the edit, it rejects the held request. A disconnect, shutdown, or deadline can also end the hold before the decision is applied.

The control runtime uses `HTTPControlLimits` defaults: 256 rules, 256 KiB configuration, 16 callback tasks, 1 MiB per control body and 16 MiB retained control data. Replay uses `ReplayConfiguration` defaults of eight concurrent attempts and a 1 MiB request body. These bounds can be changed by an embedding application through the public configuration values.

The explicit proxy's outer CONNECT handshake bypasses HTTP control evaluation. Within an intercepted connection, edits, breakpoints and faults on protocol-upgrade handshakes are unsupported; matching unsupported actions fail rather than modifying upgraded traffic. Unmatched WebSocket traffic retains its existing forwarding and observation behavior. A WebSocket message observation is not an HTTP-body edit capability.

`HTTPFault.resetAfterBytes` counts forwarded body payload bytes in the selected direction, after body replacement and before HTTP framing. Headers and trailers do not count; content-encoded bodies remain encoded for this counter.

## Configuration lifetime

A control runtime belongs to one active proxy server. Stopping and restarting a server does not erase retained callback charges; noncooperative callbacks remain charged until they exit. A concurrent server cannot share the same runtime.

TLS HTTP/1 tunnels opened with an empty rule snapshot keep their raw forwarding path for that connection. Adding rules affects new tunnels; existing controlled HTTP/1 connections and explicit HTTP requests select snapshots per message, and HTTP/2 selects them per new stream. Response rules use the admitted request snapshot.

## Replay and redacted HAR export

Replay one explicit GET through the library's egress and upstream TLS policy:

```sh
swift run InterceptionLab replay "$HOME/.swiftmitm/ca-cert.pem" "$HOME/.swiftmitm/ca-key.pem" \
  https://localhost:9443/resource replay.har --allow-internal --origin-ca "$HOME/.swiftmitm/origin-ca.pem"
```

The command expects an HTTPS origin you control at `localhost:9443`; the flags explicitly permit internal egress and trust `origin-ca.pem` in addition to system roots. Without `replay.har`, it reports response metadata only. With a path, it overwrites that path with a streamed HAR 1.2 document through the public `HARExporter` API when the replay result includes measured send/wait/receive timings and a known decoded response content size. Timings are in milliseconds and come from monotonic transport boundaries; the example does not infer them from HTTP head timestamps. Synthetic responses, incomplete uploads, overlapping exchanges and unknown decoded content sizes produce an explicit export error. The sample omits bodies, includes the replay request and response heads, and applies the exporter's built-in credential-header and query-name redaction. Review the file before sharing it because application-specific secrets may use other header or query names.

The replay is a new GET with no copied headers or body. Its `ReplaySource` identifies a new source request ID and the `.original` representation; `ReplayResult` returns the attempt's separate request ID. It strips credentials by construction, uses one explicit destination, and does not follow redirects or retry. The sample briefly starts its loopback listener on an automatically selected port because `ProxyServer.replay` uses the server's event loop; it does not print or expose that port. Upstream TLS verification remains enabled.

The application owns replay authorization and export storage. A successful origin response does not establish a complete or redacted export; inspect the HAR and handle typed export errors.

## Admission overload

With shared admission enabled, an accepted CONNECT client and its upstream connection each consume a global connection slot. The upstream also consumes a per-destination slot. IPv4 and IPv6 attempts for one upstream dial share that slot until the winner closes or all attempts fail.

When the optional shared stream limit rejects a raw HTTP/1 request, the proxy closes that overloaded connection and reports its unfinished pipelined captures as failed. It cannot reset one raw HTTP/1 exchange independently. Other connections remain active; HTTP/2 stream rejection is scoped to the rejected stream.
