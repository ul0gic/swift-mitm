# SwiftMITM 0.2.4

## Fixed

- Preserve the client's TLS server name when controlled HTTP/1.1 reconnects to the original destination. An explicit route change uses the new authority for upstream TLS verification.
- Keep one global and one per-destination connection slot reserved across IPv4/IPv6 attempts until the winning socket closes or all attempts fail.
- Reject intercepted TLS connections at the configured connection limit without triggering a SwiftNIO debug assertion over unused DNS promises.
- Report distinct SSE byte ranges for events delivered together or across reads. Compressed events report the wire prefix consumed when each event was emitted.
- Refresh the FirstCapture example's dependency pins so it resolves with the package's Swift Crypto 5 requirement in frozen builds.
