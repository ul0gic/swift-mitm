# HTTP/3 status

HTTP/3 interception is unavailable in the current candidate. Do not configure this package as an HTTP/3 proxy or claim HTTP/3 capture, replay, editing or inspection.

`Packages/SwiftMITMHTTP3` contains two experimental executables: `HTTP3DependencyProbe` checks the combined dependency graph, and `HTTP3TransportProbe` exercises a fixed-route, two-leg HTTP/3 connection against independent peers. It exports no transport library and defines no public `HTTP3ProxyServer`. These development probes are not the package’s supported HTTP/3 API.

The researched Apple SwiftNIO QUIC and HTTP/3 revisions require Crypto 5, while the core currently depends on Crypto 4. The companion package isolates dependency resolution from core-only consumers, but combining both packages still requires one compatible Crypto graph. Core support remains macOS 26; the researched HTTP/3 handlers require macOS 27. The probe does not establish a core deployment-floor increase or a Crypto major-version change.

The planned boundary is a separately resolved, route-bound HTTP/3 capability on macOS 27 or later, after remote dependency-graph, macOS 26 core compatibility, identity-conversion, two-leg streaming and independent-peer evidence all pass. Until then, use supported HTTP/1.1 or HTTP/2 paths. HTTP/3 is not transparent QUIC interception and does not imply WebTransport, HTTP Datagrams, MASQUE, upstream HTTP/3 mutual TLS or WebSocket over HTTP/3 support.
