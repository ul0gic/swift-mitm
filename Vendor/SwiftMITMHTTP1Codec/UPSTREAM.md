# Byte-preserving HTTP/1 codec

This target is a narrow source fork of Apple SwiftNIO 2.104.0, revision `6db9d9abd92ad7399b8a564cafbb87d3507245fa`. It uses the official `NIOCore` and `NIOHTTP1` products and the separately named `CSwiftMITMLLHTTP` target. It does not replace or duplicate NIO's public message, buffer, channel or error types.

The source comes from these files at [the pinned upstream revision](https://github.com/apple/swift-nio/tree/6db9d9abd92ad7399b8a564cafbb87d3507245fa/Sources/NIOHTTP1): `HTTPDecoder.swift`, `HTTPEncoder.swift`, `HTTPHeaders+Validation.swift`, `HTTPServerRequestMethodQueue.swift`, and the header writer/request-body classification in `HTTPTypes.swift`. `LICENSE.txt` and `NOTICE-SwiftNIO.txt` retain the upstream Apache license and notice.

## Deliberate differences

- Distinct `BytePreservingHTTPDecoder`, request/response aliases and encoder classes avoid shadowing official NIO codecs.
- Received field values and trailers map each byte to its equal-valued Unicode scalar; encoding applies the inverse mapping.
- Outbound head/trailer validation rejects invalid names, prohibited controls and scalars above U+00FF before emitting that write or changing encoder state.
- Public `HTTPHeaders` construction/iteration replaces upstream internal storage and cached keep-alive access.
- `BytePreservingHTTP1MessageMetadata.isKeepAlive` carries llhttp's decision as an inbound user event immediately before each emitted head, including forwarded informational responses; the original header list is retained.
- The request decoder's `responseEncoder` initializer argument pairs the actual response encoder for HEAD/CONNECT suppression; response decoding retains NIO's outbound request observation.
- Public NIO decoder-limit, upgrade-strategy, informational-strategy and error types are reused; request-body classification is copied locally because NIO's equivalent is internal.
- Transfer-Encoding chunk detection uses the final coding across repeated comma-separated fields, ASCII whitespace trimming and ASCII case-insensitive comparison, including when automatic framing is disabled.
- Method serialization uses public `HTTPMethod.rawValue`; upstream generated response-line serialization and body/chunk behavior are retained.
- Source comments are omitted except attribution and the retained llhttp workaround; C identifiers use the separate target's mechanical namespace map.

`HTTP1MessageMetadata.swift` is new SwiftMITM integration code. Dedicated codec instances always preserve field bytes. They preserve permitted field octets, not original whitespace or complete wire formatting. They do not decode transfer codings other than NIO's existing chunk handling.

The cost is maintaining these codec sources and a namespaced llhttp snapshot as upstream changes. Public NIO types keep one NIO package identity in consumers. Byte mapping and preflight validation add work and temporary collections proportional to field size; upstream parsing limits remain in effect. Compilation, linkage and runtime fidelity require evidence from the project's GitHub workflows.
