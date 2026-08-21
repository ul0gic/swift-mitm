# SwiftMITM 0.2.1

SwiftMITM 0.2.1 fixes explicit HTTP `CONNECT` interoperability with Apple TLS clients.

## Fixed

- Successful CONNECT responses no longer emit a chunked-transfer terminator before downstream TLS begins.
- WebKit and SecureTransport clients can complete the downstream TLS handshake through explicit proxy mode.
- Wire-level framing and public-proxy SecureTransport regressions are covered by local tests.

## Requirements

- macOS 14 or newer
- Swift 6.0 or newer

This patch does not change the public API or the 0.2.0 migration requirements.

See the [README](https://github.com/ul0gic/swift-mitm/blob/0.2.1/README.md) for installation and the [DocC catalog](https://github.com/ul0gic/swift-mitm/blob/0.2.1/Sources/SwiftMITM/SwiftMITM.docc/SwiftMITM.md) for the complete consumer contract.
