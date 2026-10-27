# SwiftMITM 0.2.2

Unreleased candidate.

## Added

- Optional `SwiftMITMInspection`: incremental SSE, native gRPC, binary/text gRPC-Web, descriptor-backed protobuf, gzip/deflate/Brotli decoding, and fragmented WebSocket messages with supported permessage-deflate negotiation.
- Ordered HTTP rules, header edits, bounded body replacement, synthetic responses, destination mapping, breakpoints, delays, throttling, and resets.
- Explicit request replay and streaming HAR 1.2 export with credential redaction, measured timings, and separate original/effective provenance.
- Shared resource admission, configurable capture/inspection budgets, and scoped HTTP/1 connection reuse.
- Ordinary HTTP proxying alongside HTTPS CONNECT, configured prior-knowledge HTTP/2, exact-authority encrypted pass-through, and typed setup diagnostics.
- `InterceptionLab` demonstrates inspection and active debugging through public APIs; `FirstCapture` provides bounded asynchronous event delivery and explicit loss reporting.

## Changed

Requires macOS 26+ and Swift tools 6.4; builds use Xcode 27.1. The package uses Crypto 5 and official Apple NIO transport dependencies. A narrow vendored HTTP/1 codec preserves permitted header/trailer octets; whitespace and message framing may be normalized.

Existing CONNECT-only ingress remains the default. Inspection, active controls, shared admission, and connection reuse are opt-in. Host applications still own client routing/trust, authorization, CA persistence, and stored traffic.

## Migration and boundaries

The deployment minimum increases to macOS 26. Review exhaustive switches over public ingress/capture enums and the [capture migration guide](Sources/SwiftMITM.docc/CaptureEvents.md#migrate-to-the-0-2-2-candidate). Add the `SwiftMITMInspection` product only when semantic inspection is needed.

HTTP/3 and QUIC are not included. There is no h2c upgrade, ECH decryption, or certificate-pinning bypass. HTTP controls do not edit CONNECT or upgrade handshakes. See [inspection](Documentation/Inspection.md), [active debugging](Documentation/ActiveDebugging.md), and the [security model](Sources/SwiftMITM.docc/SecurityModel.md).
