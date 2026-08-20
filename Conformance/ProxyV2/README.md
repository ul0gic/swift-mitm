# PROXY v2 conformance contract

`v1.json` is the language-neutral, versioned byte-level interoperability contract between SwiftMITM's trusted PROXY v2 receiver and external forwarders. Consume the file from the exact SwiftMITM release tag rather than copying individual vectors.

## Pin the release artifact

The current artifact is `Conformance/ProxyV2/v1.json` with SHA-256:

`d9953424cbb63011c25820feca56ca60feb824b1db44dc4f82ef1940588fbf43`

Guest-agent and host-helper repositories must pin the release tag and verify this digest before using the vectors. A new contract version receives a new versioned file; consumers must not silently substitute a branch tip or a different artifact.

## Schema roles

The JSON document identifies itself with `format`, `schemaVersion`, and `contractVersion`, and fixes `maximumHeaderBytes` for the corpus. Every vector has a stable `id`, a `roles` list, and a `disposition`:

- `emitter` vectors define the exact binary header an external forwarder must produce, including source, destination, optional TLVs, and coalesced application bytes.
- `receiver` vectors define whether SwiftMITM must accept or reject the supplied `headerHex`.
- Accepted vectors include decoded source and destination endpoints, TLV bytes and count, and the application bytes that must remain available after the header.
- Rejected vectors include the required stable reason and intentionally omit decoded metadata.

`headerHex`, `tlvHex`, and `applicationHex` are lowercase hexadecimal byte strings. Endpoint objects state IPv4 or IPv6 family, literal address, and port. The vectors cover supported TCP/IPv4 and TCP/IPv6 headers plus malformed, unsupported, oversize, and invalid-port cases.

## Evidence and ownership

SwiftMITM's package tests prove the receiver against this artifact and prove a rootless local accepting forwarder against the public transparent listener. That is package evidence only.

Each guest-agent and host-helper repository owns its encoder, its CI, and its evidence that every applicable `emitter` vector is emitted byte-for-byte. The guest repository also owns Linux image construction, capabilities, redirect rules, and loop prevention. The host-product repository owns macOS forwarding and helper deployment.

## macOS PF boundary

SwiftMITM V2 rejects a direct `/dev/pf` provider: the public macOS SDK does not expose a stable lookup API for `DIOCNATLOOK` or its private ABI. The package will not vendor that ABI or take PF privilege ownership.

A privileged host helper owns PF rule installation and removal, authorization, lookup and redirect serialization, tuple and stale-result validation, cancellation, and cleanup. It forwards only normalized trusted PROXY v2 metadata to SwiftMITM. Any real PF smoke test belongs to an isolated trusted host-product environment; it is not a SwiftMITM package or pull-request CI responsibility.
