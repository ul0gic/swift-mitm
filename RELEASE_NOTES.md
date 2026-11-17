# SwiftMITM 0.2.3

## Changed

- Update resolved SwiftNIO to 2.105.0, SwiftNIO SSL to 2.37.6, and Swift Crypto to 5.1.1.
- Update the exact SwiftProtobuf dependency requirement from 1.32.0 to 1.40.0 for `SwiftMITMInspection`.
- Refresh integration guidance, documentation navigation, and the local interception lab.

macOS 26+ and Swift tools 6.4 remain required. The SwiftNIO, SwiftNIO SSL, and Swift Crypto manifest minimums remain unchanged; their resolved versions describe this release's verified dependency graph.

## Fixed

- Report version 0.2.3 in exported HAR creator metadata.
