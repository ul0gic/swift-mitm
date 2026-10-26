# Namespaced llhttp source

This target copies the llhttp 9.3.0 sources vendored by Apple SwiftNIO 2.104.0 at revision `6db9d9abd92ad7399b8a564cafbb87d3507245fa`. Its source of record is [SwiftNIO's CNIOLLHTTP directory](https://github.com/apple/swift-nio/tree/6db9d9abd92ad7399b8a564cafbb87d3507245fa/Sources/CNIOLLHTTP), not an independently reconstructed llhttp checkout.

| Upstream path | Local path |
|---|---|
| `c_nio_api.c` | `api.c` |
| `c_nio_http.c` | `http.c` |
| `c_nio_llhttp.c` | `llhttp.c` |
| `include/CNIOLLHTTP.h` | `include/CSwiftMITMLLHTTP.h` |
| `include/c_nio_llhttp.h` | `include/swiftmitm_llhttp.h` |

`LICENSE.txt` preserves llhttp's MIT license. `LICENSE-SwiftNIO.txt` and `NOTICE-SwiftNIO.txt` preserve SwiftNIO's Apache license and notice for its wrapper and modifications. Upstream vendoring scripts are not included or executed.

## Mechanical namespace transform

C lexical identifiers are renamed consistently in declarations, definitions and references. Comments, string literals, character literals, parse-table numbers and parsing branches retain their upstream content. The only string changes are local header include paths and the Swift wrapper's Apache-license path.

- `c_nio_*` becomes `c_swiftmitm_*`.
- `llhttp*` type/tag/helper identifiers gain the `swiftmitm_` prefix.
- `wasm_on_*` callback/helper identifiers gain the `swiftmitm_` prefix, including conditional WebAssembly declarations.
- `HTTP_*`, `HPE_*`, `F_*`, `LENIENT_*` and `LLHTTP_*` enum/macro identifiers gain the `SWIFTMITM_` prefix, except `LLHTTP_STRICT_MODE`.
- Token-pasted suffixes and stringified labels in mapping macros retain their upstream spelling; the consuming macro supplies the namespace prefix.
- Header guards `INCLUDE_LLHTTP*`, `LLLLHTTP_C_HEADERS_` and `C_NIO_LLHTTP_SWIFT` gain the `SWIFTMITM_` prefix.

The target retains upstream's `_GNU_SOURCE` and `LLHTTP_STRICT_MODE` preprocessor definitions. No unsafe compiler flags or external package override are required. C types, enum constants and header guards are namespaced as well as linkable functions so this module can coexist with official NIO's parser module.

Inverse identifier/include transformations recover all five original source/header files byte-for-byte. Source inspection found no unprefixed external-scope function declarations/definitions in the copied C files. This is source evidence; it does not prove binary symbol isolation or successful linking. Both remain subject to GitHub verification.

The additional parser copy spends binary size and upstream maintenance effort to preserve one framing authority in the byte-preserving codec without replacing consumers' official NIO package graph.
