# Native dependency patch candidates

These source patches are experimental. Candidate `18a954b` compiled remotely and passed nine of thirteen transport scenarios. Follow-on SNI, GOAWAY, storage-lifetime and runtime-ordering repairs await the next remote run. They do not establish a production memory bound. Apply each patch with strip level 1 to the exact upstream revision below; publish the resulting reviewed trees as immutable dependency revisions. Preserve each repository's license, notices, contributors file and existing source headers.

| Patch | Upstream repository | Revision |
|---|---|---|
| `swift-network-evolution-split-fin.patch` | `apple/swift-network-evolution` | `ea74abb2beced205f5eb0379e4fa8cd66de2e66e` |
| `swift-nio-quic-bounded-read.patch` | `apple/swift-nio-quic` | `11b231126952b3ef408c4e1ab95d14e16af5d867` |
| `swift-nio-http3-bounded-input.patch` | `apple/swift-nio-http3` | `e2c54cdbf50dc64bf9e80861922f8d9400d2acce` |
| `swift-nio-http3-typed-decode-error.patch` | `ul0gic/swift-nio-http3` | `63a3ba36b9c05aa9bf2851c7d9adb39c9988dde0` plus reviewed control-budget delta |

The three baseline patches affect separate repositories. Apply the QUIC receive patch before the separately owned outbound patch. No patch replaces protocol parsing, cryptography or transport state machines. Upstream source is Apache-2.0; these diff artifacts preserve its attribution through their unchanged source files and upstream provenance.

Apply `swift-nio-http3-typed-decode-error.patch` after the full HTTP3 input patch. It addresses the actual compiler diagnostic from GitHub run 40422090426 by declaring the existing loop's `do throws(QPACKDecoderError)` boundary explicitly. It does not cast errors or change error mapping. The full input artifact remains frozen for reproducibility; this incremental repair requires a subsequent remote build.

Apply `swift-nio-http3-rounded-read-capacity.patch` after the reviewed full input/control-budget patches and typed-error repair. Apply `swift-nio-quic-rounded-read-reservation.patch` after `swift-nio-quic-stream-admission.patch` (published admission base `c289735403ecc93b5f04e599f64d96109c6795e5`). Together these address BUG-027: logical read limits remain unchanged, but guards and raw-storage reservations use NIO's power-of-two UInt32-backed capacity, saturated at UInt32.max. UInt64 rounding cannot overflow this bounded range; existing checked reservation addition/multiplication remains intact. A 10,000-byte logical read therefore reserves/allows 16,384 bytes of backing while cumulative payload checks still enforce 10,000. Source-reviewed application and remote non-power-of-two coverage are required before closing the issue.

## Receive and completion ownership

`QUICMaximumBytesPerReadOption` defaults to `Int.max`; set a positive limit before issuing reads. `QUICConfiguration.maximumBytesPerRead` propagates the limit before initial, inbound and outbound child streams start. The same propagation carries the separately implemented `maximumPendingWriteBytes` option before initial HTTP3 stream-type writes. Both QUIC patches are required together. The adapter passes the receive limit to the existing lower receive API. Network's `AutomaticUpperStreamProcessing.receiveStreamData` delegates to `FrameArray.drainArray(maximumByteCount:)`, which splits the final frame when necessary. The lower patch preserves completion on the retained suffix in the branch where the returned prefix uses the original allocation. Both split branches, equal halves and separate zero-byte FIN need remote proof.

The adapter moves its input buffer out before calling the pipeline. It no longer retains and clears the same buffer after a reentrant callback. This change alone does not provide application ownership accounting or outbound backpressure.

The lower receive allowance subtracts bytes already buffered by the initial optimistic read. A subsequent demand cannot concatenate two full-sized pulls into an oversized delivery; FIN remains available for the next read when the current buffer fills the allowance.

## Parser reservation contract

Client and server configuration optionally accept `HTTP3InboundMemoryConfiguration`. Its reservation factory receives the backend's computed minimum payload-storage model and runs before the framed stream handler is constructed or installed. A throwing factory rejects installation. The handler retains the returned `HTTP3InboundStorageReservation` object for its lifetime, covering parser allocation before any input arrives and through parser destruction. Callers must retain separate reservations for any emitted storage that outlives the handler. Earliest QUIC child admission before unidirectional type classification remains a separate integration requirement.

The configuration separates encoded field-section length from raw read size. Declared oversized HEADERS/PUSH_PROMISE payloads fail with the existing connection-level `H3_EXCESSIVE_LOAD` path before payload accumulation. Raw batch capacity and cumulative bytes are checked before buffering. In this opt-in mode, consumed parser bytes are compacted after decoding and read completion reaches the application even when only part of a frame arrived. The caller still controls demand.

The proposed companion settings are 32,768 encoded bytes and 16,384 raw bytes. The byte-buffer envelope under review is 147,456 bytes per framed stream: at most 65,536 bytes for the accumulated parser allocation, another 65,536 during copy-on-write, and a 16,384-byte active adapter read. This uses NIO's power-of-two allocation rounding and includes up to 16 framing bytes. SETTINGS payloads retain their independent 1,024-byte allowance when the encoded header limit is smaller. A peer cannot be credited with this proof merely because its initial flow-control window is small.

The configured decoded field-section size is enforced in both existing QPACK stages. Before each field-line decode, 32 bytes of RFC overhead are charged; literal and Huffman output is bounded before growth. Resolved static/dynamic names and values are checked cumulatively before appending each `HTTPField`. With a 32,768-byte limit, at most 1,024 fields can be built. If SETTINGS omits the decoded limit, the opt-in local encoded limit supplies the same effective decoded limit to parsing and reservation.

The modeled metadata addition is `N * (stride(FieldLine) + 2 * stride(HTTPField)) + 6 * D`, computed inside the backend, where `D` is the decoded limit and `N = D / 32`. Each array reserves its bounded count before append: field lines, resolved fields, and the HTTPTypes parsed-head copy. The six string byte allowances cover completed literals, current Huffman storage, up to three times the raw bytes during UTF-8 repair, and resolved/canonical field copies. Shared strings are conservatively counted twice. This is a payload model, not exact heap usage: allocator padding, object headers and implementation-specific spare String/Array capacity still require measured RSS evidence. Synchronous QPACK callbacks cannot recursively decode more sections while the previous section is still retained.

Both QPACK instruction handlers also acquire a reservation before construction. Their processors allow a bounded read plus a partial integer, preserving valid coalesced instructions. In the opt-in static profile, illegal insert/duplicate instructions fail in the existing native instruction decoder before literal allocation; legal zero-capacity instructions remain supported. Native output storage and earliest child admission are separately accounted integration work.

Control and QPACK instruction streams use smaller envelopes. Control streams allow the existing 1,024-byte SETTINGS payload and no decoded header metadata; HEADERS are invalid on those streams. Instruction streams allow the bounded raw batch plus a partial integer. Both need an 81,920-byte buffer envelope with the proposed read size and no field-array/string model. This avoids charging every unidirectional control stream for a request-header parser.

The companion must compact-copy retained DATA into its own precharged storage and keep that reservation until the actual downstream consumer releases it. The native envelope and application copy overlap are charged concurrently. The outbound patch must establish that release boundary; the unpatched adapter completes write promises before native buffered output is consumed.

## Required remote evidence

- Build the patched graph with the pinned Swift/Xcode toolchain.
- Exercise incomplete frame prefixes and fragmented headers under manual reads.
- Reject oversized declared headers before receiving their payload and recover on a new connection.
- Exercise partial lower reads with FIN in both split branches, plus separate FIN and cancellation.
- Stall downstream transport credit while observing reservations, native output, sibling progress and cleanup.
- Prove reservation failure rejects parser installation and all parser/output reservations return to zero after teardown.

No local verification was executed. Patch application, compilation, upstream contribution submission and publication remain the lead's responsibility.

## Pinned experimental revisions

[revisions.json](revisions.json) records exact upstream/fork revisions and the ordered patch sequence for each repository. These published fork commits are source-reviewed candidates, not completed runtime acceptance. The QUIC fork manifest pins the patched network revision; the HTTP3 fork manifest pins the patched QUIC revision. Those manifest rewrites are intentionally separate from the protocol source patches prepared for upstream review. No upstream pull request has been submitted.

Admission now precedes all real native child starts and retains reservations through raw-buffer disposal and native output release. The256 limit counts submitted storage tickets, not every internally split native Frame. Record charges are a stride-derived logical model; allocator/spare-capacity/native callback workspace still requires independent ceiling measurement. The rounded-capacity patches preserve logical read limits while reserving/checking actual NIO backing capacity.

The terminal-input-close patch preserves the finished state when native read abort synchronously delivers channel inactivity before a late input-closed event. Open-stream FIN handling remains unchanged. The prior runtime reached this assertion in streaming and slow-reader scenarios; the repair still requires those scenarios to pass.

## Follow-on native seams

The SwiftTLS policy validates parsed client SNI before handshake progress; a nil policy preserves upstream behavior. SwiftNetwork and NIOQUIC propagate the same pinned implementation. The companion accepts absent SNI and canonical fixed DNS identity, and keeps certificate verification enabled.

Accepted GOAWAY notifications follow the native state transition and precede cancellation effects. Reentrant observers cannot cause duplicate shutdown or cancellation of an already-removed stream. Static QPACK accepts legal stream-cancellation instructions without weakening invalid acknowledgement/increment handling. A secondary connection error after the terminal transition is ignored; the first error still closes the connection.

The connection storage barrier closes admission before terminal callbacks, detaches native ingress and disposes pending children/coalescers before sealing, then waits for every registered child's read/write storage release. It is distinct from channel close completion. Final native reads must deliver the returned DATA/FIN before deferred disconnect cleanup. Failed parser installation retains a close callback so native stream accounting can unwind even before the application handler exists.

These seams add pinned SwiftTLS maintenance and small lifecycle state. Their purpose is fixed-route identity enforcement, protocol-correct cancellation, and truthful disposal accounting. They do not establish the production proxy's stop deadline, maximum-state memory bound, or total RSS. Remote checks must prove those properties separately.
