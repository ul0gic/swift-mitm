import Foundation
import NIOCore
import NIOHPACK
import XCTest

import SwiftMITM
import SwiftMITMInspection

struct Stage2SchemaProbe {
    let budget: InspectionMemoryBudget
    let registry: ProtobufSchemaRegistry
    let original: ProtobufSchemaSnapshot
    let renamedDescriptor: [UInt8]
    let maximumDescriptorBytes: Int

    init() async throws {
        let originalPath = try XCTUnwrap(ProcessInfo.processInfo.environment["STAGE2_DESCRIPTOR_PATH"])
        let renamedPath = try XCTUnwrap(ProcessInfo.processInfo.environment["STAGE2_RENAMED_DESCRIPTOR_PATH"])
        let originalDescriptor = [UInt8](try Data(contentsOf: URL(fileURLWithPath: originalPath)))
        let renamedDescriptor = [UInt8](try Data(contentsOf: URL(fileURLWithPath: renamedPath)))
        let maximumDescriptorBytes = max(originalDescriptor.count, renamedDescriptor.count)
        let budget = try InspectionMemoryBudget()
        let registry = ProtobufSchemaRegistry(budget: budget, limits: try ProtobufSchemaLimits(
            descriptorBytes: maximumDescriptorBytes,
            files: 1,
            fields: 1
        ))
        let original = try await registry.load(descriptorBytes: originalDescriptor)
        self.renamedDescriptor = renamedDescriptor
        self.maximumDescriptorBytes = maximumDescriptorBytes
        self.budget = budget
        self.registry = registry
        self.original = original
    }

    func exercise(
        upstream: Release022H2Stream,
        downstream: Release022H2Stream,
        events: Stage2InspectionEvents,
        raw: Release022Events
    ) async throws {
        try upstream.wait { !$0.isEmpty }
        try upstream.write(.headers(.init(headers: HPACKHeaders([
            (":status", "200"),
            ("content-type", "application/grpc"),
            ("grpc-encoding", "gzip")
        ]))))
        let envelope: [UInt8] = [0, 0, 0, 0, 5, 8, 42, 0x98, 6, 7]
        for slice in [Array(envelope.prefix(2)), Array(envelope[2..<7]), Array(envelope.dropFirst(7))] {
            try upstream.write(.data(.init(data: .byteBuffer(ByteBuffer(bytes: slice)))))
        }
        let first = try await events.wait { events in
            events.contains {
                if case .protobuf = $0.payload {
                    true
                } else {
                    false
                }
            }
        }
        try assertInitialEvents(first)
        XCTAssertTrue(release022ResponseEnds(raw.events).isEmpty)
        try await replaceRegistry()
        try upstream.write(.data(.init(data: .byteBuffer(ByteBuffer(bytes: envelope)))))
        let vectorPath = try XCTUnwrap(ProcessInfo.processInfo.environment["STAGE2_COMPRESSION_VECTORS"])
        let compressedFrames =
            [UInt8](try Data(contentsOf: URL(fileURLWithPath: vectorPath).appendingPathComponent("grpc-gzip.bin")))
        try upstream.write(.data(.init(data: .byteBuffer(ByteBuffer(bytes: compressedFrames)))))
        try upstream.write(.headers(.init(headers: HPACKHeaders([("grpc-status", "0")]), endStream: true)))
        try downstream.wait(until: stage2H2Ended)
        let completed = try await events.wait { events in
            events.contains {
                if case let .grpc(.status(status)) = $0.payload {
                    status.code == 0
                } else {
                    false
                }
            }
        }
        assertCompletedEvents(completed)
        XCTAssertEqual(stage2H2Bytes(downstream.frames), envelope + envelope + compressedFrames)
        XCTAssertTrue(release022ResponseBodyBytes(raw.events).isEmpty)
    }

    private struct DecodedFields {
        let name: String
        let digest: String
        let fields: [ProtobufFieldOccurrence]
    }

    private func assertInitialEvents(_ first: [InspectionEvent]) throws {
        let decoded = try XCTUnwrap(first.compactMap { event -> DecodedFields? in
            if case let .protobuf(name, digest, fields) = event.payload {
                DecodedFields(name: name, digest: digest, fields: fields)
            } else {
                nil
            }
        }
        .first)
        XCTAssertEqual(decoded.name, "stage2.Envelope")
        XCTAssertEqual(decoded.digest, original.digest)
        XCTAssertEqual(decoded.fields.map(\.number), [1, 99])
        XCTAssertEqual(decoded.fields.first?.name, "value")
        if let field = decoded.fields.first, case let .signed(value) = field.value {
            XCTAssertEqual(value, 42)
        } else {
            XCTFail("Schema scalar value was not interpreted as int32")
        }
        if let field = decoded.fields.last, case let .unknown(wireType, bytes) = field.value {
            XCTAssertEqual(wireType, 0)
            XCTAssertEqual(bytes, [0x98, 6, 7])
        } else {
            XCTFail("Unknown wire field was lost")
        }
    }

    private func replaceRegistry() async throws {
        do {
            _ = try await registry.load(descriptorBytes: [0x0A, 0xFF])
            XCTFail("Malformed descriptor replaced active schema")
        } catch {
            let selected = await registry.current
            XCTAssertEqual(selected?.digest, original.digest)
        }
        do {
            _ = try await registry.load(descriptorBytes: [UInt8](repeating: 0, count: maximumDescriptorBytes + 1))
            XCTFail("Descriptor limit plus one was accepted")
        } catch ProtobufInspectionError.limitExceeded {
            let selected = await registry.current
            XCTAssertEqual(selected?.digest, original.digest)
        } catch { throw error }
        let replacement = try await registry.load(descriptorBytes: renamedDescriptor)
        XCTAssertNotEqual(replacement.digest, original.digest)
        XCTAssertEqual(try replacement.decode([8, 42], messageName: "stage2.Envelope").first?.name, "changed")
    }

    private func assertCompletedEvents(_ completed: [InspectionEvent]) {
        let names = completed.compactMap { event -> String? in
            if case let .protobuf(_, _, fields) = event.payload {
                fields.first?.name
            } else {
                nil
            }
        }
        XCTAssertEqual(names, ["value", "value", "value", "value"])
        let messages = completed.compactMap { event -> GRPCMessage? in
            if case let .grpc(.message(message)) = event.payload, event.direction == .response {
                message
            } else {
                nil
            }
        }
        XCTAssertEqual(messages.map(\.compressed), [false, false, true, true])
        XCTAssertEqual(messages.suffix(2).map(\.bytes), [[8, 42], [8, 42]])
    }
}
