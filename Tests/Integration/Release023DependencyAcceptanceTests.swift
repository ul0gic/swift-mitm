import XCTest

import SwiftMITMInspection

final class Release023DependencyAcceptanceTests: XCTestCase {
    func testDescriptorLoadingDecodesStringFieldAndEnforcesConfiguredDepth() async throws {
        // Descriptor wire tags: apple/swift-protobuf, Sources/SwiftProtobuf/descriptor.pb.swift:2822.
        var descriptor: [UInt8] = [0x0A, 0x3B, 0x0A, 0x0E] + Array("greeting.proto".utf8)
        descriptor += [0x12, 0x07] + Array("example".utf8)
        descriptor += [0x22, 0x18, 0x0A, 0x08] + Array("Greeting".utf8)
        descriptor += [0x12, 0x0C, 0x0A, 0x04] + Array("text".utf8)
        descriptor += [0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x62, 0x06] + Array("proto3".utf8)
        let budget = try InspectionMemoryBudget()
        let registry = ProtobufSchemaRegistry(budget: budget, limits: try ProtobufSchemaLimits(depth: 1))
        let snapshot = try await registry.load(descriptorBytes: descriptor)

        XCTAssertEqual(snapshot.messageNames, ["example.Greeting"])
        let fields = try snapshot.decode([0x0A, 0x02, 0x6F, 0x6B], messageName: "example.Greeting")
        XCTAssertEqual(fields.count, 1)
        let field = try XCTUnwrap(fields.first)
        XCTAssertEqual(field.number, 1)
        XCTAssertEqual(field.name, "text")
        guard case let .string(value) = field.value else {
            XCTFail("Field 1 must decode as a string")
            return
        }
        XCTAssertEqual(value, "ok")

        var tooDeepDescriptor: [UInt8] = [0x0A, 0x3B, 0x0A, 0x0E] + Array("greeting.proto".utf8)
        tooDeepDescriptor += [0x12, 0x07] + Array("example".utf8)
        tooDeepDescriptor += [0x22, 0x18, 0x0A, 0x05] + Array("Outer".utf8)
        tooDeepDescriptor += [0x1A, 0x0F, 0x0A, 0x05] + Array("Inner".utf8)
        tooDeepDescriptor += [0x1A, 0x06, 0x0A, 0x04] + Array("Deep".utf8)
        tooDeepDescriptor += [0x62, 0x06] + Array("proto3".utf8)
        do {
            _ = try await registry.load(descriptorBytes: tooDeepDescriptor)
            XCTFail("A message nested two levels must exceed the configured depth of one")
        } catch ProtobufInspectionError.limitExceeded {
            let current = await registry.current
            XCTAssertTrue(current === snapshot)
        }
    }
}
