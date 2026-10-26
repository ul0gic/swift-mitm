import NIOConcurrencyHelpers
import NIOCore
import NIOEmbedded
import XCTest

@testable import SwiftMITM

extension TransparentApplicationClassifierTests {
    func makeChannel(
        configuration: TrustedProxyV2Ingress? = TrustedProxyV2Ingress(trustedPeers: .loopback),
        storage: DecisionStorage,
        errors: ClassifierErrorStorage? = nil,
        readMode: TransparentApplicationReadMode = .automatic,
        readCounter: ClassifierReadCounter? = nil
    ) throws -> EmbeddedChannel {
        try makeChannelWithClassifier(
            configuration: configuration,
            storage: storage,
            errors: errors,
            readMode: readMode,
            readCounter: readCounter
        ).channel
    }

    func makeChannelWithClassifier(
        configuration: TrustedProxyV2Ingress? = TrustedProxyV2Ingress(trustedPeers: .loopback),
        storage: DecisionStorage,
        errors: ClassifierErrorStorage? = nil,
        readMode: TransparentApplicationReadMode = .automatic,
        readCounter: ClassifierReadCounter? = nil
    ) throws -> (channel: EmbeddedChannel, classifier: TransparentApplicationClassifier) {
        let configuration = try XCTUnwrap(configuration)
        let channel = EmbeddedChannel()
        if let errors {
            try channel.pipeline.syncOperations.addHandler(errors)
        }
        if let readCounter {
            try channel.pipeline.syncOperations.addHandler(readCounter)
        }
        let classifier = TransparentApplicationClassifier(configuration: configuration) { context, classification in
            storage.classification = classification
            storage.decisionCount += 1
            return context.eventLoop.makeSucceededFuture(readMode)
        }
        try channel.pipeline.syncOperations.addHandler(classifier, position: .first)
        return (channel, classifier)
    }

    func makeTLSClientHello(
        protocols: [[UInt8]]?,
        serverName: String? = nil,
        ech: Bool = false
    ) -> [UInt8] {
        var body: [UInt8] = [3, 3]
        body.append(contentsOf: repeatElement(0, count: 32))
        body.append(0)
        body.append(contentsOf: [0, 2, 0x13, 0x01])
        body.append(contentsOf: [1, 0])

        var extensions: [UInt8] = []
        if let serverName {
            let name = Array(serverName.utf8)
            let entry = [UInt8(0)] + encodedUInt16(name.count) + name
            let payload = encodedUInt16(entry.count) + entry
            extensions.append(contentsOf: makeExtension(type: 0, payload: payload))
        }
        if ech {
            extensions.append(contentsOf: makeExtension(type: 0xFE0D, payload: [1]))
        }
        if let protocols {
            let identifiers = protocols.flatMap { [UInt8($0.count)] + $0 }
            let payload = encodedUInt16(identifiers.count) + identifiers
            extensions.append(contentsOf: makeExtension(type: 16, payload: payload))
        }
        body.append(contentsOf: encodedUInt16(extensions.count))
        body.append(contentsOf: extensions)

        let handshake = [UInt8(1)] + encodedUInt24(body.count) + body
        return [22, 3, 3] + encodedUInt16(handshake.count) + handshake
    }

    func assertEmpty(
        _ state: EmbeddedChannel.BufferState,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        guard case .empty = state else {
            return XCTFail("expected empty embedded buffer", file: file, line: line)
        }
    }

    func assertFull(
        _ state: EmbeddedChannel.BufferState,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        guard case .full = state else {
            return XCTFail("expected readable embedded buffer", file: file, line: line)
        }
    }

    func readAllInboundBytes(_ channel: EmbeddedChannel) throws -> [UInt8] {
        var bytes: [UInt8] = []
        while let buffer = try channel.readInbound(as: ByteBuffer.self) {
            bytes.append(contentsOf: buffer.readableBytesView)
        }
        return bytes
    }

    private func makeExtension(type: Int, payload: [UInt8]) -> [UInt8] {
        encodedUInt16(type) + encodedUInt16(payload.count) + payload
    }

    private func encodedUInt16(_ value: Int) -> [UInt8] {
        [UInt8((value >> 8) & 0xFF), UInt8(value & 0xFF)]
    }

    private func encodedUInt24(_ value: Int) -> [UInt8] {
        [UInt8((value >> 16) & 0xFF), UInt8((value >> 8) & 0xFF), UInt8(value & 0xFF)]
    }
}

final class DecisionStorage: Sendable {
    private let classificationStorage = NIOLockedValueBox<TransparentApplicationClassification?>(nil)
    private let decisionCountStorage = NIOLockedValueBox(0)

    var classification: TransparentApplicationClassification? {
        get { classificationStorage.withLockedValue { $0 } }
        set { classificationStorage.withLockedValue { $0 = newValue } }
    }

    var decisionCount: Int {
        get { decisionCountStorage.withLockedValue { $0 } }
        set { decisionCountStorage.withLockedValue { $0 = newValue } }
    }
}

final class ClassifierErrorStorage: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    private(set) var classificationErrors: [TransparentApplicationClassificationError] = []

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        if let error = error as? TransparentApplicationClassificationError {
            classificationErrors.append(error)
        }
    }
}

final class ClassifierReadCounter: ChannelOutboundHandler {
    typealias OutboundIn = ByteBuffer

    private(set) var count = 0

    func read(context: ChannelHandlerContext) {
        count += 1
        context.read()
    }
}
