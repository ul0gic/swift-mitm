import Crypto
import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOHTTP1
import NIOPosix

import SwiftMITM

enum Phase6ResourceStream {
    static let chunkSize = 16 * 1024
    static let captureLimit = 64 * 1024
    static let maximumRSSGrowth = UInt64(64 * 1024 * 1024)
    static let timeout = TimeAmount.seconds(120)

    static func bytes(seed: UInt8, offset: Int, count: Int) -> [UInt8] {
        (offset ..< offset + count).map { index in
            UInt8(truncatingIfNeeded: index &* 31 &+ Int(seed))
        }
    }

    static func digest(seed: UInt8, count: Int) -> [UInt8] {
        var hasher = SHA256()
        var offset = 0
        while offset < count {
            let size = min(chunkSize, count - offset)
            let chunk = bytes(seed: seed, offset: offset, count: size)
            chunk.withUnsafeBytes { hasher.update(bufferPointer: $0) }
            offset += size
        }
        return Array(hasher.finalize())
    }

    static func digest(repeating byte: UInt8, count: Int) -> [UInt8] {
        var hasher = SHA256()
        let chunk = [UInt8](repeating: byte, count: chunkSize)
        var remaining = count
        while remaining > 0 {
            let size = min(chunk.count, remaining)
            chunk.withUnsafeBytes { bytes in
                hasher.update(bufferPointer: UnsafeRawBufferPointer(rebasing: bytes.prefix(size)))
            }
            remaining -= size
        }
        return Array(hasher.finalize())
    }
}

struct Phase6DigestResult: Equatable, Sendable {
    let byteCount: Int
    let digest: [UInt8]
}

final class Phase6DigestAccumulator {
    private let expectedByteCount: Int
    private var byteCount = 0
    private var hasher = SHA256()
    private var finished = false

    init(expectedByteCount: Int) {
        self.expectedByteCount = expectedByteCount
    }

    var observedByteCount: Int { byteCount }

    func append(_ buffer: ByteBuffer) throws -> Phase6DigestResult? {
        guard !finished else { return nil }
        byteCount += buffer.readableBytes
        guard byteCount <= expectedByteCount else { throw Phase2FixtureError.exceededByteLimit }
        buffer.withUnsafeReadableBytes { hasher.update(bufferPointer: $0) }
        guard byteCount == expectedByteCount else { return nil }
        finished = true
        return .init(byteCount: byteCount, digest: Array(hasher.finalize()))
    }

    func finishIfComplete() throws -> Phase6DigestResult {
        guard !finished, byteCount == expectedByteCount else {
            throw Phase2FixtureError.closedBeforeExpectedBytes
        }
        finished = true
        return .init(byteCount: byteCount, digest: Array(hasher.finalize()))
    }
}

final class Phase6ResourceSink: CaptureEventSink, @unchecked Sendable {
    struct DirectionEnd: Sendable {
        let direction: OpaqueFlowDirection
        let byteCount: Int
        let truncated: Bool
    }

    struct Snapshot: Sendable {
        var requestHeads: [CapturedRequestHead] = []
        var responseHeads = 0
        var responseRetainedBytes: [UInt8] = []
        var responseObservedBytes = 0
        var responseEnds: [(requestID: UUID, truncated: Bool)] = []
        var streamErrors = 0
        var opaqueFlow: CapturedOpaqueFlow?
        var clientRetainedBytes: [UInt8] = []
        var serverRetainedBytes: [UInt8] = []
        var clientObservedBytes = 0
        var serverObservedBytes = 0
        var directionEnds: [DirectionEnd] = []
        var opaqueCloseReasons: [OpaqueFlowCloseReason] = []
        var opaqueErrors: [CapturedConnectionFailureReason] = []
    }

    private let condition = NSCondition()
    private var storage = Snapshot()

    var snapshot: Snapshot {
        condition.lock()
        defer { condition.unlock() }
        return storage
    }

    func receive(_ event: CaptureEvent) {
        condition.lock()
        recordHTTP(event)
        recordOpaque(event)
        condition.broadcast()
        condition.unlock()
    }

    func waitForOpaqueOpen(timeout: TimeInterval = 120) throws -> Snapshot {
        try wait(timeout: timeout) { $0.opaqueFlow != nil }
    }

    func waitForOpaqueTerminal(timeout: TimeInterval = 120) throws -> Snapshot {
        try wait(timeout: timeout) {
            $0.directionEnds.count == 2 && $0.opaqueCloseReasons.count == 1
        }
    }

    func waitForHTTPCompletion(timeout: TimeInterval = 120) throws -> Snapshot {
        try wait(timeout: timeout) { $0.responseEnds.count == 1 }
    }

    func waitForHTTPProgress(minimumObservedBytes: Int, timeout: TimeInterval = 120) throws -> Snapshot {
        try wait(timeout: timeout) { $0.responseObservedBytes >= minimumObservedBytes }
    }

    private func wait(
        timeout: TimeInterval,
        until predicate: (Snapshot) -> Bool
    ) throws -> Snapshot {
        let deadline = Date().addingTimeInterval(timeout)
        condition.lock()
        defer { condition.unlock() }
        while !predicate(storage) {
            guard condition.wait(until: deadline) else { throw Phase2FixtureError.deadlineExceeded }
        }
        return storage
    }

    private func recordHTTP(_ event: CaptureEvent) {
        switch event {
        case .requestHead(let head):
            storage.requestHeads.append(head)
        case .responseHead:
            storage.responseHeads += 1
        case let .responseBodyChunk(_, bytes, byteCount):
            storage.responseRetainedBytes += bytes
            storage.responseObservedBytes += byteCount
        case let .responseEnd(requestID, truncated):
            storage.responseEnds.append((requestID, truncated))
        case .streamError:
            storage.streamErrors += 1
        default:
            break
        }
    }

    private func recordOpaque(_ event: CaptureEvent) {
        switch event {
        case .opaqueOpen(let flow):
            storage.opaqueFlow = flow
        case let .opaqueData(_, _, direction, bytes, byteCount):
            recordOpaqueData(direction: direction, bytes: bytes, byteCount: byteCount)
        case let .opaqueDirectionEnd(_, _, direction, byteCount, truncated):
            storage.directionEnds.append(.init(
                direction: direction,
                byteCount: byteCount,
                truncated: truncated
            ))
        case let .opaqueClose(_, _, reason):
            storage.opaqueCloseReasons.append(reason)
        case let .opaqueError(_, _, reason):
            storage.opaqueErrors.append(reason)
        default:
            break
        }
    }

    private func recordOpaqueData(
        direction: OpaqueFlowDirection,
        bytes: [UInt8],
        byteCount: Int
    ) {
        switch direction {
        case .clientToServer:
            storage.clientRetainedBytes += bytes
            storage.clientObservedBytes += byteCount
        case .serverToClient:
            storage.serverRetainedBytes += bytes
            storage.serverObservedBytes += byteCount
        }
    }
}

final class Phase6ResourcePermit: @unchecked Sendable {
    private static let semaphore = DispatchSemaphore(value: 1)

    static func acquire() async -> Phase6ResourcePermit {
        await withCheckedContinuation { continuation in
            DispatchQueue.global().async {
                semaphore.wait()
                continuation.resume(returning: Phase6ResourcePermit())
            }
        }
    }

    deinit {
        Self.semaphore.signal()
    }
}

final class Phase6TransparentStalledFetch: @unchecked Sendable {
    let result: EventLoopFuture<Phase6DigestResult>

    private let connection: Channel
    private let receivedState = NIOLockedValueBox(0)

    init(connection: Channel, result: EventLoopFuture<Phase6DigestResult>) {
        self.connection = connection
        self.result = result
    }

    var receivedBytes: Int { receivedState.withLockedValue { $0 } }

    func addReceivedBytes(_ count: Int) {
        receivedState.withLockedValue { $0 += count }
    }

    func resume() async throws {
        try await connection.setOption(ChannelOptions.autoRead, value: true).map { self.connection.read() }.get()
    }

    func shutdown() async {
        if connection.isActive {
            try? await connection.close().get()
        }
    }
}

extension ProxyTestClient {
    func beginPhase6TransparentStalledFetch(
        adapterPort: Int,
        originHost: String,
        originPort: Int,
        mitmCACertificatePEM: String,
        bodySize: Int
    ) throws -> Phase6TransparentStalledFetch {
        let completion = Phase2FixtureCompletion<Phase6DigestResult>(
            eventLoop: group.next(),
            timeout: Phase6ResourceStream.timeout
        )
        let channel = try openTunnel(proxyPort: adapterPort, originHost: originHost, originPort: originPort)
        _ = try startTLS(
            on: channel,
            serverHostname: originHost,
            mitmCACertificatePEM: mitmCACertificatePEM,
            alpn: "http/1.1"
        )
        try channel.setOption(ChannelOptions.autoRead, value: false).wait()
        let fetch = Phase6TransparentStalledFetch(connection: channel, result: completion.futureResult)
        let authority = "\(originHost):\(originPort)"
        let installed = channel.eventLoop.submit {
            try channel.pipeline.syncOperations.addHandlers([
                HTTPRequestEncoder(),
                ByteToMessageHandler(HTTPResponseDecoder()),
                Phase6StalledHTTPClientHandler(
                    authority: authority,
                    bodySize: bodySize,
                    fetch: fetch,
                    completion: completion
                )
            ])
        }
        try installed.wait()
        return fetch
    }
}

private final class Phase6StalledHTTPClientHandler: ChannelInboundHandler {
    typealias InboundIn = HTTPClientResponsePart
    typealias OutboundOut = HTTPClientRequestPart

    private let authority: String
    private let fetch: Phase6TransparentStalledFetch
    private let completion: Phase2FixtureCompletion<Phase6DigestResult>
    private let accumulator: Phase6DigestAccumulator
    private var result: Phase6DigestResult?
    private var sent = false

    init(
        authority: String,
        bodySize: Int,
        fetch: Phase6TransparentStalledFetch,
        completion: Phase2FixtureCompletion<Phase6DigestResult>
    ) {
        accumulator = Phase6DigestAccumulator(expectedByteCount: bodySize)
        self.authority = authority
        self.fetch = fetch
        self.completion = completion
    }

    func handlerAdded(context: ChannelHandlerContext) {
        sendRequest(context: context)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        switch unwrapInboundIn(data) {
        case .head(let head) where head.status != .ok:
            completion.complete(.failure(Phase2FixtureError.unexpectedBytes))
            context.close(promise: nil)
        case .head:
            break
        case .body(let buffer):
            do {
                result = try accumulator.append(buffer) ?? result
                fetch.addReceivedBytes(buffer.readableBytes)
            } catch {
                completion.complete(.failure(error))
                context.close(promise: nil)
            }
        case .end:
            guard let result else {
                completion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
                return
            }
            completion.complete(.success(result))
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        completion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        completion.complete(.failure(error))
        context.close(promise: nil)
    }

    private func sendRequest(context: ChannelHandlerContext) {
        guard !sent else { return }
        sent = true
        var headers = HTTPHeaders()
        headers.add(name: "host", value: authority)
        let head = HTTPRequestHead(version: .http1_1, method: .GET, uri: "/stream", headers: headers)
        context.write(wrapOutboundOut(.head(head)), promise: nil)
        context.writeAndFlush(wrapOutboundOut(.end(nil)), promise: nil)
    }
}
