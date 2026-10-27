import CryptoKit
import Dispatch
import Foundation
import NIOCore
import NIOHTTP1
import NIOTLS

final class TunnelResponse: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = ByteBuffer
    private let completion: Completion<Void>
    private var bytes = ByteBuffer()
    private var finished = false

    init(_ completion: Completion<Void>) { self.completion = completion }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard !finished else { return }
        var incoming = unwrapInboundIn(data)
        bytes.writeBuffer(&incoming)
        guard bytes.readableBytes <= 8192 else {
            completion.complete(.failure(MeasurementError.responseMismatch))
            context.close(promise: nil)
            return
        }
        guard bytes.readableBytesView.firstRange(of: [13, 10, 13, 10]) != nil else { return }
        finished = true
        guard let head = bytes.getString(at: bytes.readerIndex, length: bytes.readableBytes),
              head.hasPrefix("HTTP/1.1 200 ") else {
            completion.complete(.failure(MeasurementError.responseMismatch))
            return
        }
        completion.complete(.success(()))
    }

    func channelInactive(context: ChannelHandlerContext) {
        completion.complete(.failure(MeasurementError.unexpectedClosure))
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        completion.complete(.failure(error))
        context.close(promise: nil)
    }
}

final class HandshakeObserver: ChannelInboundHandler {
    typealias InboundIn = NIOAny
    private let completion: Completion<String>

    init(_ completion: Completion<String>) { self.completion = completion }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if case let TLSUserEvent.handshakeCompleted(selected) = event {
            if let selected { completion.complete(.success(selected)) }
            else { completion.complete(.failure(MeasurementError.protocolMismatch)) }
        }
        context.fireUserInboundEventTriggered(event)
    }

    func channelInactive(context: ChannelHandlerContext) {
        completion.complete(.failure(MeasurementError.unexpectedClosure))
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        completion.complete(.failure(error))
        context.close(promise: nil)
    }
}

final class ResponseCollector: ChannelDuplexHandler {
    typealias InboundIn = HTTPClientResponsePart
    typealias OutboundIn = HTTPClientRequestPart
    typealias OutboundOut = HTTPClientRequestPart

    private struct Pending {
        let path: String
        var start: UInt64?
        let completion: Completion<ExchangeMeasurement>
        var bytes = 0
        var heads = 0
        var hash = SHA256()
    }

    private var pending: Pending?

    func send(
        path: String,
        authority: String,
        completion: Completion<ExchangeMeasurement>,
        context: ChannelHandlerContext
    ) {
        guard pending == nil else {
            completion.complete(.failure(MeasurementError.responseMismatch))
            return
        }
        let head = HTTPRequestHead(
            version: .http1_1, method: .GET, uri: path, headers: HTTPHeaders([("host", authority)])
        )
        pending = Pending(path: path, completion: completion)
        pending?.start = DispatchTime.now().uptimeNanoseconds
        context.write(wrapOutboundOut(.head(head)), promise: nil)
        context.writeAndFlush(wrapOutboundOut(.end(nil)), promise: nil)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard var current = pending else {
            context.close(promise: nil)
            return
        }
        switch unwrapInboundIn(data) {
        case let .head(head):
            guard head.status == .ok, current.heads == 0 else {
                current.completion.complete(.failure(MeasurementError.responseMismatch))
                context.close(promise: nil)
                return
            }
            current.heads += 1
        case let .body(buffer):
            current.bytes += buffer.readableBytes
            current.hash.update(data: Data(buffer.readableBytesView))
        case .end:
            let end = DispatchTime.now().uptimeNanoseconds
            guard current.heads == 1, let start = current.start else {
                current.completion.complete(.failure(MeasurementError.responseMismatch))
                return
            }
            pending = nil
            current.completion.complete(.success(ExchangeMeasurement(
                path: current.path, start: start, end: end, bytes: current.bytes,
                sha256: current.hash.finalize().map { String(format: "%02x", $0) }.joined()
            )))
            return
        }
        pending = current
    }

    func channelInactive(context: ChannelHandlerContext) {
        pending?.completion.complete(.failure(MeasurementError.unexpectedClosure))
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        pending?.completion.complete(.failure(error))
        context.close(promise: nil)
    }
}
