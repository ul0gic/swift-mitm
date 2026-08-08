import Foundation
import NIOCore
import NIOHPACK
import NIOHTTP2

final class HTTP2CaptureTapHandler: ChannelInboundHandler {
    typealias InboundIn = HTTP2Frame.FramePayload
    typealias InboundOut = HTTP2Frame.FramePayload

    enum Direction {
        case request
        case response
    }

    private let direction: Direction
    private let requestID: UUID
    private let authority: String
    private let sink: CaptureEventSink
    private var bodyBuffer: CaptureBodyBuffer

    init(direction: Direction, requestID: UUID, authority: String, sink: CaptureEventSink, captureBodyLimit: Int = 0) {
        self.direction = direction
        self.requestID = requestID
        self.authority = authority
        self.sink = sink
        self.bodyBuffer = CaptureBodyBuffer(limit: captureBodyLimit)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let payload = unwrapInboundIn(data)
        switch payload {
        case .headers(let frame):
            emitHead(frame.headers)
            if frame.endStream {
                emitEnd()
            }
        case .data(let frame):
            captureBody(frame.data)
            if frame.endStream {
                emitEnd()
            }
        default:
            break
        }
        context.fireChannelRead(data)
    }

    private func captureBody(_ data: IOData) {
        let fullSize = data.readableBytes
        let bytes: [UInt8]
        switch data {
        case .byteBuffer(let buffer):
            bytes = bodyBuffer.take(buffer.readableBytesView)
        case .fileRegion:
            bytes = bodyBuffer.take(EmptyCollection<UInt8>())
        }
        sink.receive(
            direction == .request
                ? .requestBodyChunk(requestID: requestID, bytes: bytes, byteCount: fullSize)
                : .responseBodyChunk(requestID: requestID, bytes: bytes, byteCount: fullSize)
        )
    }

    private func emitHead(_ headers: HPACKHeaders) {
        let fields = headers.compactMap { name, value, _ -> HTTPHeaderField? in
            name.hasPrefix(":") ? nil : HTTPHeaderField(name: name, value: value)
        }
        switch direction {
        case .request:
            let head = CapturedRequestHead(
                id: requestID,
                timestamp: Date(),
                scheme: headers.first(name: ":scheme") ?? "https",
                authority: headers.first(name: ":authority") ?? authority,
                method: headers.first(name: ":method") ?? "",
                path: headers.first(name: ":path") ?? "",
                version: .http2,
                headers: fields
            )
            sink.receive(.requestHead(head))
        case .response:
            let status = headers.first(name: ":status").flatMap(Int.init) ?? 0
            let head = CapturedResponseHead(
                requestID: requestID,
                timestamp: Date(),
                status: status,
                version: .http2,
                headers: fields
            )
            sink.receive(.responseHead(head))
        }
    }

    private func emitEnd() {
        let truncated = bodyBuffer.truncated
        switch direction {
        case .request: sink.receive(.requestEnd(requestID: requestID, truncated: truncated))
        case .response: sink.receive(.responseEnd(requestID: requestID, truncated: truncated))
        }
    }
}
