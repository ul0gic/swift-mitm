import HTTPTypes
import NIOCore
import NIOHTTPTypes

enum HTTP3Message {
    case request(HTTPRequestPart)
    case response(HTTPResponsePart)

    var body: ByteBuffer? {
        switch self {
        case let .request(.body(body)), let .response(.body(body)): body
        default: nil
        }
    }

    var endsMessage: Bool {
        switch self {
        case .request(.end), .response(.end): true
        default: false
        }
    }

    var payloadBytes: Int {
        switch self {
        case let .request(.head(head)):
            head.method.rawValue.utf8.count + (head.authority?.utf8.count ?? 0)
                + (head.path?.utf8.count ?? 0) + (head.scheme?.utf8.count ?? 0) + Self.fieldBytes(head.headerFields)
        case let .response(.head(head)): 3 + Self.fieldBytes(head.headerFields)
        case let .request(.body(body)), let .response(.body(body)): body.readableBytes
        case let .request(.end(fields)), let .response(.end(fields)): fields.map(Self.fieldBytes) ?? 0
        }
    }

    var retainedBytes: Int {
        switch self {
        case let .request(.body(body)), let .response(.body(body)):
            var capacity = 1
            while capacity < body.readableBytes { capacity *= 2 }
            return body.readableBytes == 0 ? 0 : capacity
        case let .request(.head(head)): payloadBytes + head.headerFields.count * MemoryLayout<HTTPField>.stride
        case let .response(.head(head)): payloadBytes + head.headerFields.count * MemoryLayout<HTTPField>.stride
        case let .request(.end(fields)), let .response(.end(fields)):
            payloadBytes + (fields?.count ?? 0) * MemoryLayout<HTTPField>.stride
        }
    }

    func compacted() -> Self {
        guard let body else { return self }
        return replacingBody(ByteBuffer(bytes: body.readableBytesView))
    }

    func replacingBody(_ body: ByteBuffer) -> Self {
        switch self {
        case .request: .request(.body(body))
        case .response: .response(.body(body))
        }
    }

    func write(to channel: any Channel) -> EventLoopFuture<Void> {
        switch self {
        case let .request(part): channel.writeAndFlush(part)
        case let .response(part): channel.writeAndFlush(part)
        }
    }

    private static func fieldBytes(_ fields: HTTPFields) -> Int {
        fields.reduce(0) { $0 + $1.name.rawName.utf8.count + $1.withUnsafeBytesOfValue { $0.count } }
    }
}
