import NIOCore
import NIOPosix
import NIOSSL
import NIOTLS

enum Phase4TLSClientMode: Equatable, Sendable {
    case dnsSNI(String)
    case noSNI
    case ipIdentity(String)
    case encryptedClientHello
}

final class Phase4DirectTLSClient {
    private let group: EventLoopGroup
    private var channel: Channel?

    init(group: EventLoopGroup) {
        self.group = group
    }

    func rawClientHello(for mode: Phase4TLSClientMode) -> [UInt8] {
        switch mode {
        case .dnsSNI:
            Phase2TLSIngressVectors.http11ClientHello
        case .noSNI, .ipIdentity:
            Phase2TLSIngressVectors.noSNIClientHello
        case .encryptedClientHello:
            Self.addECH(to: Phase2TLSIngressVectors.http11ClientHello)
        }
    }

    func sendRawClientHello(port: Int, mode: Phase4TLSClientMode) throws {
        let channel = try connect(port: port)
        try phase4BoundedWait(channel.writeAndFlush(ByteBuffer(bytes: rawClientHello(for: mode))))
    }

    func installTLS(
        on channel: Channel,
        serverHostname: String?,
        trustRootPEM: String,
        applicationProtocols: [String]
    ) throws -> EventLoopFuture<String?> {
        var configuration = TLSConfiguration.makeClientConfiguration()
        configuration.applicationProtocols = applicationProtocols
        configuration.certificateVerification = .fullVerification
        configuration.trustRoots = .certificates([
            try NIOSSLCertificate(bytes: Array(trustRootPEM.utf8), format: .pem)
        ])
        let context = try NIOSSLContext(configuration: configuration)
        let completion = Phase2FixtureCompletion<String?>(eventLoop: channel.eventLoop)
        try phase4BoundedWait(channel.eventLoop.submit {
            try channel.pipeline.syncOperations.addHandlers([
                NIOSSLClientHandler(context: context, serverHostname: serverHostname),
                Phase4TLSHandshakeObserver(completion: completion)
            ])
        })
        self.channel = channel
        return completion.futureResult
    }

    func stop() {
        if let channel {
            try? phase4BoundedWait(channel.close())
        }
    }

    private func connect(port: Int) throws -> Channel {
        let channel = try phase4BoundedWait(ClientBootstrap(group: group)
            .connectTimeout(.seconds(2))
            .connect(host: "127.0.0.1", port: port))
        self.channel = channel
        return channel
    }

    private static func addECH(to input: [UInt8]) -> [UInt8] {
        var bytes = input
        let extensionBytes: [UInt8] = [0xFE, 0x0D, 0x00, 0x01, 0x01]
        let extensionLengthIndex = clientHelloExtensionLengthIndex(bytes)
        let extensionLength = integer(bytes[extensionLengthIndex], bytes[extensionLengthIndex + 1])
        bytes.append(contentsOf: extensionBytes)
        writeUInt16(extensionLength + extensionBytes.count, to: &bytes, at: extensionLengthIndex)
        writeUInt24(integer(bytes[6], bytes[7], bytes[8]) + extensionBytes.count, to: &bytes, at: 6)
        writeUInt16(integer(bytes[3], bytes[4]) + extensionBytes.count, to: &bytes, at: 3)
        return bytes
    }

    private static func clientHelloExtensionLengthIndex(_ bytes: [UInt8]) -> Int {
        var index = 9 + 34
        index += 1 + Int(bytes[index])
        index += 2 + integer(bytes[index], bytes[index + 1])
        index += 1 + Int(bytes[index])
        return index
    }

    private static func writeUInt16(_ value: Int, to bytes: inout [UInt8], at index: Int) {
        bytes[index] = UInt8((value >> 8) & 0xFF)
        bytes[index + 1] = UInt8(value & 0xFF)
    }

    private static func writeUInt24(_ value: Int, to bytes: inout [UInt8], at index: Int) {
        bytes[index] = UInt8((value >> 16) & 0xFF)
        bytes[index + 1] = UInt8((value >> 8) & 0xFF)
        bytes[index + 2] = UInt8(value & 0xFF)
    }

    private static func integer(_ bytes: UInt8...) -> Int {
        bytes.reduce(0) { ($0 << 8) | Int($1) }
    }
}

private final class Phase4TLSHandshakeObserver: ChannelInboundHandler {
    typealias InboundIn = NIOAny

    private let completion: Phase2FixtureCompletion<String?>

    init(completion: Phase2FixtureCompletion<String?>) {
        self.completion = completion
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if case TLSUserEvent.handshakeCompleted(let negotiatedProtocol) = event {
            completion.complete(.success(negotiatedProtocol))
        }
        context.fireUserInboundEventTriggered(event)
    }

    func channelInactive(context: ChannelHandlerContext) {
        completion.complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        completion.complete(.failure(error))
        context.close(promise: nil)
    }
}
