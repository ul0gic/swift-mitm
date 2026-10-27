import Foundation
import NIOCore
import NIOHTTP1
import NIOHTTP2
import NIOPosix
import NIOSSL

struct ClientInput: Sendable {
    let proxyPort: Int
    let originPort: Int
    let protocolName: String
    let workload: String
    let rootPEM: String
}

func measureClient(group: EventLoopGroup, input: ClientInput) throws -> [ExchangeMeasurement] {
    let channel = try openTunnel(group: group, input: input)
    do {
        let multiplexer = try configureClient(channel, input: input)
        let count = input.workload == "latency" ? 100 : 16
        let shared: NIOLoopBound<ResponseCollector>?
        if input.protocolName == "http/1.1" {
            shared = try addCollector(channel)
        } else {
            shared = nil
        }
        var result: [ExchangeMeasurement] = []
        for index in 0 ... count {
            let stream = try measurementStream(channel, multiplexer: multiplexer)
            let collector = try shared ?? addCollector(stream)
            let measurement = try request(
                channel: stream,
                collector: collector,
                path: "/\(input.workload)/\(index)",
                authority: "localhost:\(input.originPort)"
            )
            result.append(measurement)
        }
        try channel.close().wait()
        return result
    } catch {
        if channel.isActive {
            try channel.close().wait()
        }
        throw error
    }
}

private func measurementStream(
    _ channel: Channel,
    multiplexer: NIOHTTP2Handler.StreamMultiplexer?
) throws -> Channel {
    guard let multiplexer else { return channel }
    let future = multiplexer.createStreamChannel { stream in
        stream.eventLoop.makeCompletedFuture {
            try stream.pipeline.syncOperations.addHandler(HTTP2FramePayloadToHTTP1ClientCodec(httpProtocol: .https))
        }
    }
    return try future.wait()
}

private func openTunnel(group: EventLoopGroup, input: ClientInput) throws -> Channel {
    let completion = Completion<Void>(on: group.next())
    let channel = try ClientBootstrap(group: group)
        .connectTimeout(.seconds(5))
        .channelInitializer { channel in
            channel.eventLoop.makeCompletedFuture {
                try channel.pipeline.syncOperations.addHandler(TunnelResponse(completion), name: "connect")
            }
        }
        .connect(host: "127.0.0.1", port: input.proxyPort)
        .wait()
    let authority = "localhost:\(input.originPort)"
    var bytes = ByteBuffer()
    bytes.writeString("CONNECT \(authority) HTTP/1.1\r\nHost: \(authority)\r\n\r\n")
    try channel.writeAndFlush(bytes).wait()
    try completion.wait(seconds: 10)
    try channel.pipeline.removeHandler(name: "connect").wait()
    return channel
}

private func configureClient(
    _ channel: Channel,
    input: ClientInput
) throws -> NIOHTTP2Handler.StreamMultiplexer? {
    var configuration = TLSConfiguration.makeClientConfiguration()
    configuration.applicationProtocols = [input.protocolName]
    configuration.certificateVerification = .fullVerification
    configuration.trustRoots = .certificates([
        try NIOSSLCertificate(bytes: Array(input.rootPEM.utf8), format: .pem)
    ])
    let context = try NIOSSLContext(configuration: configuration)
    let handshake = Completion<String>(on: channel.eventLoop)
    try channel.eventLoop
        .submit {
            try channel.pipeline.syncOperations.addHandlers([
                NIOSSLClientHandler(context: context, serverHostname: "localhost"),
                HandshakeObserver(handshake)
            ])
        }
        .wait()
    let multiplexer: NIOHTTP2Handler.StreamMultiplexer?
    if input.protocolName == "h2" {
        let pipeline = channel.configureHTTP2Pipeline(
            mode: .client,
            connectionConfiguration: .init(),
            streamConfiguration: .init()
        ) { $0.close() }
        multiplexer = try pipeline.wait()
    } else {
        try channel.pipeline.addHTTPClientHandlers().wait()
        multiplexer = nil
    }
    guard try handshake.wait(seconds: 10) == input.protocolName else { throw MeasurementError.protocolMismatch }
    return multiplexer
}

private func addCollector(_ channel: Channel) throws -> NIOLoopBound<ResponseCollector> {
    try channel.eventLoop
        .submit {
            let collector = ResponseCollector()
            try channel.pipeline.syncOperations.addHandler(collector, name: "measurement")
            return NIOLoopBound(collector, eventLoop: channel.eventLoop)
        }
        .wait()
}

private func request(
    channel: Channel,
    collector: NIOLoopBound<ResponseCollector>,
    path: String,
    authority: String
) throws -> ExchangeMeasurement {
    let completion = Completion<ExchangeMeasurement>(on: channel.eventLoop)
    try channel.eventLoop
        .submit {
            let context = try channel.pipeline.syncOperations.context(name: "measurement")
            collector.value.send(path: path, authority: authority, completion: completion, context: context)
        }
        .wait()
    return try completion.wait(seconds: 120)
}
