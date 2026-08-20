import NIOCore

final class Phase3HTTP2SettingsObserver: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    private static let maximumSettingsBytes = 1_024

    private let completion: Phase2FixtureCompletion<Bool>
    private var bytes: [UInt8] = []
    private var completed = false

    init(completion: Phase2FixtureCompletion<Bool>) {
        self.completion = completion
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let buffer = unwrapInboundIn(data)
        if !completed {
            bytes.append(contentsOf: buffer.readableBytesView)
            inspect(context: context)
        }
        context.fireChannelRead(data)
    }

    func channelInactive(context: ChannelHandlerContext) {
        complete(.failure(Phase2FixtureError.closedBeforeExpectedBytes))
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        complete(.failure(error))
        context.fireErrorCaught(error)
    }

    private func inspect(context: ChannelHandlerContext) {
        guard bytes.count <= Self.maximumSettingsBytes else {
            complete(.failure(Phase2FixtureError.exceededByteLimit))
            context.close(promise: nil)
            return
        }
        guard bytes.count >= 9 else { return }
        let length = Int(bytes[0]) << 16 | Int(bytes[1]) << 8 | Int(bytes[2])
        guard bytes.count >= 9 + length else { return }
        guard bytes[3] == 0x04, bytes[5 ... 8].allSatisfy({ $0 == 0 }), length.isMultiple(of: 6) else {
            complete(.failure(Phase2FixtureError.unexpectedBytes))
            context.close(promise: nil)
            return
        }
        var enabled = false
        var offset = 9
        while offset < 9 + length {
            let identifier = UInt16(bytes[offset]) << 8 | UInt16(bytes[offset + 1])
            let value = UInt32(bytes[offset + 2]) << 24
                | UInt32(bytes[offset + 3]) << 16
                | UInt32(bytes[offset + 4]) << 8
                | UInt32(bytes[offset + 5])
            if identifier == 0x08 {
                enabled = value == 1
            }
            offset += 6
        }
        complete(.success(enabled))
    }

    private func complete(_ result: Result<Bool, Error>) {
        guard !completed else { return }
        completed = true
        completion.complete(result)
    }
}
