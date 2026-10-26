import NIOCore

final class Phase2TransparentTLSClient {
    enum Offer: Sendable {
        case http11
        case http2
        case noSNI
    }

    private let client: Phase2LoopbackByteClient

    init(group: EventLoopGroup) {
        self.client = Phase2LoopbackByteClient(group: group)
    }

    func connectDirectly(port: Int, offer: Offer) throws {
        try client.connect(port: port)
        try client.write(bytes(for: offer))
    }

    func stop() {
        client.stop()
    }

    private func bytes(for offer: Offer) -> [UInt8] {
        switch offer {
        case .http11:
            Phase2TLSIngressVectors.http11ClientHello
        case .http2:
            Phase2TLSIngressVectors.http2ClientHello
        case .noSNI:
            Phase2TLSIngressVectors.noSNIClientHello
        }
    }
}
