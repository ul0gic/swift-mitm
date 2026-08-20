import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
import NIOSSL

public final class ProxyServer: Sendable {
    private enum LifecycleState {
        case ready
        case starting
        case running(Channel, ProxyTLSRuntime)
        case stopping
        case shutdown
    }

    public struct UpstreamPolicy: Sendable {
        public var verifyCertificate: Bool
        public var additionalTrustRootsPEM: [String]

        public init(verifyCertificate: Bool = true, additionalTrustRootsPEM: [String] = []) {
            self.verifyCertificate = verifyCertificate
            self.additionalTrustRootsPEM = additionalTrustRootsPEM
        }

        public static let `default` = UpstreamPolicy()
    }

    static let encoderName = "swiftmitm.connect.encoder"
    static let decoderName = "swiftmitm.connect.decoder"

    private let group: EventLoopGroup
    private let ownsGroup: Bool
    let authority: CertificateAuthority
    let sink: CaptureEventSink
    let upstreamPolicy: UpstreamPolicy
    let egressPolicy: EgressPolicy
    let ingress: ProxyIngress
    let timeoutPolicy: ProxyTimeoutPolicy
    private let allowNonLoopbackBind: Bool
    let targetWindowSize: Int
    let captureBodyLimit: Int
    let opaqueCaptureByteLimit: Int
    private let lifecycleState: NIOLockedValueBox<LifecycleState>
    let connectionRegistry = ProxyConnectionRegistry()

    public init(
        certificateAuthority: CertificateAuthority,
        sink: CaptureEventSink,
        group: EventLoopGroup? = nil,
        upstreamPolicy: UpstreamPolicy = .default,
        egressPolicy: EgressPolicy = .default,
        ingress: ProxyIngress = .explicitConnect,
        timeoutPolicy: ProxyTimeoutPolicy = .default,
        allowNonLoopbackBind: Bool = false,
        targetWindowSize: Int = 65535,
        captureBodyLimit: Int = 0,
        opaqueCaptureByteLimit: Int = 0
    ) {
        self.authority = certificateAuthority
        self.sink = sink
        self.upstreamPolicy = upstreamPolicy
        self.egressPolicy = egressPolicy
        self.ingress = ingress
        self.timeoutPolicy = timeoutPolicy
        self.allowNonLoopbackBind = allowNonLoopbackBind
        self.targetWindowSize = targetWindowSize
        self.captureBodyLimit = captureBodyLimit
        self.opaqueCaptureByteLimit = max(0, opaqueCaptureByteLimit)
        if let group {
            self.group = group
            self.ownsGroup = false
        } else {
            self.group = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
            self.ownsGroup = true
        }
        self.lifecycleState = NIOLockedValueBox(.ready)
    }

    @discardableResult
    public func start(host: String = "127.0.0.1", port: Int) async throws -> Int {
        guard allowNonLoopbackBind || Self.isLoopbackHost(host) else {
            throw ProxyServerError.nonLoopbackBindRejected(host)
        }
        try lifecycleState.withLockedValue { state in
            switch state {
            case .ready:
                state = .starting
            case .running:
                throw ProxyServerError.alreadyRunning
            case .starting, .stopping:
                throw ProxyServerError.lifecycleOperationInProgress
            case .shutdown:
                throw ProxyServerError.eventLoopGroupShutdown
            }
        }
        connectionRegistry.startAccepting()
        var tlsRuntime: ProxyTLSRuntime?
        do {
            let runtime = try await ProxyTLSRuntime.make(authority: authority, upstreamPolicy: upstreamPolicy)
            tlsRuntime = runtime
            let bootstrap = ServerBootstrap(group: group)
                .serverChannelOption(ChannelOptions.backlog, value: 256)
                .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
                .childChannelInitializer { [self] clientChannel in
                    configureInbound(clientChannel, tlsRuntime: runtime)
                }
            if case .trustedProxyV2 = ingress {
                _ = bootstrap.childChannelOption(ChannelOptions.allowRemoteHalfClosure, value: true)
            }
            let channel = try await bootstrap
                .bind(host: host, port: port)
                .get()
            lifecycleState.withLockedValue { $0 = .running(channel, runtime) }
            return channel.localAddress?.port ?? port
        } catch {
            await shutDownConnections()
            _ = await tlsRuntime?.shutdownGracefully()
            lifecycleState.withLockedValue { state in
                if case .starting = state {
                    state = .ready
                }
            }
            throw error
        }
    }

    public func stop() async throws {
        let resources = try transitionToStopping()
        guard resources.channel != nil || ownsGroup else { return }
        let connections = connectionRegistry.beginShutdown()
        let listenerError = await closeListener(resources.channel)
        forceClose(connections)
        await connectionRegistry.waitForQuiescence()
        connectionRegistry.finishShutdown()
        let tlsRuntimeError = await resources.tlsRuntime?.shutdownGracefully()
        let groupError = await shutDownOwnedGroup()
        lifecycleState.withLockedValue { state in
            state = ownsGroup ? .shutdown : .ready
        }
        if let error = listenerError ?? tlsRuntimeError ?? groupError {
            throw error
        }
    }

    private func shutDownConnections() async {
        let connections = connectionRegistry.beginShutdown()
        forceClose(connections)
        await connectionRegistry.waitForQuiescence()
        connectionRegistry.finishShutdown()
    }

    private func forceClose(_ channels: [Channel]) {
        for channel in channels {
            channel.eventLoop.execute {
                if let handler = try? channel.pipeline.syncOperations.handler(
                    type: TransparentConnectionFailureHandler.self
                ) {
                    handler.cancel()
                }
                if let handler = try? channel.pipeline.syncOperations.handler(type: OpaqueFlowBridgeHandler.self) {
                    handler.cancel()
                }
            }
        }
        for channel in channels {
            channel.eventLoop.execute {
                if let context = try? channel.pipeline.syncOperations.context(handlerType: NIOSSLClientHandler.self) {
                    context.close(promise: nil)
                } else if let context = try? channel.pipeline.syncOperations.context(
                    handlerType: NIOSSLServerHandler.self
                ) {
                    context.close(promise: nil)
                } else {
                    channel.close(promise: nil)
                }
            }
        }
    }

    private static func isLoopbackHost(_ host: String) -> Bool {
        if host == "localhost" {
            return true
        }
        guard let address = try? SocketAddress(ipAddress: host, port: 0) else { return false }
        return EgressPolicy.isLoopback(address)
    }

    private func transitionToStopping() throws -> (channel: Channel?, tlsRuntime: ProxyTLSRuntime?) {
        try lifecycleState.withLockedValue { state in
            switch state {
            case .ready where ownsGroup:
                state = .stopping
                return (nil, nil)
            case .ready, .shutdown:
                return (nil, nil)
            case let .running(channel, tlsRuntime):
                state = .stopping
                return (channel, tlsRuntime)
            case .starting, .stopping:
                throw ProxyServerError.lifecycleOperationInProgress
            }
        }
    }

    private func closeListener(_ channel: Channel?) async -> Error? {
        guard let channel else { return nil }
        do {
            try await channel.close().get()
            return nil
        } catch {
            return error
        }
    }

    private func shutDownOwnedGroup() async -> Error? {
        guard ownsGroup else { return nil }
        do {
            try await group.shutdownGracefully()
            return nil
        } catch {
            return error
        }
    }
}

public enum ProxyServerError: Error, Equatable, Sendable {
    case alreadyRunning
    case eventLoopGroupShutdown
    case lifecycleOperationInProgress
    case nonLoopbackBindRejected(String)
}

enum ProxyConnectionError: Error, Equatable, Sendable {
    case egressBlocked(String)
    case serverStopping
    case setupTimedOut
}
