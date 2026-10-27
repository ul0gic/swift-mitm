import Dispatch
import NIOCore
import NIOPosix
import NIOQUIC
import SwiftMITM

actor HTTP3ServerLifecycle {
    enum Phase { case ready, starting, running, stopping, draining, shutdown }

    let inputs: HTTP3ServerInputs
    let group: any EventLoopGroup
    let ownsGroup: Bool
    let resources: HTTP3ResourceTracker
    let resolutionQueue = DispatchQueue(label: "SwiftMITM.HTTP3.bind-resolution")
    var phase = Phase.ready
    var transport: NIOLoopBound<HTTP3Transport>?
    var loop: (any EventLoop)?
    var controlSession: HTTPControlServingSession?

    init(inputs: HTTP3ServerInputs, group: any EventLoopGroup, ownsGroup: Bool, resources: HTTP3ResourceTracker) {
        self.inputs = inputs
        self.group = group
        self.ownsGroup = ownsGroup
        self.resources = resources
    }

    func start(host: String, port: Int) async throws -> Int {
        switch phase {
        case .running: throw ProxyServerError.alreadyRunning
        case .starting, .stopping, .draining: throw ProxyServerError.lifecycleOperationInProgress
        case .shutdown: throw ProxyServerError.eventLoopGroupShutdown
        case .ready: break
        }
        guard (0...65535).contains(port) else { throw HTTP3ProxyError.invalidConfiguration }
        phase = .starting
        do {
            try Task.checkCancellation()
            let address = try await resolveBind(host: host, port: port)
            try authorizeBind(address, host: host)
            guard !inputs.configuration.egressPolicy.denies(inputs.configuration.route.address) else {
                throw HTTP3ProxyError.egressDenied
            }
            controlSession = try await inputs.controlRuntime?.startServing(
                admission: resources.shared, transportAdmission: resources.local
            )
            let selectedLoop = group.next()
            loop = selectedLoop
            let identity = try inputs.certificateAuthority.mintCertificateIdentity(forHost: inputs.configuration.route.identity.host)
            let authenticator: Authenticator
            do { authenticator = try Authenticator(certificates: identity.certificateChain, privateKey: identity.privateKey) } catch {
                throw HTTP3ProxyError.invalidConfiguration
            }
            let inputs = inputs
            let resources = resources
            let state = try await selectedLoop.submit {
                NIOLoopBound(HTTP3Transport(
                    eventLoop: selectedLoop, configuration: inputs.configuration, sink: inputs.sink,
                    observationSink: inputs.observationSink, controlRuntime: inputs.controlRuntime,
                    controlEventSink: inputs.controlEventSink, resources: resources
                ), eventLoop: selectedLoop)
            }.get()
            transport = state
            let boundPort = try await bind(state: state, loop: selectedLoop, authenticator: authenticator, address: address)
            try Task.checkCancellation()
            phase = .running
            return boundPort
        } catch {
            try await rollbackStart()
            throw error
        }
    }

    func stop() async throws -> HTTPControlStopReport? {
        switch phase {
        case .starting, .stopping: throw ProxyServerError.lifecycleOperationInProgress
        case .shutdown: return nil
        case .ready, .running, .draining: phase = .stopping
        }
        do {
            if let transport, let loop { try await loop.flatSubmit { transport.value.stop() }.get() }
        } catch {
            phase = .draining
            throw HTTP3ProxyError.shutdownTimedOut
        }
        let report = await controlSession?.end()
        controlSession = nil
        transport = nil
        loop = nil
        if ownsGroup {
            phase = .shutdown
            do { try await group.shutdownGracefully() } catch { throw HTTP3ProxyError.shutdownTimedOut }
        } else { phase = .ready }
        return report
    }

    func resourceUsage() async -> HTTP3ResourceUsage {
        guard let transport, let loop else { return resources.snapshot() }
        do { return try await loop.submit { transport.value.sampleResources() }.get() } catch {
            resources.state.withLock { $0.nativeOutputSampleComplete = false }
            return resources.snapshot()
        }
    }

    private func rollbackStart() async throws {
        if let transport, let loop {
            do { try await loop.flatSubmit { transport.value.stop() }.get() } catch {
                phase = .draining
                throw HTTP3ProxyError.shutdownTimedOut
            }
        }
        _ = await controlSession?.end()
        controlSession = nil
        transport = nil
        loop = nil
        phase = .ready
    }

    func makeVerifier(on loop: any EventLoop) throws -> AsyncVerifier {
        if let certificates = inputs.configuration.certificates {
            do {
                return try AsyncVerifier(trustRoots: certificates, certificateVerification: .fullVerification, eventLoop: loop)
            } catch { throw HTTP3ProxyError.invalidConfiguration }
        }
        return AsyncVerifier(certificateVerification: .fullVerification, eventLoop: loop)
    }
}
