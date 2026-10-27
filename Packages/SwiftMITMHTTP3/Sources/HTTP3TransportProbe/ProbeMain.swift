import Darwin
import Dispatch
import Foundation
import NIOCore
import NIOPosix
import NIOQUIC
import SwiftMITM

@main
struct ProbeMain {
    static func main() async {
        do {
            try await run()
        } catch {
            let message: String
            if let probeError = error as? ProbeError, case .deniedRoute = probeError {
                message = "HTTP3TransportProbe: denied-route"
            } else if let error = error as? CertificateAuthorityRestorationError {
                message = "HTTP3TransportProbe: certificate-authority-\(error)"
            } else {
                message = "HTTP3TransportProbe: startup-failure \(type(of: error))"
            }
            do {
                try FileHandle.standardError.write(contentsOf: Data((message + "\n").utf8))
            } catch {
                exit(3)
            }
            exit(2)
        }
    }

    private static func run() async throws {
        let arguments = try ProbeArguments(Array(CommandLine.arguments.dropFirst()))
        try arguments.authorize()
        let certificate = try String(contentsOfFile: arguments.certificatePath, encoding: .utf8)
        let key = try String(contentsOfFile: arguments.keyPath, encoding: .utf8)
        let authority = try CertificateAuthority(privateKeyPEM: key, certificatePEM: certificate)
        let identity = try authority.mintCertificateIdentity(forHost: arguments.hostname)
        let authenticator = try Authenticator(certificates: identity.certificateChain, privateKey: identity.privateKey)
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let loop = group.next()
        let metrics: ProbeMetrics
        do {
            let state = try await loop.submit {
                NIOLoopBound(try ProbeTransport(arguments: arguments), eventLoop: loop)
            }
            .get()
            let verifier = try AsyncVerifier(trustRootsPath: arguments.originRootPath, eventLoop: loop)
            metrics = try await serve(
                arguments,
                state: state,
                loop: loop,
                authenticator: authenticator,
                verifier: verifier
            )
        } catch {
            try await group.shutdownGracefully()
            throw error
        }
        try await group.shutdownGracefully()
        try metrics.write(to: arguments.readyPath + ".metrics.json", stopped: true)
    }

    private static func serve(
        _ arguments: ProbeArguments,
        state: NIOLoopBound<ProbeTransport>,
        loop: EventLoop,
        authenticator: Authenticator,
        verifier: AsyncVerifier
    ) async throws -> ProbeMetrics {
        let metrics = try await loop.submit { state.value.metrics }.get()
        let client = try await ProbeTransport.startClient(state: state, loop: loop, verifier: verifier).get()
        let server: Channel
        do {
            server = try await loop.flatSubmit {
                ProbeTransport.startServer(state: state, loop: loop, authenticator: authenticator)
            }
            .get()
        } catch {
            try await client.close().get()
            throw error
        }
        do {
            try await announceAndWait(arguments, channel: server, metrics: metrics)
        } catch {
            try await close(state: state, server: server, client: client, loop: loop)
            throw error
        }
        try await close(state: state, server: server, client: client, loop: loop)
        return metrics
    }

    private static func announceAndWait(
        _ arguments: ProbeArguments,
        channel: Channel,
        metrics: ProbeMetrics
    ) async throws {
        guard let port = channel.localAddress?.port, let originPort = arguments.originAddress.port else {
            throw ProbeError.arguments
        }
        let (signals, continuation) = AsyncStream<Void>.makeStream(bufferingPolicy: .bufferingNewest(1))
        signal(SIGINT, SIG_IGN)
        signal(SIGTERM, SIG_IGN)
        let sources = [SIGINT, SIGTERM].map { number in
            let source = DispatchSource.makeSignalSource(signal: number, queue: .global())
            source.setEventHandler { continuation.yield(()) }
            source.resume()
            return source
        }
        defer {
            sources.forEach { $0.cancel() }
            continuation.finish()
        }
        let metricsPath = arguments.readyPath + ".metrics.json"
        try metrics.write(to: metricsPath)
        let writer = Task {
            while !Task.isCancelled {
                try await Task.sleep(for: .milliseconds(100))
                try await metrics.sampleNativeOutput()
                try metrics.write(to: metricsPath)
            }
        }
        defer { writer.cancel() }
        let data = try JSONSerialization.data(withJSONObject: [
            "port": port,
            "hostname": arguments.hostname,
            "originPort": originPort,
            "metricsPath": metricsPath
        ])
        try data.write(to: URL(fileURLWithPath: arguments.readyPath), options: .atomic)
        for await _ in signals { break }
        writer.cancel()
        do {
            try await writer.value
        } catch is CancellationError {
            // The periodic writer stops with the signal-driven probe shutdown.
        }
    }

    private static func close(
        state: NIOLoopBound<ProbeTransport>,
        server: Channel,
        client: Channel,
        loop: EventLoop
    ) async throws {
        let shutdown = loop.flatSubmit {
            let deadline = NIODeadline.now() + .seconds(2)
            let handlers = [state.value.serverHandler, state.value.clientHandler].compactMap(\.self)
            return EventLoopFuture.andAllSucceed(
                handlers.map { $0.value.shutdownGracefully(deadline: deadline) },
                on: loop
            )
        }
        do {
            try await shutdown.get()
        } catch {
            try await closeSocketsAndStorage(state: state, server: server, client: client, loop: loop)
            throw error
        }
        try await closeSocketsAndStorage(state: state, server: server, client: client, loop: loop)
    }

    private static func closeSocketsAndStorage(
        state: NIOLoopBound<ProbeTransport>,
        server: Channel,
        client: Channel,
        loop: EventLoop
    ) async throws {
        var closeError: any Error?
        do {
            try await EventLoopFuture.andAllSucceed([server.close(), client.close()], on: loop).get()
        } catch {
            closeError = error
        }
        try await loop.flatSubmit {
            EventLoopFuture.andAllSucceed(Array(state.value.connectionStorage.values), on: loop)
        }
        .get()
        if let closeError {
            throw closeError
        }
    }
}
