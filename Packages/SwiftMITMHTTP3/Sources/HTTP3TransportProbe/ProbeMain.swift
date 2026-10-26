import Darwin
import Dispatch
import Foundation
import NIOCore
import NIOPosix
import NIOQUIC
import SwiftMITM

@main
struct ProbeMain {
    static func main() async throws {
        let arguments = try ProbeArguments(Array(CommandLine.arguments.dropFirst()))
        try arguments.authorize()
        let certificate = try String(contentsOfFile: arguments.certificatePath, encoding: .utf8)
        let key = try String(contentsOfFile: arguments.keyPath, encoding: .utf8)
        let authority = try CertificateAuthority(privateKeyPEM: key, certificatePEM: certificate)
        let identity = try authority.mintCertificateIdentity(forHost: arguments.hostname)
        let authenticator = try Authenticator(certificates: identity.certificateChain, privateKey: identity.privateKey)
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let loop = group.next()
        do {
            let state = try await loop.submit {
                NIOLoopBound(try ProbeTransport(arguments: arguments), eventLoop: loop)
            }
            .get()
            let verifier = try AsyncVerifier(trustRootsPath: arguments.originRootPath, eventLoop: loop)
            try await serve(arguments, state: state, loop: loop, authenticator: authenticator, verifier: verifier)
        } catch {
            try await group.shutdownGracefully()
            throw error
        }
        try await group.shutdownGracefully()
    }

    private static func serve(
        _ arguments: ProbeArguments,
        state: NIOLoopBound<ProbeTransport>,
        loop: EventLoop,
        authenticator: Authenticator,
        verifier: AsyncVerifier
    ) async throws {
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
            try await announceAndWait(arguments, channel: server)
        } catch {
            try await close(state: state, server: server, client: client, loop: loop)
            throw error
        }
        try await close(state: state, server: server, client: client, loop: loop)
    }

    private static func announceAndWait(_ arguments: ProbeArguments, channel: Channel) async throws {
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
        let data = try JSONSerialization.data(withJSONObject: [
            "port": port,
            "hostname": arguments.hostname,
            "originPort": originPort
        ])
        try data.write(to: URL(fileURLWithPath: arguments.readyPath), options: .atomic)
        for await _ in signals { break }
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
            try await EventLoopFuture.andAllSucceed([server.close(), client.close()], on: loop).get()
            throw error
        }
        try await EventLoopFuture.andAllSucceed([server.close(), client.close()], on: loop).get()
    }
}
