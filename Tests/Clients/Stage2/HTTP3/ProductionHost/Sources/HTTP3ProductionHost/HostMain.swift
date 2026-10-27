import Darwin
import Dispatch
import Foundation
import NIOPosix
import SwiftMITM
import SwiftMITMHTTP3

@main
struct HostMain {
    static func main() async {
        do {
            try await run()
        } catch {
            let category = (error as? HTTP3ProxyError)
                .map { String(describing: $0) } ?? String(describing: type(of: error))
            do {
                try FileHandle.standardError.write(contentsOf: Data("HTTP3ProductionHost: \(category)\n".utf8))
            } catch {
                exit(3)
            }
            exit(2)
        }
    }

    private static func run() async throws {
        let arguments = CommandLine.arguments
        guard arguments.count == 3,
              ["--configuration", "--shared-ceiling"].contains(arguments[1]) else { throw HostError.arguments }
        let input = try Data(contentsOf: URL(fileURLWithPath: arguments[2]))
        let config = try JSONDecoder().decode(HostConfiguration.self, from: input)
        if arguments[1] == "--shared-ceiling" {
            try await HostSharedCeiling.run(config)
            return
        }
        let authority = try CertificateAuthority.generate()
        let sink = HostCapture()
        let embedding = try HostEmbedding(config)
        let group = config.callerOwnedGroup == true ? MultiThreadedEventLoopGroup(numberOfThreads: 1) : nil
        let server = try HTTP3ProxyServer(
            configuration: config.configuration(),
            certificateAuthority: authority.authority,
            sink: sink,
            group: group,
            observationSink: embedding.observation,
            controlRuntime: embedding.runtime,
            controlEventSink: embedding.controlEvents,
            admission: embedding.shared
        )
        do {
            try await serve(config, rootPEM: authority.certificatePEM, server: server, sink: sink, embedding: embedding)
        } catch {
            do {
                try await server.stop()
                try await group?.shutdownGracefully()
            } catch {
                try FileHandle.standardError.write(contentsOf: Data("HTTP3ProductionHost: group-cleanup-failed\n".utf8))
            }
            throw error
        }
        try await group?.shutdownGracefully()
    }

    private static func serve(
        _ config: HostConfiguration,
        rootPEM: String,
        server: HTTP3ProxyServer,
        sink: HostCapture,
        embedding: HostEmbedding
    ) async throws {
        let resourcesPath = config.readyPath + ".resources.json"
        try await HostResources.write(server: server, phase: "initialized", path: resourcesPath)
        let port = try await server.start(port: 0)
        let lifecycle = HostLifecycleController(server: server, readyPath: config.readyPath, initialPort: port)
        do {
            try rootPEM.write(toFile: config.readyPath + ".root.pem", atomically: true, encoding: .utf8)
            try await waitForStop(
                config,
                port: port,
                server: server,
                sink: sink,
                embedding: embedding,
                lifecycle: lifecycle
            )
        } catch {
            embedding.controlEvents?.releaseBlocked()
            do { try await server.stop() } catch {
                try FileHandle.standardError.write(contentsOf: Data("HTTP3ProductionHost: cleanup-failed\n".utf8))
            }
            throw error
        }
        embedding.controlEvents?.releaseBlocked()
        let report = try await server.stop()
        if report != nil, embedding.runtime == nil {
            throw HostError.unexpectedControlSession
        }
        try await HostResources.write(server: server, phase: "stopped", path: resourcesPath)
        try sink.write(to: config.readyPath + ".capture.json")
        try await embedding.write(stopReport: report)
    }

    private static func waitForStop(
        _ config: HostConfiguration,
        port: Int,
        server: HTTP3ProxyServer,
        sink: HostCapture,
        embedding: HostEmbedding,
        lifecycle: HostLifecycleController
    ) async throws {
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
        let resourcesPath = config.readyPath + ".resources.json"
        try await HostResources.write(server: server, phase: "running", path: resourcesPath)
        try announce(config, port: port)
        let writer = Task {
            do {
                while !Task.isCancelled {
                    try await Task.sleep(for: .milliseconds(100))
                    try await embedding.commands?.poll()
                    try await lifecycle.poll()
                    try await HostResources.write(server: server, phase: lifecycle.phase, path: resourcesPath)
                    try sink.write(to: config.readyPath + ".capture.json")
                    try await embedding.write(stopReport: lifecycle.lastStopReport)
                }
            } catch {
                continuation.yield(())
                throw error
            }
        }
        for await _ in signals { break }
        writer.cancel()
        do { try await writer.value } catch is CancellationError {
            // Signal shutdown cancels only the host's periodic observation task.
        }
    }

    private static func announce(_ config: HostConfiguration, port: Int) throws {
        let certificatePath = config.readyPath + ".root.pem"
        let ready: [String: Any] = [
            "port": port,
            "authority": config.authority,
            "certificate": certificatePath,
            "resourcesPath": config.readyPath + ".resources.json",
            "capturePath": config.readyPath + ".capture.json",
            "prefix": config.readyPath
        ]
        try JSONSerialization.data(withJSONObject: ready)
            .write(to: URL(fileURLWithPath: config.readyPath), options: .atomic)
    }
}
