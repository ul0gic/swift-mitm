import Darwin
import Dispatch
import Foundation
import SwiftMITM
import SwiftMITMHTTP3

@main
struct HostMain {
    static func main() async {
        do {
            try await run()
        } catch {
            let category = (error as? HTTP3ProxyError).map { String(describing: $0) } ?? String(describing: type(of: error))
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
        guard arguments.count == 3, arguments[1] == "--configuration" else { throw HostError.arguments }
        let input = try Data(contentsOf: URL(fileURLWithPath: arguments[2]))
        let config = try JSONDecoder().decode(HostConfiguration.self, from: input)
        let authority = try CertificateAuthority.generate()
        let sink = HostCapture()
        let server = try HTTP3ProxyServer(
            configuration: config.configuration(), certificateAuthority: authority.authority, sink: sink
        )
        let resourcesPath = config.readyPath + ".resources.json"
        try await HostResources.write(server: server, phase: "initialized", path: resourcesPath)
        let port = try await server.start(port: 0)
        do {
            try await waitForStop(config, port: port, rootPEM: authority.certificatePEM, server: server, sink: sink)
        } catch {
            do { try await server.stop() } catch {
                try FileHandle.standardError.write(contentsOf: Data("HTTP3ProductionHost: cleanup-failed\n".utf8))
            }
            throw error
        }
        if let _ = try await server.stop() { throw HostError.unexpectedControlSession }
        try await HostResources.write(server: server, phase: "stopped", path: resourcesPath)
        try sink.write(to: config.readyPath + ".capture.json")
    }

    private static func waitForStop(
        _ config: HostConfiguration,
        port: Int,
        rootPEM: String,
        server: HTTP3ProxyServer,
        sink: HostCapture
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
        let certificatePath = config.readyPath + ".root.pem"
        try rootPEM.write(toFile: certificatePath, atomically: true, encoding: .utf8)
        try await HostResources.write(server: server, phase: "running", path: resourcesPath)
        let ready: [String: Any] = [
            "port": port, "authority": config.authority, "certificate": certificatePath,
            "resourcesPath": resourcesPath, "capturePath": config.readyPath + ".capture.json"
        ]
        try JSONSerialization.data(withJSONObject: ready)
            .write(to: URL(fileURLWithPath: config.readyPath), options: .atomic)
        let writer = Task {
            while !Task.isCancelled {
                try await Task.sleep(for: .milliseconds(100))
                try await HostResources.write(server: server, phase: "running", path: resourcesPath)
                try sink.write(to: config.readyPath + ".capture.json")
            }
        }
        for await _ in signals { break }
        writer.cancel()
        do { try await writer.value } catch is CancellationError {
            // Signal shutdown cancels only the host's periodic observation task.
        }
    }
}
