import Darwin
import Dispatch
import Foundation
import SwiftMITM
import SwiftMITMHTTP3

enum HostSharedCeiling {
    private struct Node: Sendable {
        let server: HTTP3ProxyServer
        let sink: HostCapture
        let prefix: String
    }

    static func run(_ config: HostConfiguration) async throws {
        guard config.maximumRetainedBytes == 268_435_456,
              config.sharedRetainedBytes == 1_073_741_824 else { throw HostError.arguments }
        let authority = try CertificateAuthority.generate()
        let shared = ResourceAdmissionController(configuration: try ResourceAdmissionConfiguration(
            maximumConnections: 4096,
            maximumStreams: 16_384,
            maximumConnectionsPerDestination: 1024,
            maximumRetainedBytes: 1_073_741_824
        ))
        var nodes: [Node] = []
        do {
            for index in 0..<5 {
                let sink = HostCapture()
                let server = try HTTP3ProxyServer(
                    configuration: config.configuration(),
                    certificateAuthority: authority.authority,
                    sink: sink,
                    admission: shared
                )
                nodes.append(Node(server: server, sink: sink, prefix: config.readyPath + ".server-\(index)"))
            }
            try await serve(config, nodes: nodes, shared: shared, rootPEM: authority.certificatePEM)
        } catch {
            do { try await stop(nodes) } catch {
                try FileHandle.standardError
                    .write(contentsOf: Data("HTTP3ProductionHost: shared-cleanup-failed\n".utf8))
            }
            throw error
        }
    }

    private static func serve(
        _ config: HostConfiguration,
        nodes: [Node],
        shared: ResourceAdmissionController,
        rootPEM: String
    ) async throws {
        var endpoints: [[String: Any]] = []
        for (index, node) in nodes.enumerated() {
            let port = try await node.server.start(port: 0)
            endpoints.append([
                "index": index,
                "port": port,
                "resourcesPath": node.prefix + ".resources.json",
                "capturePath": node.prefix + ".capture.json"
            ])
        }
        try rootPEM.write(toFile: config.readyPath + ".root.pem", atomically: true, encoding: .utf8)
        try await snapshots(nodes, shared: shared, prefix: config.readyPath, phase: "running")
        let ready: [String: Any] = [
            "servers": endpoints,
            "authority": config.authority,
            "certificate": config.readyPath + ".root.pem",
            "sharedPath": config.readyPath + ".shared.json"
        ]
        try JSONSerialization.data(withJSONObject: ready)
            .write(to: URL(fileURLWithPath: config.readyPath), options: .atomic)
        try await waitForStop(nodes, shared: shared, prefix: config.readyPath)
        try await stop(nodes)
        try await snapshots(nodes, shared: shared, prefix: config.readyPath, phase: "stopped")
    }

    private static func waitForStop(
        _ nodes: [Node],
        shared: ResourceAdmissionController,
        prefix: String
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
        let writer = Task {
            do {
                while !Task.isCancelled {
                    try await Task.sleep(for: .milliseconds(100))
                    try await snapshots(nodes, shared: shared, prefix: prefix, phase: "running")
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

    private static func snapshots(
        _ nodes: [Node],
        shared: ResourceAdmissionController,
        prefix: String,
        phase: String
    ) async throws {
        let started = DispatchTime.now().uptimeNanoseconds
        for node in nodes {
            try await HostResources.write(server: node.server, phase: phase, path: node.prefix + ".resources.json")
            try node.sink.write(to: node.prefix + ".capture.json")
        }
        let usage = shared.usage
        let resident = try HostMemory.residentBytes()
        let value: [String: Any] = [
            "phase": phase,
            "sampleStartedNanoseconds": started,
            "sampleEndedNanoseconds": DispatchTime.now().uptimeNanoseconds,
            "residentBytes": resident,
            "shared": [
                "connections": usage.connections,
                "streams": usage.streams,
                "retainedBytes": usage.retainedBytes,
                "replayAttempts": usage.replayAttempts
            ]
        ]
        try JSONSerialization.data(withJSONObject: value, options: [.sortedKeys])
            .write(to: URL(fileURLWithPath: prefix + ".shared.json"), options: .atomic)
    }

    private static func stop(_ nodes: [Node]) async throws {
        var failure: any Error?
        for node in nodes {
            do { try await node.server.stop() } catch {
                if failure == nil {
                    failure = error
                }
            }
        }
        if let failure {
            throw failure
        }
    }
}
