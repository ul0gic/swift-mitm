import Darwin
import Dispatch
import Foundation
import NIOCore
import NIOPosix
import SwiftMITM

struct BenchmarkOptions: Sendable {
    let protocolName: String
    let workload: String
    let identityDirectory: String
    let output: String
    let diagnosticCPU: Bool
    let diagnosticCallbacks: Bool

    init(_ arguments: [String]) throws {
        let flags = ["--diagnostic-cpu", "--diagnostic-http1-callbacks"]
        guard arguments.count == 5 || (arguments.count == 6 && flags.contains(arguments[5])),
              ["h2", "http/1.1"].contains(arguments[1]),
              ["latency", "throughput"].contains(arguments[2]) else { throw MeasurementError.arguments }
        protocolName = arguments[1]
        workload = arguments[2]
        identityDirectory = arguments[3]
        output = arguments[4]
        diagnosticCPU = arguments.count == 6
        diagnosticCallbacks = arguments.count == 6 && arguments[5] == "--diagnostic-http1-callbacks"
        guard !diagnosticCallbacks || (protocolName == "http/1.1" && workload == "throughput") else {
            throw MeasurementError.arguments
        }
    }
}

@main
struct PerformanceMain {
    static func main() async {
        do {
            try await run(BenchmarkOptions(CommandLine.arguments))
        } catch {
            do {
                try FileHandle.standardError.write(contentsOf: Data("benchmark: \(error)\n".utf8))
            } catch { exit(3) }
            exit(2)
        }
    }

    private static func run(_ options: BenchmarkOptions) async throws {
        let originGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let proxyGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let clientGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        do {
            try await exercise(options, originGroup: originGroup, proxyGroup: proxyGroup, clientGroup: clientGroup)
        } catch {
            try await clientGroup.shutdownGracefully()
            try await proxyGroup.shutdownGracefully()
            try await originGroup.shutdownGracefully()
            throw error
        }
        try await clientGroup.shutdownGracefully()
        try await proxyGroup.shutdownGracefully()
        try await originGroup.shutdownGracefully()
    }

    private static func exercise(
        _ options: BenchmarkOptions,
        originGroup: EventLoopGroup,
        proxyGroup: EventLoopGroup,
        clientGroup: EventLoopGroup
    ) async throws {
        let state = OriginState()
        let sink = MeasurementSink(diagnosticCallbacks: options.diagnosticCallbacks)
        let authority = try loadAuthority(options.identityDirectory + "/proxy")
        let originRoot = try String(
            contentsOfFile: options.identityDirectory + "/origin/origin-ca.pem",
            encoding: .utf8
        )
        let proxy = ProxyServer(
            certificateAuthority: authority,
            sink: sink,
            group: proxyGroup,
            upstreamPolicy: .init(additionalTrustRootsPEM: [originRoot]),
            egressPolicy: .init(allowInternal: true),
            captureBodyLimit: 0
        )
        let origin = try await startOrigin(
            group: originGroup,
            directory: options.identityDirectory,
            protocolName: options.protocolName,
            state: state
        )
        do {
            let proxyPort = try await proxy.start(port: 0)
            guard let originPort = origin.localAddress?.port else { throw MeasurementError.arguments }
            let input = ClientInput(
                proxyPort: proxyPort,
                originPort: originPort,
                protocolName: options.protocolName,
                workload: options.workload,
                rootPEM: authority.caCertificatePEM,
                diagnosticCPU: options.diagnosticCPU,
                diagnosticCallbacks: options.diagnosticCallbacks
            )
            let exchanges = try await runClient(group: clientGroup, input: input)
            try await proxy.stop()
            try await closeOrigin(origin, state: state)
            try writeResult(options, state: state, sink: sink, measurements: exchanges)
        } catch {
            try await proxy.stop()
            try await closeOrigin(origin, state: state)
            throw error
        }
    }

    private static func writeResult(
        _ options: BenchmarkOptions,
        state: OriginState,
        sink: MeasurementSink,
        measurements: ClientMeasurements
    ) throws {
        let snapshot = state.snapshot()
        let result = BenchmarkResult(
            protocolName: options.protocolName,
            workload: options.workload,
            downstreamALPN: options.protocolName,
            upstreamALPN: snapshot.protocols,
            originPaths: snapshot.paths,
            exchanges: measurements.exchanges,
            captures: try sink.snapshot(),
            processObservations: measurements.processObservations
        )
        try JSONEncoder().encode(result).write(to: URL(fileURLWithPath: options.output), options: .atomic)
    }

    private static func loadAuthority(_ path: String) throws -> CertificateAuthority {
        try CertificateAuthority(
            privateKeyPEM: String(contentsOfFile: path + "/origin-key.pem", encoding: .utf8),
            certificatePEM: String(contentsOfFile: path + "/origin-ca.pem", encoding: .utf8)
        )
    }

    private static func runClient(group: EventLoopGroup, input: ClientInput) async throws -> ClientMeasurements {
        try await withCheckedThrowingContinuation { continuation in
            DispatchQueue.global(qos: .userInitiated).async {
                do {
                    continuation.resume(returning: try measureClient(group: group, input: input))
                } catch {
                    continuation.resume(throwing: error)
                }
            }
        }
    }

    private static func closeOrigin(_ channel: Channel, state: OriginState) async throws {
        if channel.isActive {
            try await channel.close().get()
        }
        let children = state.children.withLockedValue { Array($0.values) }
        for child in children where child.isActive { try await child.close().get() }
    }
}
