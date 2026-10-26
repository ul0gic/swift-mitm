import Darwin
import Foundation
import SwiftMITM
import SwiftMITMInspection

struct Arguments {
    let command: String
    let certificatePath: String
    let keyPath: String
    let value: String?
    let exportPath: String?
    let allowInternal: Bool
    let originCAPath: String?
    let editHeader: (name: String, value: String)?

    init(_ arguments: [String]) throws {
        guard arguments.count >= 3 else { throw LabError.usage }
        command = arguments[0]
        certificatePath = arguments[1]
        keyPath = arguments[2]
        if command == "serve" {
            value = nil
            exportPath = nil
            let options = try ServeOptions(arguments)
            allowInternal = options.allowInternal
            originCAPath = options.originCAPath
            editHeader = options.editHeader
        } else if command == "replay", arguments.count >= 4 {
            value = arguments[3]
            var allowInternal = false
            var originCAPath: String?
            var exportPath: String?
            var index = 4
            while index < arguments.count {
                switch arguments[index] {
                case "--allow-internal":
                    allowInternal = true
                    index += 1
                case "--origin-ca" where index + 1 < arguments.count:
                    originCAPath = arguments[index + 1]
                    index += 2
                case let path where !path.hasPrefix("--") && exportPath == nil:
                    exportPath = path
                    index += 1
                default: throw LabError.usage
                }
            }
            self.exportPath = exportPath
            self.allowInternal = allowInternal
            self.originCAPath = originCAPath
            editHeader = nil
        } else {
            throw LabError.usage
        }
    }
}

private struct ServeOptions {
    var allowInternal = false
    var originCAPath: String?
    var editHeader: (name: String, value: String)?

    init(_ arguments: [String]) throws {
        var index = 3
        while index < arguments.count {
            switch arguments[index] {
            case "--allow-internal":
                allowInternal = true
                index += 1
            case "--origin-ca" where index + 1 < arguments.count:
                originCAPath = arguments[index + 1]
                index += 2
            case "--edit-header" where index + 1 < arguments.count:
                let value = arguments[index + 1]
                guard let separator = value.firstIndex(of: "=") else { throw LabError.usage }
                editHeader = (String(value[..<separator]), String(value[value.index(after: separator)...]))
                index += 2
            default: throw LabError.usage
            }
        }
    }
}

private struct LabServer {
    let server: ProxyServer
    let inspector: HTTPInspector
    let breakpoints: ConsoleBreakpointSink
    let limits: HTTPControlLimits
}

enum LabError: Error {
    case usage
    case invalidURL
    case missingResponse
    case outputFile
    case missingHARTimings(ReplayTimingUnavailable?)
    case missingHARContentSize
}

@main
struct InterceptionLab {
    static func main() async {
        do {
            let arguments = try Arguments(Array(CommandLine.arguments.dropFirst()))
            try await run(arguments)
        } catch LabError.usage {
            writeError(
                "Usage: InterceptionLab serve CA_CERT CA_KEY [--allow-internal] [--origin-ca PATH] "
                    + "[--edit-header NAME=VALUE] | replay CA_CERT CA_KEY URL [HAR_PATH] "
                    + "[--allow-internal] [--origin-ca PATH]"
            )
            exit(2)
        } catch let LabError.missingHARTimings(reason) {
            writeError(
                "HAR export unavailable: measured replay timings are missing "
                    + "(\(reason?.label ?? "unknown reason"))."
            )
            exit(1)
        } catch LabError.missingHARContentSize {
            writeError(
                "HAR export unavailable: replay could not establish decoded response content size; "
                    + "the current replay result reports it only for identity-coded responses."
            )
            exit(1)
        } catch {
            writeError("InterceptionLab failed; check the CA inputs, URL, policy, and output path.")
            exit(1)
        }
    }

    private static func run(_ arguments: Arguments) async throws {
        let certificate = try String(contentsOfFile: arguments.certificatePath, encoding: .utf8)
        let key = try String(contentsOfFile: arguments.keyPath, encoding: .utf8)
        let authority = try CertificateAuthority(privateKeyPEM: key, certificatePEM: certificate)
        let output = try ConsoleOutput()
        let consumer = Task { await output.consume() }
        do {
            let emitStopped: Bool
            switch arguments.command {
            case "serve": emitStopped = try await serve(arguments, authority: authority, output: output)
            case "replay":
                try await replay(arguments, authority: authority, output: output)
                emitStopped = true
            default: throw LabError.usage
            }
            if arguments.command == "replay" {
                output.finish(cancel: false)
            }
            await consumer.value
            if emitStopped {
                try await output.complete()
            } else {
                try output.restoreFlags()
            }
        } catch {
            output.finish(cancel: false)
            await consumer.value
            try output.restoreFlags()
            throw error
        }
    }

    private static func serve(
        _ arguments: Arguments,
        authority: CertificateAuthority,
        output: ConsoleOutput
    ) async throws -> Bool {
        let components = try makeServer(
            arguments,
            authority: authority,
            output: output
        )
        let server = components.server
        let inspector = components.inspector
        let breakpoints = components.breakpoints
        let limits = components.limits
        let editHeader = arguments.editHeader
        let breakpointConsumer = Task {
            for await handle in breakpoints.handles {
                await resolve(handle, limits: limits, editHeader: editHeader, output: output)
            }
        }
        let signals = stopSignals()
        do {
            let port = try await server.start(port: 8080)
            output.emit(
                "listening http://127.0.0.1:\(port); requests to /debug pause for a configured decision; "
                    + "internal-egress=\(arguments.allowInternal)"
            )
            let receivedSignal = await waitForStop(signals.stream, output: output)
            try await server.stop()
            breakpoints.finish()
            breakpointConsumer.cancel()
            await breakpointConsumer.value
            let report = await inspector.shutdown()
            let inspectionComplete = report.drained && !report.observationLossDetected
            output.emit(
                "inspectionComplete=\(inspectionComplete) "
                    + "lossDetected=\(report.observationLossDetected) queued=\(report.queuedItems) "
                    + "retainedBytes=\(report.retainedBytes)"
            )
            signals.cancel()
            if receivedSignal == SIGTERM {
                return false
            }
            output.finish(cancel: false)
            return inspectionComplete
        } catch {
            signals.cancel()
            breakpoints.finish()
            breakpointConsumer.cancel()
            do { try await server.stop() } catch { writeError("Proxy cleanup failed.") }
            await breakpointConsumer.value
            _ = await inspector.shutdown()
            throw error
        }
    }

    private static func waitForStop(_ signals: AsyncStream<Int32>, output: ConsoleOutput) async -> Int32? {
        for await number in signals {
            if number == SIGTERM {
                output.finish(cancel: true)
            }
            return number
        }
        return nil
    }

    private static func makeServer(
        _ arguments: Arguments,
        authority: CertificateAuthority,
        output: ConsoleOutput
    ) throws -> LabServer {
        let inspection = ConsoleInspectionSink(output: output)
        let admission = ResourceAdmissionController(configuration: try ResourceAdmissionConfiguration())
        let inspector = try HTTPInspector(
            configuration: InspectionConfiguration(budget: InspectionMemoryBudget(admission: admission)),
            sink: inspection
        )
        let breakpoints = ConsoleBreakpointSink(output: output)
        let limits = try HTTPControlLimits()
        let configuration = try HTTPControlConfiguration(rules: [
            HTTPRule(
                id: "pause-debug-path",
                match: HTTPRuleMatch(phase: .request, path: "/debug"),
                action: .breakpoint
            )
        ], limits: limits)
        let runtime = HTTPControlRuntime(configuration: configuration)
        let server = ProxyServer(
            certificateAuthority: authority,
            sink: ConsoleCaptureSink(output: output),
            upstreamPolicy: ProxyServer.UpstreamPolicy(
                additionalTrustRootsPEM: try arguments.originCAPath.map {
                    [try String(contentsOfFile: $0, encoding: .utf8)]
                } ?? []
            ),
            egressPolicy: EgressPolicy(allowInternal: arguments.allowInternal),
            ingress: .explicitConnect,
            observationSink: inspector,
            controlRuntime: runtime,
            controlEventSink: breakpoints,
            admission: admission
        )
        return LabServer(server: server, inspector: inspector, breakpoints: breakpoints, limits: limits)
    }

    private static func resolve(
        _ handle: BreakpointHandle,
        limits: HTTPControlLimits,
        editHeader: (name: String, value: String)?,
        output: ConsoleOutput
    ) async {
        output.emit("paused \(handle.original.method) \(handle.original.path.prefix(256)) [\(handle.phase)]")
        guard let editHeader else {
            _ = await handle.resolve(.forwardOriginal)
            return
        }
        guard !editHeader.name.isEmpty else {
            _ = await handle.resolve(.reject)
            return
        }
        do {
            let edit = try HTTPControlConfiguration(rules: [
                HTTPRule(
                    id: "console-header-edit",
                    match: HTTPRuleMatch(
                        phase: handle.original.phase,
                        authority: handle.original.authority,
                        method: handle.original.method,
                        path: handle.original.path
                    ),
                    action: .editHeaders(
                        remove: [editHeader.name],
                        append: [HTTPHeaderField(name: editHeader.name, value: editHeader.value)]
                    )
                )
            ], limits: limits)
            let decision = try edit.evaluate(handle.original)
            _ = await handle.resolve(.decision(decision))
        } catch {
            _ = await handle.resolve(.reject)
            output.emit("header edit rejected by HTTPControlConfiguration")
        }
    }

    private static func replay(
        _ arguments: Arguments,
        authority: CertificateAuthority,
        output: ConsoleOutput
    ) async throws {
        guard let rawURL = arguments.value, let url = URL(string: rawURL),
            let scheme = url.scheme, ["http", "https"].contains(scheme.lowercased()), url.host != nil
        else { throw LabError.invalidURL }
        let sink = ReplayConsoleSink(output: output)
        let capture = ReplayCaptureSink(output: output)
        let server = ProxyServer(
            certificateAuthority: authority,
            sink: capture,
            upstreamPolicy: ProxyServer.UpstreamPolicy(
                additionalTrustRootsPEM: try arguments.originCAPath.map {
                    [try String(contentsOfFile: $0, encoding: .utf8)]
                } ?? []
            ),
            egressPolicy: EgressPolicy(allowInternal: arguments.allowInternal)
        )
        let source = ReplaySource(requestID: UUID(), representation: .original)
        let request = ReplayRequest(
            source: source,
            destination: url,
            method: "GET",
            headers: [],
            body: [],
            bodyIsComplete: true
        )
        let result = try await performReplay(request, on: server, sink: sink)
        guard let response = await sink.responseHead, let requestHead = capture.requestHead,
            requestHead.id == result.requestID else { throw LabError.missingResponse }
        output.emit("replay status=\(result.status) bytes=\(result.responseBodyBytes) requestID=\(result.requestID)")
        if let path = arguments.exportPath {
            try await exportHAR(path, result: result, request: requestHead, response: response, output: output)
        }
    }

    private static func exportHAR(
        _ path: String,
        result: ReplayResult,
        request: CapturedRequestHead,
        response: CapturedResponseHead,
        output: ConsoleOutput
    ) async throws {
        guard let timings = result.timings else {
            throw LabError.missingHARTimings(result.timingUnavailable)
        }
        guard let contentBytes = result.responseContentBytes else {
            throw LabError.missingHARContentSize
        }
        let exchange = HARExchange(
            representation: .original,
            request: request,
            response: response,
            completed: true,
            timings: timings,
            responseContentBytes: contentBytes
        )
        let entries = AsyncStream<HARExchange>(bufferingPolicy: .bufferingOldest(1)) { continuation in
            continuation.yield(exchange)
            continuation.finish()
        }
        let writer = try FileHARWriter(path: path)
        try await HARExporter.export(
            entries,
            configuration: HARExportConfiguration(representation: .original),
            writer: writer
        )
        try await writer.close()
        output.emit("HAR 1.2 written to \(path); bodies omitted and credential headers redacted")
    }

    private static func performReplay(
        _ request: ReplayRequest,
        on server: ProxyServer,
        sink: any ReplayResponseSink
    ) async throws -> ReplayResult {
        do {
            _ = try await server.start(port: 0)
            let result = try await server.replay(request, responseSink: sink)
            try await server.stop()
            return result
        } catch {
            do { try await server.stop() } catch { writeError("Replay proxy cleanup failed.") }
            throw error
        }
    }
}

extension ReplayTimingUnavailable {
    var label: String {
        switch self {
        case .syntheticResponse: "syntheticResponse"
        case .incompleteUpload: "incompleteUpload"
        case .overlappingExchange: "overlappingExchange"
        }
    }
}
