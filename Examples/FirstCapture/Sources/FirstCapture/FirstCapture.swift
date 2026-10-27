import Darwin
import Dispatch
import Foundation
import SwiftMITM

enum CommandError: Error {
    case invalidArguments
}

struct Options {
    let certificatePath: String
    let keyPath: String
    let port: Int
    let captureBytes: Int
    let asynchronous: Bool
    let allowInternal: Bool
    let passThrough: [String]
    let upstreamCAPaths: [String]

    init(arguments: [String]) throws {
        var certificate: String?
        var key: String?
        var port = 8080
        var captureBytes = 0
        var asynchronous = false
        var allowInternal = false
        var passThrough: [String] = []
        var upstreamCAPaths: [String] = []
        var index = 0
        while index < arguments.count {
            let flag = arguments[index]
            index += 1
            if flag == "--async" {
                asynchronous = true
                continue
            }
            if flag == "--allow-internal" {
                allowInternal = true
                continue
            }
            let value = try Self.argument(arguments, at: index)
            index += 1
            switch flag {
            case "--ca-cert": certificate = value
            case "--ca-key": key = value
            case "--port":
                port = try Self.number(value, range: 0...65535)
            case "--capture-bytes":
                captureBytes = try Self.number(value, range: 0...Int.max)
            case "--passthrough": passThrough.append(value)
            case "--upstream-ca": upstreamCAPaths.append(value)
            default: throw CommandError.invalidArguments
            }
        }
        certificatePath = try Self.required(certificate)
        keyPath = try Self.required(key)
        self.port = port
        self.captureBytes = captureBytes
        self.asynchronous = asynchronous
        self.allowInternal = allowInternal
        self.passThrough = passThrough
        self.upstreamCAPaths = upstreamCAPaths
    }

    private static func number(_ value: String, range: ClosedRange<Int>) throws -> Int {
        guard let number = Int(value), range.contains(number) else { throw CommandError.invalidArguments }
        return number
    }

    private static func required(_ value: String?) throws -> String {
        guard let value else { throw CommandError.invalidArguments }
        return value
    }

    private static func argument(_ arguments: [String], at index: Int) throws -> String {
        guard index < arguments.count else { throw CommandError.invalidArguments }
        return arguments[index]
    }

    func makeServer(output: CaptureOutput) throws -> ProxyServer {
        let certificate = try String(contentsOfFile: certificatePath, encoding: .utf8)
        let key = try String(contentsOfFile: keyPath, encoding: .utf8)
        let authority = try CertificateAuthority(privateKeyPEM: key, certificatePEM: certificate)
        guard let interception = ProxyInterceptionPolicy(passThroughAuthorities: passThrough) else {
            throw CommandError.invalidArguments
        }
        let roots = try upstreamCAPaths.map { try String(contentsOfFile: $0, encoding: .utf8) }
        return ProxyServer(
            certificateAuthority: authority,
            sink: output,
            upstreamPolicy: .init(additionalTrustRootsPEM: roots),
            egressPolicy: .init(allowInternal: allowInternal),
            ingress: .explicitHTTP,
            captureBodyLimit: captureBytes,
            opaqueCaptureByteLimit: captureBytes,
            interceptionPolicy: interception
        )
    }
}

@main
struct FirstCapture {
    static func main() async {
        do {
            try await run(Options(arguments: Array(CommandLine.arguments.dropFirst())))
        } catch CommandError.invalidArguments {
            Console.error(
                "Usage: FirstCapture --ca-cert PATH --ca-key PATH [--port INT] "
                    + "[--passthrough AUTHORITY] [--capture-bytes INT] [--async] "
                    + "[--allow-internal] [--upstream-ca PATH]"
            )
            exit(2)
        } catch {
            Console.error("FirstCapture failed; check CA inputs, policy, and listener availability.")
            exit(1)
        }
    }

    private static func run(_ options: Options) async throws {
        let output = try CaptureOutput(asynchronous: options.asynchronous)
        let server: ProxyServer
        do {
            server = try options.makeServer(output: output)
        } catch {
            try output.asynchronousOutput?.restoreFlags()
            throw error
        }
        let signals = AsyncStream<Int32>.makeStream(bufferingPolicy: .bufferingNewest(1))
        let sources = stopSources(signals.continuation)
        defer {
            for source in sources { source.cancel() }
            signals.continuation.finish()
        }
        let consumer = Task { await output.consume() }
        var cancelDelivery = false
        do {
            let port = try await server.start(port: options.port)
            var listening = OutputRecord(type: "listening")
            listening.port = port
            output.emit(listening)
            for await number in signals.stream {
                cancelDelivery = number == SIGTERM
                if cancelDelivery, options.asynchronous {
                    output.finishDelivery(cancel: true)
                }
                break
            }
        } catch {
            do {
                try await server.stop()
            } catch {
                Console.error("FirstCapture cleanup failed.")
            }
            output.finishDelivery(cancel: true)
            await consumer.value
            try output.asynchronousOutput?.restoreFlags()
            throw error
        }
        do {
            try await server.stop()
        } catch {
            output.finishDelivery(cancel: true)
            await consumer.value
            try output.asynchronousOutput?.restoreFlags()
            throw error
        }
        output.finishDelivery(cancel: cancelDelivery && options.asynchronous)
        await consumer.value
        try await output.complete()
    }

    private static func stopSources(_ continuation: AsyncStream<Int32>.Continuation) -> [DispatchSourceSignal] {
        signal(SIGINT, SIG_IGN)
        signal(SIGTERM, SIG_IGN)
        return [SIGINT, SIGTERM].map { number in
            let source = DispatchSource.makeSignalSource(signal: number, queue: .global())
            source.setEventHandler { continuation.yield(number) }
            source.resume()
            return source
        }
    }
}
