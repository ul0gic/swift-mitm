import Foundation
import SwiftMITM

final class HTTPInspectionBody {
    let context: HTTPBodyObservation
    let codingChain: [ContentCoding]
    private let configuration: InspectionConfiguration
    private let deliver: (
        HTTPBodyObservation, Range<UInt64>, Range<UInt64>, [ContentCoding], InspectionEvent.Payload
    ) -> Void
    private var metadataReservation = 0
    private let decoders: [ContentDecoder]
    private var pending: [[UInt8]]
    private var positions: [Int]
    private var reservations: [Int]
    private var finished: [Bool]
    private var drainNeeded: [Bool]
    private var sse: SSEParser?
    private var grpc: GRPCStreamDecoder?
    private var grpcCarry: [UInt8] = []
    private var grpcPosition = 0
    private var grpcCharge = 0
    private var unaccountedWork = 0
    private var wireOffset: UInt64 = 0
    private var decodedOffset: UInt64 = 0
    private var eventStart: UInt64 = 0
    private var terminal = false
    private var ending = false
    private var trailers: [HTTPHeaderField] = []

    init(
        context: HTTPBodyObservation,
        headers: [HTTPHeaderField],
        path: String,
        status: Int?,
        configuration: InspectionConfiguration,
        deliver: @escaping (
            HTTPBodyObservation, Range<UInt64>, Range<UInt64>, [ContentCoding], InspectionEvent.Payload
        ) -> Void
    ) throws {
        self.context = context
        self.configuration = configuration
        self.deliver = deliver
        let chain = try Self.codings(headers)
        codingChain = chain
        decoders = try chain.reversed().map { try ContentDecoder(coding: $0, budget: configuration.budget) }
        pending = Array(repeating: [], count: decoders.count)
        positions = Array(repeating: 0, count: decoders.count)
        reservations = Array(repeating: 0, count: decoders.count)
        finished = Array(repeating: false, count: decoders.count)
        drainNeeded = Array(repeating: false, count: decoders.count)
        let charge = headers.reduce(path.utf8.count + 1024) { $0 + $1.name.utf8.count + $1.value.utf8.count }
        guard configuration.budget.reserve(charge) else { throw InspectionBudgetError.capacityExceeded }
        metadataReservation = charge
        try configureMedia(headers: headers, path: path, status: status)
        output(.head(headers))
    }

    private static func codings(_ headers: [HTTPHeaderField]) throws -> [ContentCoding] {
        let declared = headers.filter { $0.name.lowercased() == "content-encoding" }
            .flatMap { $0.value.split(separator: ",", omittingEmptySubsequences: false) }
            .map { $0.trimmingCharacters(in: .whitespaces).lowercased() }
        guard declared.count <= 2 else { throw BodyFailure.unsupportedCoding(declared.joined(separator: ",")) }
        var chain: [ContentCoding] = []
        for coding in declared {
            switch coding {
            case "gzip", "x-gzip": chain.append(.gzip)
            case "deflate": chain.append(.deflate)
            case "br": chain.append(.brotli)
            case "identity": break
            default: throw BodyFailure.unsupportedCoding(coding)
            }
        }
        return chain
    }

    private func configureMedia(headers: [HTTPHeaderField], path: String, status: Int?) throws {
        let media = headers.first { $0.name.lowercased() == "content-type" }?.value
            .split(separator: ";", maxSplits: 1).first?
            .trimmingCharacters(in: .whitespaces).lowercased() ?? ""
        if media == "text/event-stream" {
            sse = try SSEParser(
                maximumLineBytes: configuration.maximumLineBytes,
                maximumEventBytes: configuration.maximumEventBytes,
                budget: configuration.budget
            )
        }
        if let format = Self.grpcFormat(media) {
            let encoding = headers.first { $0.name.lowercased() == "grpc-encoding" }?.value ?? "identity"
            grpc = GRPCStreamDecoder(
                format: format,
                budget: configuration.budget,
                maximumMessageBytes: configuration.maximumMessageBytes,
                expectsStatus: context.direction == .response,
                initialMetadata: headers.map { ($0.name, $0.value) },
                httpStatus: status,
                compressionEncoding: encoding,
                emit: { [weak self] event in
                    guard let self else { return }
                    self.output(.grpc(event))
                    if case .message(let message) = event,
                        media == "application/grpc" || media.hasSuffix("+proto") {
                        self.inspectProtobuf(message.bytes, path: path)
                    }
                }
            )
        }
    }

    private static func grpcFormat(_ media: String) -> GRPCWireFormat? {
        if media == "application/grpc" || media.hasPrefix("application/grpc+") {
            return .native
        }
        if media == "application/grpc-web" || media.hasPrefix("application/grpc-web+") {
            return .webBinary
        }
        if media == "application/grpc-web-text" || media.hasPrefix("application/grpc-web-text+") {
            return .webText
        }
        return nil
    }

    deinit {
        configuration.budget.release(metadataReservation)
        for charge in reservations { configuration.budget.release(charge) }
        configuration.budget.release(grpcCharge)
    }

    func accept(_ bytes: [UInt8]) throws {
        guard !terminal else { return }
        wireOffset += UInt64(bytes.count)
        if decoders.isEmpty {
            try bytes.withUnsafeBytes { try consume($0) }
        } else {
            pending[0] = bytes
            positions[0] = 0
        }
    }

    func setTrailers(_ value: [HTTPHeaderField]) throws {
        let charge = value.reduce(0) { $0 + $1.name.utf8.count + $1.value.utf8.count }
        guard configuration.budget.reserve(charge) else { throw InspectionBudgetError.capacityExceeded }
        reservations.append(charge)
        trailers += value
        output(.trailers(value))
    }

    func end() { ending = true }

    func turn() throws -> Bool {
        guard !terminal else { return true }
        var work = 0
        while work < 1_048_576 {
            work += unaccountedWork
            unaccountedWork = 0
            if work >= 1_048_574 {
                return false
            }
            if let grpc, grpc.hasPendingWork || grpcPosition < grpcCarry.count {
                work += drainGRPC(grpc, allowance: min(65_536, 1_048_576 - work))
                continue
            }
            if let stage = pending.indices.reversed().first(where: {
                positions[$0] < pending[$0].count || drainNeeded[$0]
            }) {
                work += try feedStage(stage, allowance: min(65_536, (1_048_576 - work) / 3))
                continue
            }
            if !ending {
                return true
            }
            if let stage = finished.firstIndex(of: false) {
                work += try finishStage(stage, allowance: min(65_536, (1_048_576 - work) / 3))
                continue
            }
            if let sse, case .unfinishedRecord = sse.finish() {
                output(.diagnostic(.unfinishedSSERecord))
            }
            grpc?.finish(trailers: trailers.map { ($0.name, $0.value) })
            output(.end)
            terminal = true
            return true
        }
        return false
    }

    private func drainGRPC(_ grpc: GRPCStreamDecoder, allowance: Int) -> Int {
        let progress = grpcCarry.withUnsafeBytes { buffer in
            grpc.feedResumable(
                UnsafeRawBufferPointer(rebasing: buffer[grpcPosition...]),
                maximumOutputBytes: allowance
            )
        }
        grpcPosition += progress.consumedBytes
        if grpcPosition == grpcCarry.count {
            grpcCarry = []
            grpcPosition = 0
            configuration.budget.release(grpcCharge)
            grpcCharge = 0
        }
        return max(1, progress.producedBytes + progress.consumedBytes)
    }

    private func feedStage(_ stage: Int, allowance: Int) throws -> Int {
        let current = pending[stage]
        let progress = try current.withUnsafeBytes { buffer in
            try decoders[stage].feed(
                UnsafeRawBufferPointer(rebasing: buffer[positions[stage]...]),
                maximumOutputBytes: allowance
            ) { output in try self.produced(output, stage: stage) }
        }
        positions[stage] += progress.consumedBytes
        if case .outputLimit = progress.state {
            drainNeeded[stage] = true
        }
        else {
            drainNeeded[stage] = false
        }
        if positions[stage] == pending[stage].count {
            pending[stage] = []
            positions[stage] = 0
            configuration.budget.release(reservations[stage])
            reservations[stage] = 0
        }
        if progress.producedBytes == 0 && progress.consumedBytes == 0 && !current.isEmpty {
            throw ContentDecoderError.malformed
        }
        return progress.producedBytes + progress.consumedBytes
    }

    private func finishStage(_ stage: Int, allowance: Int) throws -> Int {
        let progress = try decoders[stage].finish(maximumOutputBytes: allowance) {
            try self.produced($0, stage: stage)
        }
        if case .complete = progress.state {
            finished[stage] = true
        }
        return max(1, progress.producedBytes)
    }

    func fail(_ diagnostic: InspectionDiagnostic) {
        guard !terminal else { return }
        terminal = true
        output(.diagnostic(diagnostic))
    }
}

private extension HTTPInspectionBody {
    func produced(_ bytes: UnsafeRawBufferPointer, stage: Int) throws {
        if stage + 1 == decoders.count {
            try consume(bytes)
        }
        else {
            let charge = bytes.count * 2
            guard configuration.budget.reserve(charge) else { throw InspectionBudgetError.capacityExceeded }
            pending[stage + 1].append(contentsOf: bytes)
            reservations[stage + 1] += charge
        }
    }

    func consume(_ bytes: UnsafeRawBufferPointer) throws {
        let charge = bytes.count * 2
        guard configuration.budget.reserve(charge) else { throw InspectionBudgetError.capacityExceeded }
        defer { configuration.budget.release(charge) }
        let start = decodedOffset
        decodedOffset += UInt64(bytes.count)
        unaccountedWork += bytes.count
        deliver(context, 0..<wireOffset, start..<decodedOffset, codingChain, .decodedBody(Array(bytes)))
        try sse?.feed(bytes) { event in
            self.output(.sse(event))
            self.eventStart = self.decodedOffset
        }
        if let grpc {
            if grpc.hasPendingWork || grpcPosition < grpcCarry.count {
                let carryCharge = bytes.count * 2
                guard configuration.budget.reserve(carryCharge) else { throw InspectionBudgetError.capacityExceeded }
                grpcCarry.append(contentsOf: bytes)
                grpcCharge += carryCharge
                return
            }
            let progress = grpc.feedResumable(bytes, maximumOutputBytes: 65_536)
            unaccountedWork += progress.producedBytes
            if progress.consumedBytes < bytes.count {
                let remainder = UnsafeRawBufferPointer(rebasing: bytes[progress.consumedBytes...])
                let charge = remainder.count * 2
                guard configuration.budget.reserve(charge) else { throw InspectionBudgetError.capacityExceeded }
                grpcCarry.append(contentsOf: remainder)
                grpcCharge += charge
            }
        }
    }

    func output(_ payload: InspectionEvent.Payload) {
        deliver(context, 0..<wireOffset, eventStart..<decodedOffset, codingChain, payload)
    }

    func inspectProtobuf(_ bytes: [UInt8], path: String) {
        guard let schema = configuration.schema,
            let name = schema.messageName(methodPath: path, response: context.direction == .response)
        else {
            output(.diagnostic(.schemaUnavailable))
            return
        }
        do {
            let fields = try schema.decode(bytes, messageName: name)
            output(.protobuf(messageName: name, schemaDigest: schema.digest, fields: fields))
        } catch let error as ProtobufInspectionError {
            output(.diagnostic(.schemaFailure(error)))
        } catch {
            output(.diagnostic(.schemaFailure(.malformedWire)))
        }
    }
}

enum BodyFailure: Error {
    case unsupportedCoding(String)
}
