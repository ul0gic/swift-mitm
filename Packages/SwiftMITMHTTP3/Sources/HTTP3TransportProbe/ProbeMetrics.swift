import Foundation
import NIOCore
import NIOHTTP3
import NIOQUIC
import os
import SwiftMITM

enum ProbeStorageKind: Sendable {
    case parser
    case nativeStream
    case queuedCopy
}

final class ProbeRawReadMeter: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    private let metrics: ProbeMetrics

    init(metrics: ProbeMetrics) { self.metrics = metrics }

    func handlerAdded(context: ChannelHandlerContext) { metrics.track(context.channel) }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        metrics.observeRawCapacity(unwrapInboundIn(data).capacity)
        context.fireChannelRead(data)
    }
}

final class ProbeStorageLease: HTTP3InboundStorageReservation {
    let bytes: Int
    private let kind: ProbeStorageKind
    private let metrics: ProbeMetrics

    init(bytes: Int, kind: ProbeStorageKind, metrics: ProbeMetrics) {
        self.bytes = bytes
        self.kind = kind
        self.metrics = metrics
    }

    deinit { metrics.release(bytes, kind: kind) }
}

final class ProbeMetrics: Sendable {
    struct Snapshot: Encodable, Sendable {
        let version = 1
        var phase = "running"
        var sharedRetainedBytes = 0
        var peakSharedRetainedBytes = 0
        var storageAdmissionRefusals = 0
        var parserReservedBytes = 0
        var nativeStreamReservedBytes = 0
        var queuedCopyBytes = 0
        var peakQueuedCopyBytesPerStream = 0
        var peakForwardedPayloadBytesPerStream = 0
        var peakObservedRawReadCapacity = 0
        var sampledNativePendingBytes = 0
        var sampledNativeRetainedBytes = 0
        var peakSampledNativePendingBytesPerStream = 0
        var sampledNativeStorageTickets = 0
        var peakSampledNativeStorageTicketsPerStream = 0
        var sampledNativeTrackedStreams = 0
    }

    private let state = OSAllocatedUnfairLock(initialState: Snapshot())
    private let admission: ResourceAdmissionController
    private let account: ResourceRetainedByteAccount
    private let channels = OSAllocatedUnfairLock(initialState: [ObjectIdentifier: Channel]())

    init(admission: ResourceAdmissionController) {
        self.admission = admission
        account = admission.makeRetainedByteAccount()
    }

    func reserve(_ bytes: Int, kind: ProbeStorageKind) throws -> ProbeStorageLease {
        try state.withLock { state in
            guard account.reserve(bytes) else {
                state.storageAdmissionRefusals += 1
                throw ProbeError.capacity
            }
            switch kind {
            case .parser: state.parserReservedBytes += bytes
            case .nativeStream: state.nativeStreamReservedBytes += bytes
            case .queuedCopy: state.queuedCopyBytes += bytes
            }
            state.sharedRetainedBytes = admission.usage.retainedBytes
            state.peakSharedRetainedBytes = max(state.peakSharedRetainedBytes, state.sharedRetainedBytes)
        }
        return ProbeStorageLease(bytes: bytes, kind: kind, metrics: self)
    }

    fileprivate func release(_ bytes: Int, kind: ProbeStorageKind) {
        state.withLock { state in
            account.release(bytes)
            switch kind {
            case .parser: state.parserReservedBytes -= bytes
            case .nativeStream: state.nativeStreamReservedBytes -= bytes
            case .queuedCopy: state.queuedCopyBytes -= bytes
            }
            state.sharedRetainedBytes = admission.usage.retainedBytes
        }
    }

    func observeQueuedBytes(_ bytes: Int, payload: Int) {
        state.withLock {
            $0.peakQueuedCopyBytesPerStream = max($0.peakQueuedCopyBytesPerStream, bytes)
            $0.peakForwardedPayloadBytesPerStream = max($0.peakForwardedPayloadBytesPerStream, payload)
        }
    }

    func observeRawCapacity(_ bytes: Int) {
        state.withLock { $0.peakObservedRawReadCapacity = max($0.peakObservedRawReadCapacity, bytes) }
    }

    func track(_ channel: Channel) {
        let id = ObjectIdentifier(channel)
        channels.withLock { $0[id] = channel }
        channel.getOption(QUICWriteStorageReleasedFutureOption()).flatMap(\.self).whenComplete { _ in
            _ = self.channels.withLock { $0.removeValue(forKey: id) }
        }
    }

    func sampleNativeOutput() async throws {
        let tracked = channels.withLock { Array($0.values) }
        var pending = 0
        var retained = 0
        var peak = 0
        var tickets = 0
        var peakTickets = 0
        for channel in tracked {
            let available = try await channel.getOption(QUICPendingWriteBytesAvailableOption()).get()
            let bytes = 65_536 - available
            pending += bytes
            peak = max(peak, bytes)
            retained += try await channel.getOption(QUICNativeWriteBytesRetainedOption()).get()
            let streamTickets = try await channel.getOption(QUICNativeWriteStorageTicketsRetainedOption()).get()
            tickets += streamTickets
            peakTickets = max(peakTickets, streamTickets)
        }
        state.withLock { [pending, retained, peak, tickets, peakTickets] in
            $0.sampledNativePendingBytes = pending
            $0.sampledNativeRetainedBytes = retained
            $0.peakSampledNativePendingBytesPerStream = max($0.peakSampledNativePendingBytesPerStream, peak)
            $0.sampledNativeStorageTickets = tickets
            $0.peakSampledNativeStorageTicketsPerStream = max($0.peakSampledNativeStorageTicketsPerStream, peakTickets)
            $0.sampledNativeTrackedStreams = tracked.count
        }
    }

    func write(to path: String, stopped: Bool = false) throws {
        var snapshot = state.withLock { $0 }
        snapshot.phase = stopped ? "stopped" : "running"
        snapshot.sharedRetainedBytes = admission.usage.retainedBytes
        if stopped, channels.withLock({ $0.isEmpty }) {
            snapshot.sampledNativePendingBytes = 0
            snapshot.sampledNativeRetainedBytes = 0
            snapshot.sampledNativeStorageTickets = 0
            snapshot.sampledNativeTrackedStreams = 0
        }
        let data = try JSONEncoder().encode(snapshot)
        try data.write(to: URL(fileURLWithPath: path), options: .atomic)
    }
}
