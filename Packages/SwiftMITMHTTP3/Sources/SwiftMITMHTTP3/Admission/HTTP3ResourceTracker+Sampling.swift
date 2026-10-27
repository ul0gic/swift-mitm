import NIOCore
import NIOQUIC

extension HTTP3ResourceTracker {
    func track(_ channel: any Channel) throws {
        guard let options = channel.syncOptions else { throw HTTP3ProxyError.protocolViolation }
        let released = try options.getOption(QUICWriteStorageReleasedFutureOption())
        let id = ObjectIdentifier(channel)
        channels.withLock { $0[id] = channel }
        released.whenSuccess { _ = self.channels.withLock { $0.removeValue(forKey: id) } }
    }

    func sampleNativeOutput() {
        let tracked = channels.withLock { Array($0.values) }
        var peakBytes = 0
        var peakTickets = 0
        var complete = true
        for channel in tracked {
            do {
                guard let options = channel.syncOptions else { throw HTTP3ProxyError.protocolViolation }
                let available = try options.getOption(QUICPendingWriteBytesAvailableOption())
                let tickets = try options.getOption(QUICNativeWriteStorageTicketsRetainedOption())
                peakBytes = max(peakBytes, 65_536 - available)
                peakTickets = max(peakTickets, tickets)
            } catch { complete = false }
        }
        state.withLock { [peakBytes, peakTickets, complete] in
            $0.peakSampledNativePendingBytesPerStream = max($0.peakSampledNativePendingBytesPerStream, peakBytes)
            $0.peakSampledNativeStorageTicketsPerStream = max($0.peakSampledNativeStorageTicketsPerStream, peakTickets)
            $0.sampledNativeTrackedStreams = tracked.count
            $0.nativeOutputSampleComplete = complete
        }
    }

    func observeListener(_ snapshot: QUICConnectionAdmissionSnapshot) {
        state.withLock {
            $0.nativeListenerConnections = snapshot.activeConnections
            $0.nativeListenerPendingHandshakes = snapshot.pendingHandshakes
            $0.nativeDrops = [
                .init(scope: .nativeListener, resource: .connections): snapshot.droppedForConnectionLimit,
                .init(scope: .nativeListener, resource: .pendingHandshakes): snapshot.droppedForHandshakeLimit,
                .init(scope: .nativeListener, resource: .connectionRate): snapshot.droppedForRateLimit
            ]
        }
    }

    func completeListenerIncarnation() {
        state.withLock {
            for (key, count) in $0.nativeDrops {
                $0.completedNativeDrops[key] = Self.add($0.completedNativeDrops[key, default: 0], count)
            }
            $0.nativeDrops.removeAll()
        }
    }

    func finalizeNativeOccupancy() {
        state.withLock {
            // Disposal proves current occupancy, not completeness of an earlier explicit sample.
            $0.nativeListenerConnections = 0
            $0.nativeListenerPendingHandshakes = 0
            $0.sampledNativeTrackedStreams = 0
        }
    }

    private static func add(_ first: UInt64, _ second: UInt64) -> UInt64 {
        let (value, overflow) = first.addingReportingOverflow(second)
        return overflow ? .max : value
    }

    func snapshot() -> HTTP3ResourceUsage {
        state.withLock { value in
            var refusals = value.refusals
            for counts in [value.completedNativeDrops, value.nativeDrops] {
                for (key, count) in counts { refusals[key] = Self.add(refusals[key, default: 0], count) }
            }
            return HTTP3ResourceUsage(
                local: local.usage,
                pendingHandshakes: value.pendingHandshakes,
                nativeListenerConnections: value.nativeListenerConnections,
                nativeListenerPendingHandshakes: value.nativeListenerPendingHandshakes,
                activeConnectionStorageLifetimes: value.activeConnectionStorageLifetimes,
                nativeStreamReservedBytes: value.nativeStreamReservedBytes,
                parserReservedBytes: value.parserReservedBytes,
                forwardingReservedBytes: value.forwardingReservedBytes,
                maximumNativeStreamReservationBytes: value.maximumNativeStreamReservationBytes,
                maximumParserReservationBytes: value.maximumParserReservationBytes,
                peakForwardedPayloadBytesPerStream: value.peakForwardedPayloadBytesPerStream,
                peakObservedRawReadCapacity: value.peakObservedRawReadCapacity,
                peakSampledNativePendingBytesPerStream: value.peakSampledNativePendingBytesPerStream,
                peakSampledNativeStorageTicketsPerStream: value.peakSampledNativeStorageTicketsPerStream,
                sampledNativeTrackedStreams: value.sampledNativeTrackedStreams,
                nativeOutputSampleComplete: value.nativeOutputSampleComplete,
                receivedGoaways: value.receivedGoaways,
                refusals: refusals
            )
        }
    }
}

final class HTTP3RawReadMeter: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    private let resources: HTTP3ResourceTracker

    init(resources: HTTP3ResourceTracker) { self.resources = resources }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        resources.observeRawCapacity(unwrapInboundIn(data).capacity)
        context.fireChannelRead(data)
    }
}
