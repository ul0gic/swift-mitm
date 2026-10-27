import NIOCore
import NIOQUIC
import os
import SwiftMITM

final class HTTP3ResourceTracker: Sendable {
    enum CountResource {
        case connections
        case requestStreams

        var resource: HTTP3AdmissionResource {
            switch self {
            case .connections: .connections
            case .requestStreams: .requestStreams
            }
        }
    }
    struct State {
        var pendingHandshakes = 0
        var activeConnectionStorageLifetimes = 0
        var nativeStreamReservedBytes = 0
        var parserReservedBytes = 0
        var forwardingReservedBytes = 0
        var captureReservedBytes = 0
        var maximumNativeStreamReservationBytes = 0
        var maximumParserReservationBytes = 0
        var peakForwardedPayloadBytesPerStream = 0
        var peakObservedRawReadCapacity = 0
        var peakSampledNativePendingBytesPerStream = 0
        var peakSampledNativeStorageTicketsPerStream = 0
        var sampledNativeTrackedStreams = 0
        var nativeOutputSampleComplete = true
        var nativeListenerConnections = 0
        var nativeListenerPendingHandshakes = 0
        var nativeDrops: [HTTP3AdmissionFailure: UInt64] = [:]
        var completedNativeDrops: [HTTP3AdmissionFailure: UInt64] = [:]
        var receivedGoaways: UInt64 = 0
        var refusals: [HTTP3AdmissionFailure: UInt64] = [:]
    }

    let state = OSAllocatedUnfairLock(initialState: State())
    let channels = OSAllocatedUnfairLock(initialState: [ObjectIdentifier: any Channel]())
    let local: ResourceAdmissionController
    let shared: ResourceAdmissionController?
    private let localBytes: ResourceRetainedByteAccount
    private let sharedBytes: ResourceRetainedByteAccount?

    init(local: ResourceAdmissionController, shared: ResourceAdmissionController?) {
        self.local = local
        self.shared = shared === local ? nil : shared
        localBytes = local.makeRetainedByteAccount()
        sharedBytes = shared === local ? nil : shared?.makeRetainedByteAccount()
    }

    func reserve(_ bytes: Int, kind: HTTP3StorageKind) throws -> HTTP3StorageLease {
        try state.withLock { value in
            switch kind {
            case .nativeStream:
                value.maximumNativeStreamReservationBytes = max(value.maximumNativeStreamReservationBytes, bytes)
            case .parser:
                value.maximumParserReservationBytes = max(value.maximumParserReservationBytes, bytes)
            case .forwarding, .capture: break
            }
            guard localBytes.reserve(bytes) else {
                Self.increment(&value.refusals, scope: .local, resource: .retainedBytes)
                throw HTTP3ProxyError.resourceLimit
            }
            if let sharedBytes, !sharedBytes.reserve(bytes) {
                localBytes.release(bytes)
                Self.increment(&value.refusals, scope: .shared, resource: .retainedBytes)
                throw HTTP3ProxyError.resourceLimit
            }
            switch kind {
            case .nativeStream: value.nativeStreamReservedBytes += bytes
            case .parser: value.parserReservedBytes += bytes
            case .forwarding: value.forwardingReservedBytes += bytes
            case .capture: value.captureReservedBytes += bytes
            }
        }
        return HTTP3StorageLease(bytes: bytes, kind: kind, tracker: self)
    }

    func release(_ bytes: Int, kind: HTTP3StorageKind) {
        state.withLock { value in
            localBytes.release(bytes)
            sharedBytes?.release(bytes)
            switch kind {
            case .nativeStream: value.nativeStreamReservedBytes -= bytes
            case .parser: value.parserReservedBytes -= bytes
            case .forwarding: value.forwardingReservedBytes -= bytes
            case .capture: value.captureReservedBytes -= bytes
            }
        }
    }

    func acquire(_ resource: CountResource, destination: String? = nil) throws -> [ResourceReservation] {
        let reservation: ResourceReservation
        do { reservation = try acquire(resource, from: local, destination: destination) } catch {
            refuse(.local, resource.resource)
            throw HTTP3ProxyError.resourceLimit
        }
        guard let shared else { return [reservation] }
        do { return [reservation, try acquire(resource, from: shared, destination: destination)] } catch {
            reservation.release()
            refuse(.shared, resource.resource)
            throw HTTP3ProxyError.resourceLimit
        }
    }

    private func acquire(
        _ resource: CountResource,
        from controller: ResourceAdmissionController,
        destination: String?
    ) throws -> ResourceReservation {
        switch resource {
        case .connections: return try controller.acquireConnection(destination: destination)
        case .requestStreams: return try controller.acquireStream()
        }
    }

    func beginHandshake(maximum: Int) throws {
        try state.withLock {
            guard $0.pendingHandshakes < maximum else {
                Self.increment(&$0.refusals, scope: .local, resource: .pendingHandshakes)
                throw HTTP3ProxyError.resourceLimit
            }
            $0.pendingHandshakes += 1
        }
    }

    func endHandshake() { state.withLock { $0.pendingHandshakes -= 1 } }
    func storageStarted() { state.withLock { $0.activeConnectionStorageLifetimes += 1 } }
    func storageReleased() { state.withLock { $0.activeConnectionStorageLifetimes -= 1 } }

    func observeForwardingPayload(_ pendingBytes: Int) {
        state
            .withLock {
                $0.peakForwardedPayloadBytesPerStream = max($0.peakForwardedPayloadBytesPerStream, pendingBytes)
            }
    }

    func observeRawCapacity(_ bytes: Int) {
        state.withLock { $0.peakObservedRawReadCapacity = max($0.peakObservedRawReadCapacity, bytes) }
    }

    func observeGoaway() {
        state.withLock {
            if $0.receivedGoaways < .max {
                $0.receivedGoaways += 1
            }
        }
    }

    func refuse(_ scope: HTTP3AdmissionScope, _ resource: HTTP3AdmissionResource) {
        state.withLock { Self.increment(&$0.refusals, scope: scope, resource: resource) }
    }

    static func increment(
        _ counts: inout [HTTP3AdmissionFailure: UInt64],
        scope: HTTP3AdmissionScope,
        resource: HTTP3AdmissionResource
    ) {
        let key = HTTP3AdmissionFailure(scope: scope, resource: resource)
        let count = counts[key, default: 0]
        if count < .max {
            counts[key] = count + 1
        }
    }

    var storageIsQuiescent: Bool {
        state.withLock {
            $0.activeConnectionStorageLifetimes == 0 && $0.nativeStreamReservedBytes == 0
                && $0.parserReservedBytes == 0 && $0.forwardingReservedBytes == 0 && $0.captureReservedBytes == 0
        }
    }
}
