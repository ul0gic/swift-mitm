import SwiftMITM

public enum HTTP3AdmissionScope: Sendable, Hashable {
    case local
    case shared
    case nativeListener
}

public enum HTTP3AdmissionResource: Sendable, Hashable {
    case connections
    case pendingHandshakes
    case requestStreams
    case retainedBytes
    case connectionRate
}

public struct HTTP3AdmissionFailure: Sendable, Hashable {
    public let scope: HTTP3AdmissionScope
    public let resource: HTTP3AdmissionResource

    public init(scope: HTTP3AdmissionScope, resource: HTTP3AdmissionResource) {
        self.scope = scope
        self.resource = resource
    }
}

public struct HTTP3ResourceUsage: Sendable {
    public let local: ResourceAdmissionUsage
    public let pendingHandshakes: Int
    public let nativeListenerConnections: Int
    public let nativeListenerPendingHandshakes: Int
    public let activeConnectionStorageLifetimes: Int
    public let nativeStreamReservedBytes: Int
    public let parserReservedBytes: Int
    public let forwardingReservedBytes: Int
    public let maximumNativeStreamReservationBytes: Int
    public let maximumParserReservationBytes: Int
    public let peakForwardedPayloadBytesPerStream: Int
    public let peakObservedRawReadCapacity: Int
    public let peakSampledNativePendingBytesPerStream: Int
    public let peakSampledNativeStorageTicketsPerStream: Int
    public let sampledNativeTrackedStreams: Int
    public let nativeOutputSampleComplete: Bool
    public let receivedGoaways: UInt64
    public let refusals: [HTTP3AdmissionFailure: UInt64]
}
