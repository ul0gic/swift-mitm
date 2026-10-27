import Dispatch
import Foundation
import SwiftMITMHTTP3

enum HostResources {
    static func write(server: HTTP3ProxyServer, phase: String, path: String) async throws {
        let before = DispatchTime.now().uptimeNanoseconds
        let usage = await server.resourceUsage()
        let after = DispatchTime.now().uptimeNanoseconds
        let value: [String: Any] = [
            "schema": "SwiftMITMHTTP3.public.resourceUsage.v1",
            "phase": phase,
            "sampleStartedNanoseconds": before,
            "sampleEndedNanoseconds": after,
            "local": [
                "connections": usage.local.connections, "streams": usage.local.streams,
                "retainedBytes": usage.local.retainedBytes, "replayAttempts": usage.local.replayAttempts
            ],
            "pendingHandshakes": usage.pendingHandshakes,
            "nativeListenerConnections": usage.nativeListenerConnections,
            "nativeListenerPendingHandshakes": usage.nativeListenerPendingHandshakes,
            "activeConnectionStorageLifetimes": usage.activeConnectionStorageLifetimes,
            "nativeStreamReservedBytes": usage.nativeStreamReservedBytes,
            "parserReservedBytes": usage.parserReservedBytes,
            "forwardingReservedBytes": usage.forwardingReservedBytes,
            "maximumNativeStreamReservationBytes": usage.maximumNativeStreamReservationBytes,
            "maximumParserReservationBytes": usage.maximumParserReservationBytes,
            "peakForwardedPayloadBytesPerStream": usage.peakForwardedPayloadBytesPerStream,
            "peakObservedRawReadCapacity": usage.peakObservedRawReadCapacity,
            "peakSampledNativePendingBytesPerStream": usage.peakSampledNativePendingBytesPerStream,
            "peakSampledNativeStorageTicketsPerStream": usage.peakSampledNativeStorageTicketsPerStream,
            "sampledNativeTrackedStreams": usage.sampledNativeTrackedStreams,
            "nativeOutputSampleComplete": usage.nativeOutputSampleComplete,
            "receivedGoaways": usage.receivedGoaways,
            "refusals": usage.refusals.map { failure, count in
                ["scope": scope(failure.scope), "resource": resource(failure.resource), "count": count] as [String: Any]
            }
        ]
        try JSONSerialization.data(withJSONObject: value, options: [.sortedKeys])
            .write(to: URL(fileURLWithPath: path), options: .atomic)
    }

    private static func scope(_ value: HTTP3AdmissionScope) -> String {
        switch value {
        case .local: "local"
        case .shared: "shared"
        case .nativeListener: "nativeListener"
        }
    }

    private static func resource(_ value: HTTP3AdmissionResource) -> String {
        switch value {
        case .connections: "connections"
        case .pendingHandshakes: "pendingHandshakes"
        case .requestStreams: "requestStreams"
        case .retainedBytes: "retainedBytes"
        case .connectionRate: "connectionRate"
        }
    }
}
