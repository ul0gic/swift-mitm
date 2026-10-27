import CryptoKit
import Foundation
import NIOConcurrencyHelpers
import SwiftMITM

final class HostObservation: HTTPObservationSink {
    private struct Body: Sendable {
        var hash = SHA256()
        var count = 0
        var unavailable = false
    }

    private struct State: Sendable {
        var bodies: [String: Body] = [:]
        var overflow = false
    }

    let original = HostCapture()
    let effective = HostCapture()
    private let state = NIOLockedValueBox(State())

    func receive(_ event: CaptureEvent) {
        original.receive(event)
    }

    func receive(_ event: CaptureEvent, representation: HTTPRepresentationKind) {
        switch representation {
        case .original: original.receive(event)
        case .effective: effective.receive(event)
        }
    }

    func receiveBody(_ observation: HTTPBodyObservation, bytes: UnsafeRawBufferPointer) {
        state.withLockedValue {
            let key = key(observation)
            guard $0.bodies[key] != nil || $0.bodies.count < 4096 else {
                $0.overflow = true
                return
            }
            var body = $0.bodies[key] ?? Body()
            body.hash.update(bufferPointer: bytes)
            body.count += bytes.count
            $0.bodies[key] = body
        }
    }

    func bodyUnavailable(_ observation: HTTPBodyObservation) {
        state.withLockedValue {
            let key = key(observation)
            guard $0.bodies[key] != nil || $0.bodies.count < 4096 else {
                $0.overflow = true
                return
            }
            var body = $0.bodies[key] ?? Body()
            body.unavailable = true
            $0.bodies[key] = body
        }
    }

    func write(prefix: String) throws {
        try original.write(to: prefix + ".original.json")
        try effective.write(to: prefix + ".effective.json")
        let bodies = try state.withLockedValue { state -> [String: [String: Any]] in
            guard !state.overflow else { throw HostError.captureOverflow }
            return state.bodies.mapValues { body in
                let digest = body.hash.finalize().map { String(format: "%02x", $0) }.joined()
                return ["byteCount": body.count, "sha256": digest, "unavailable": body.unavailable]
            }
        }
        try JSONSerialization.data(withJSONObject: bodies, options: [.sortedKeys])
            .write(to: URL(fileURLWithPath: prefix + ".bodies.json"), options: .atomic)
    }

    private func key(_ observation: HTTPBodyObservation) -> String {
        "\(observation.requestID.uuidString):\(observation.direction):\(observation.representation)"
    }
}
