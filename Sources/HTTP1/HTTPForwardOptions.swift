import Foundation
import NIOCore

struct HTTPForwardOptions: Sendable {
    var correlationPolicy = HTTP1CorrelationPolicy.default
    var onResourceLimit: @Sendable () -> Void = {}
    var controlRuntime: HTTPControlRuntime?
    var controlEventSink: (any HTTPControlEventSink)?
    var admission: ResourceAdmissionController?
    var fixedDestination: HTTPForwardTarget?
    var typedHTTP2 = false
    var externallyCaptured = false
    var requestID: UUID?
    var reusePool: HTTP1ReusePool?
    var authorizeReuse: @Sendable (HTTPForwardTarget, Channel) -> Bool = { _, _ in false }
}
