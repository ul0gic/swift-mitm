import Foundation
import SwiftMITM

struct HostEmbedding: Sendable {
    let runtime: HTTPControlRuntime?
    let controlEvents: HostControlEvents?
    let observation: HostObservation?
    let shared: ResourceAdmissionController?
    let commands: HostControlCommands?
    let prefix: String

    init(_ config: HostConfiguration) throws {
        prefix = config.readyPath
        if let preset = config.controlPreset {
            runtime = HTTPControlRuntime(configuration: try HostControlPreset.configuration(
                preset,
                mappedAuthority: config.mappedAuthority
            ))
            let events = HostControlEvents(blockCallback: preset == "orphan")
            controlEvents = events
            commands = HostControlCommands(events: events, prefix: config.readyPath)
        } else {
            runtime = nil
            controlEvents = nil
            commands = nil
        }
        observation = config.observe == true ? HostObservation() : nil
        if let bytes = config.sharedRetainedBytes {
            shared = ResourceAdmissionController(configuration: try ResourceAdmissionConfiguration(
                maximumRetainedBytes: bytes
            ))
        } else {
            shared = nil
        }
    }

    func write(stopReport: HTTPControlStopReport? = nil) async throws {
        var value: [String: Any] = [:]
        if let runtime {
            let usage = await runtime.resourceUsage()
            value["control"] = [
                "pausedExchanges": usage.pausedExchanges,
                "callbackTasks": usage.callbackTasks,
                "orphanedCallbacks": usage.orphanedCallbacks,
                "retainedBytes": usage.retainedBytes,
                "taskSlots": usage.taskSlots
            ]
        }
        if let shared {
            let usage = shared.usage
            value["shared"] = [
                "connections": usage.connections,
                "streams": usage.streams,
                "retainedBytes": usage.retainedBytes,
                "replayAttempts": usage.replayAttempts
            ]
        }
        if let stopReport {
            value["stopReport"] = [
                "abandonedCallbacks": stopReport.abandonedCallbacks,
                "retainedCallbackBytes": stopReport.retainedCallbackBytes
            ]
        }
        try JSONSerialization.data(withJSONObject: value, options: [.sortedKeys])
            .write(to: URL(fileURLWithPath: prefix + ".embedding.json"), options: .atomic)
        try controlEvents?.write(to: prefix + ".control-events.json")
        try observation?.write(prefix: prefix)
    }
}
