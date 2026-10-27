import Darwin
import Dispatch

struct ProcessSnapshot: Codable, Sendable {
    let startedNanoseconds: UInt64
    let endedNanoseconds: UInt64
    let errorNumber: Int32?
    let userSeconds: Int?
    let userMicroseconds: Int32?
    let systemSeconds: Int?
    let systemMicroseconds: Int32?
    let voluntaryContextSwitches: Int?
    let involuntaryContextSwitches: Int?

    static func take() -> Self {
        var usage = rusage()
        let started = DispatchTime.now().uptimeNanoseconds
        let status = getrusage(RUSAGE_SELF, &usage)
        let failure = status == 0 ? nil : errno
        let ended = DispatchTime.now().uptimeNanoseconds
        return Self(
            startedNanoseconds: started,
            endedNanoseconds: ended,
            errorNumber: failure,
            userSeconds: status == 0 ? usage.ru_utime.tv_sec : nil,
            userMicroseconds: status == 0 ? usage.ru_utime.tv_usec : nil,
            systemSeconds: status == 0 ? usage.ru_stime.tv_sec : nil,
            systemMicroseconds: status == 0 ? usage.ru_stime.tv_usec : nil,
            voluntaryContextSwitches: status == 0 ? usage.ru_nvcsw : nil,
            involuntaryContextSwitches: status == 0 ? usage.ru_nivcsw : nil
        )
    }
}

struct ResponseCPUObservation: Codable, Sendable {
    let path: String
    let before: ProcessSnapshot
    let after: ProcessSnapshot
}

struct ProcessObservations: Codable, Sendable {
    let workload: String
    let count: Int
    var phaseBefore: ProcessSnapshot?
    var phaseAfter: ProcessSnapshot?
    var responses: [ResponseCPUObservation] = []

    func snapshotBeforeRequest(index: Int) -> ProcessSnapshot? {
        guard index > 0, index == 1 || workload == "throughput" else { return nil }
        return ProcessSnapshot.take()
    }

    func snapshotAfterRequest(index: Int) -> ProcessSnapshot? {
        guard index > 0, index == count || workload == "throughput" else { return nil }
        return ProcessSnapshot.take()
    }

    mutating func record(index: Int, path: String, before: ProcessSnapshot?, after: ProcessSnapshot?) {
        if index == 1 {
            phaseBefore = before
        }
        if index == count {
            phaseAfter = after
        }
        if let before, let after {
            responses.append(ResponseCPUObservation(path: path, before: before, after: after))
        }
    }
}

struct ClientMeasurements: Sendable {
    let exchanges: [ExchangeMeasurement]
    let processObservations: ProcessObservations?
}
