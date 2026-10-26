import Darwin
import Foundation
import os

final class ConsoleOutput: Sendable {
    private let delivery = BoundedDelivery()
    private let stdout: AsyncStdout
    private let lost = OSAllocatedUnfairLock(initialState: false)

    init() throws { stdout = try AsyncStdout() }

    func emit(_ value: String) {
        let data = Data(value.utf8)
        guard data.count <= BoundedDelivery.maximumBytes else {
            reportLoss(.overflow)
            return
        }
        delivery.offer(data)
        if let failure = delivery.failure {
            reportLoss(failure)
        }
    }

    func consume() async {
        do {
            while let data = try await delivery.next() { try await stdout.line(data) }
        } catch let failure as DeliveryError {
            reportLoss(failure)
        } catch {
            reportLoss(.outputFailed)
        }
    }

    func finish(cancel: Bool) {
        if cancel {
            reportLoss(.cancelled)
        } else {
            delivery.finish()
            stdout.beginDrain()
        }
    }

    func complete() async throws {
        if delivery.failure == nil {
            do {
                try await stdout.line(Data("{\"type\":\"stopped\"}".utf8))
            } catch let failure as DeliveryError {
                reportLoss(failure)
            } catch {
                reportLoss(.outputFailed)
            }
        }
        try stdout.restoreFlags()
    }

    func restoreFlags() throws { try stdout.restoreFlags() }

    private func reportLoss(_ failure: DeliveryError) {
        delivery.fail(failure)
        let first = lost.withLock { value in
            guard !value else { return false }
            value = true
            return true
        }
        if first {
            stdout.reportLoss(delivery.failure ?? failure)
        }
    }
}
