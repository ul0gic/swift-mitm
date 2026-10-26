import Foundation
import XCTest

final class Stage2ConsumerCompatibilityTests: XCTestCase {
    func testExternalInterceptionLabShowsSixPublicWorkflowsAndDrainsOnStop() throws {
        guard let binary = ProcessInfo.processInfo.environment["SWIFTMITM_INTERCEPTION_LAB"] else {
            throw Release022ClientError.missingEnvironment("SWIFTMITM_INTERCEPTION_LAB")
        }
        guard let python = ProcessInfo.processInfo.environment["SWIFTMITM_CLIENT_PYTHON"] else {
            throw Release022ClientError.missingEnvironment("SWIFTMITM_CLIENT_PYTHON")
        }
        let script = Release022ExternalPeer.clientsDirectory.appendingPathComponent("Stage2/lab_acceptance.py")
        let output = try release022RunProcess(executable: python, arguments: [script.path, "--binary", binary])
        let report = try JSONDecoder().decode([String: Bool].self, from: Data(output.utf8))
        XCTAssertEqual(report, [
            "sse": true, "grpc": true, "websocket": true, "pausedEdit": true,
            "replay": true, "redactedHAR": true, "gracefulStop": true
        ])
    }
}
