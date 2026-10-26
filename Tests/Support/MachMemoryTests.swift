import Darwin
import XCTest

final class MachMemoryTests: XCTestCase {
    func testSamplingFailureIsDistinctFromAValidZeroMeasurement() throws {
        XCTAssertEqual(try MachMemory.residentBytes(result: KERN_SUCCESS, sampledBytes: 0), 0)
        XCTAssertThrowsError(try MachMemory.residentBytes(result: KERN_FAILURE, sampledBytes: 0)) { error in
            XCTAssertEqual(error as? MachMemoryError, .samplingUnavailable(KERN_FAILURE))
            XCTAssertEqual(
                (error as? LocalizedError)?.errorDescription,
                "RSS sampling unavailable: task_info returned \(KERN_FAILURE)"
            )
        }
    }

    func testRSSGrowthCannotUnderflowWhenTheLaterSampleIsLower() {
        XCTAssertEqual(MachMemory.growth(from: 10, to: 9), 0)
        XCTAssertEqual(MachMemory.growth(from: 10, to: 10), 0)
        XCTAssertEqual(MachMemory.growth(from: 10, to: 11), 1)
        XCTAssertEqual(MachMemory.growth(from: 0, to: .max), .max)
    }
}
