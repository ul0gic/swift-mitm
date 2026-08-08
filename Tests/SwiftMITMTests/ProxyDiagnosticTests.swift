import Foundation
import NIOCore
import NIOPosix
import XCTest

@testable import SwiftMITM

final class ProxyDiagnosticTests: XCTestCase {
    private func runTransfer(bodySize: Int, watchdog: Int64) throws -> Int {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try? group.syncShutdownGracefully() }

        let origin = TestOriginServer(group: group, bodySize: bodySize)
        try origin.start()
        defer { origin.stop() }

        let sink = CountingSink()
        let proxy = HTTP2ProxyBridge(
            group: group,
            upstream: .init(host: "127.0.0.1", port: origin.localPort),
            sink: sink
        )
        let proxyChannel = try proxy.start().wait()
        defer { try? proxyChannel.close().wait() }
        let proxyPort = try XCTUnwrap(proxyChannel.localAddress?.port)

        let client = TestHTTP2Client(group: group)
        defer { client.shutdown() }
        client.armWatchdog(seconds: watchdog)
        try client.connectAndRequest(
            host: "127.0.0.1",
            port: proxyPort,
            authority: "127.0.0.1:\(origin.localPort)"
        )
        return try client.completion.wait()
    }

    func testTiny1KB() throws {
        XCTAssertEqual(try runTransfer(bodySize: 1024, watchdog: 8), 1024)
    }

    func testTwoWindows128KB() throws {
        XCTAssertEqual(try runTransfer(bodySize: 128 * 1024, watchdog: 8), 128 * 1024)
    }

    func testOneMB() throws {
        XCTAssertEqual(try runTransfer(bodySize: 1024 * 1024, watchdog: 10), 1024 * 1024)
    }

    func testSixteenMB() throws {
        XCTAssertEqual(try runTransfer(bodySize: 16 * 1024 * 1024, watchdog: 15), 16 * 1024 * 1024)
    }
}
