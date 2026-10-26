import NIOCore
import NIOEmbedded
import NIOPosix
import XCTest

@testable import SwiftMITM

final class EgressFilteringResolverTests: XCTestCase {
    private struct FakeResolver: Resolver, Sendable {
        let loop: EventLoop
        let ipv4: [SocketAddress]
        let ipv6: [SocketAddress]

        func initiateAQuery(host: String, port: Int) -> EventLoopFuture<[SocketAddress]> {
            loop.makeSucceededFuture(ipv4)
        }

        func initiateAAAAQuery(host: String, port: Int) -> EventLoopFuture<[SocketAddress]> {
            loop.makeSucceededFuture(ipv6)
        }

        func cancelQueries() {}
    }

    func testMixedAnswersExposeOnlyAllowedConnectionCandidates() throws {
        let loop = EmbeddedEventLoop()
        let allowedV4 = try address("8.8.8.8")
        let allowedV6 = try address("2606:4700:4700::1111")
        let resolver = EgressFilteringResolver(
            resolver: FakeResolver(
                loop: loop,
                ipv4: [try address("127.0.0.1"), allowedV4, try address("198.18.0.1")],
                ipv6: [try address("fc00::1"), allowedV6, try address("2001:db8::1")]
            ),
            policy: .default
        )

        XCTAssertEqual(try resolver.initiateAQuery(host: "example.com", port: 443).wait(), [allowedV4])
        XCTAssertEqual(try resolver.initiateAAAAQuery(host: "example.com", port: 443).wait(), [allowedV6])
    }

    func testAllDeniedAnswersExposeNoConnectionCandidate() throws {
        let loop = EmbeddedEventLoop()
        let resolver = EgressFilteringResolver(
            resolver: FakeResolver(
                loop: loop,
                ipv4: [try address("127.0.0.1"), try address("192.168.1.1")],
                ipv6: [try address("::1"), try address("fe80::1")]
            ),
            policy: .default
        )

        XCTAssertEqual(try resolver.initiateAQuery(host: "internal.example", port: 443).wait(), [])
        XCTAssertEqual(try resolver.initiateAAAAQuery(host: "internal.example", port: 443).wait(), [])
    }

    func testAllowInternalBypassesResolverFiltering() throws {
        let loop = EmbeddedEventLoop()
        let deniedV4 = try address("127.0.0.1")
        let deniedV6 = try address("::1")
        let resolver = EgressFilteringResolver(
            resolver: FakeResolver(loop: loop, ipv4: [deniedV4], ipv6: [deniedV6]),
            policy: EgressPolicy(allowInternal: true)
        )

        XCTAssertEqual(try resolver.initiateAQuery(host: "localhost", port: 443).wait(), [deniedV4])
        XCTAssertEqual(try resolver.initiateAAAAQuery(host: "localhost", port: 443).wait(), [deniedV6])
    }

    private func address(_ ip: String) throws -> SocketAddress {
        try SocketAddress(ipAddress: ip, port: 443)
    }
}
