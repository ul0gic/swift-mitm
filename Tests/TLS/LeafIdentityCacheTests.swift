import Dispatch
import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
import NIOSSL
import XCTest

@testable import SwiftMITM

final class LeafIdentityCacheTests: XCTestCase {
    func testConcurrentSameHostMissMintsOnceOffEventLoop() async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let pool = NIOThreadPool(numberOfThreads: 2)
        pool.start()
        let authority = try CertificateAuthority()
        let identity = try authority.mintIdentity(forHost: "example.com")
        let mintCount = NIOLockedValueBox(0)
        let ranOnEventLoop = NIOLockedValueBox(false)
        let started = DispatchSemaphore(value: 0)
        let release = DispatchSemaphore(value: 0)
        let loop = group.next()
        let cache = LeafIdentityCache(
            threadPool: pool,
            maximumEntries: 2,
            maximumPendingMints: 2
        ) { _ in
            mintCount.withLockedValue { $0 += 1 }
            ranOnEventLoop.withLockedValue { $0 = loop.inEventLoop }
            started.signal()
            release.wait()
            return identity
        }

        let first = cache.identity(forHost: "Example.COM", on: loop)
        XCTAssertEqual(started.wait(timeout: .now() + 2), .success)
        let second = cache.identity(forHost: "example.com", on: group.next())
        let third = cache.identity(forHost: "EXAMPLE.COM", on: group.next())
        release.signal()

        let identities = try await [first.get(), second.get(), third.get()]
        XCTAssertEqual(try identities.map(leafDER), Array(repeating: try leafDER(identity), count: 3))
        XCTAssertEqual(mintCount.withLockedValue { $0 }, 1)
        XCTAssertFalse(ranOnEventLoop.withLockedValue { $0 })

        try await pool.shutdownGracefully()
        try await group.shutdownGracefully()
    }

    func testLRUEvictionBoundsCompletedIdentitiesUnderHostnameChurn() async throws {
        let loop = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let pool = NIOThreadPool(numberOfThreads: 1)
        pool.start()
        let authority = try CertificateAuthority()
        let cache = LeafIdentityCache(
            authority: authority,
            threadPool: pool,
            maximumEntries: 2,
            maximumPendingMints: 2
        )

        _ = try await cache.identity(forHost: "a.example", on: loop.next()).get()
        let originalB = try await cache.identity(forHost: "b.example", on: loop.next()).get()
        _ = try await cache.identity(forHost: "a.example", on: loop.next()).get()
        _ = try await cache.identity(forHost: "c.example", on: loop.next()).get()

        XCTAssertEqual(cache.snapshot().cachedHosts, ["a.example", "c.example"])
        for index in 0..<32 {
            _ = try await cache.identity(forHost: "churn-\(index).example", on: loop.next()).get()
            XCTAssertLessThanOrEqual(cache.snapshot().cachedHosts.count, 2)
        }
        let replacementB = try await cache.identity(forHost: "b.example", on: loop.next()).get()
        XCTAssertNotEqual(try leafDER(originalB), try leafDER(replacementB))

        try await pool.shutdownGracefully()
        try await loop.shutdownGracefully()
    }

    func testPendingCapacityRejectsDistinctHostButAllowsSameHostWaiter() async throws {
        let loop = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let pool = NIOThreadPool(numberOfThreads: 1)
        pool.start()
        let authority = try CertificateAuthority()
        let identity = try authority.mintIdentity(forHost: "held.example")
        let started = DispatchSemaphore(value: 0)
        let release = DispatchSemaphore(value: 0)
        let cache = LeafIdentityCache(
            threadPool: pool,
            maximumEntries: 1,
            maximumPendingMints: 1
        ) { _ in
            started.signal()
            release.wait()
            return identity
        }

        let first = cache.identity(forHost: "held.example", on: loop.next())
        XCTAssertEqual(started.wait(timeout: .now() + 2), .success)
        let waiter = cache.identity(forHost: "HELD.EXAMPLE", on: loop.next())
        let rejected = cache.identity(forHost: "other.example", on: loop.next())
        XCTAssertEqual(cache.snapshot().pendingHostCount, 1)
        do {
            _ = try await rejected.get()
            XCTFail("a distinct miss beyond the pending limit must fail")
        } catch let error as LeafIdentityCacheError {
            XCTAssertEqual(error, .pendingCapacityExceeded)
        }
        release.signal()
        let firstIdentity = try await first.get()
        let waiterIdentity = try await waiter.get()
        XCTAssertEqual(try leafDER(firstIdentity), try leafDER(waiterIdentity))

        try await pool.shutdownGracefully()
        try await loop.shutdownGracefully()
    }

    func testMintFailureResolvesEveryWaiterAndAllowsRetry() async throws {
        let loop = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let pool = NIOThreadPool(numberOfThreads: 1)
        pool.start()
        let authority = try CertificateAuthority()
        let identity = try authority.mintIdentity(forHost: "retry.example")
        let attempts = NIOLockedValueBox(0)
        let cache = LeafIdentityCache(
            threadPool: pool,
            maximumEntries: 1,
            maximumPendingMints: 1
        ) { _ in
            let attempt = attempts.withLockedValue { value in
                value += 1
                return value
            }
            if attempt == 1 {
                throw CacheTestError.mintFailed
            }
            return identity
        }

        let first = cache.identity(forHost: "retry.example", on: loop.next())
        let waiter = cache.identity(forHost: "retry.example", on: loop.next())
        for future in [first, waiter] {
            do {
                _ = try await future.get()
                XCTFail("the failed single flight must fail every waiter")
            } catch CacheTestError.mintFailed {}
        }
        XCTAssertEqual(cache.snapshot().pendingHostCount, 0)
        let retry = try await cache.identity(forHost: "retry.example", on: loop.next()).get()
        XCTAssertEqual(try leafDER(retry), try leafDER(identity))
        XCTAssertEqual(attempts.withLockedValue { $0 }, 2)

        try await pool.shutdownGracefully()
        try await loop.shutdownGracefully()
    }

    private func leafDER(_ identity: MintedIdentity) throws -> [UInt8] {
        guard case .certificate(let certificate) = identity.certificateChain.first else {
            throw CacheTestError.invalidIdentity
        }
        return try certificate.toDERBytes()
    }
}

private enum CacheTestError: Error {
    case invalidIdentity
    case mintFailed
}
