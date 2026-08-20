import Dispatch
import NIOConcurrencyHelpers
import NIOCore
import NIOEmbedded
import XCTest

@testable import SwiftMITM

final class ProxyConnectionRegistryTests: XCTestCase {
    func testShutdownCancelsPendingSetupAndRunsCallbackOnOwningEventLoop() async throws {
        let registry = ProxyConnectionRegistry()
        let loop = EmbeddedEventLoop()
        let callbackState = NIOLockedValueBox((count: 0, ranOnEventLoop: false))
        registry.startAccepting()
        let token = try XCTUnwrap(registry.beginSetup())
        token.onCancellation(on: loop) {
            callbackState.withLockedValue { state in
                state.count += 1
                state.ranOnEventLoop = loop.inEventLoop
            }
        }

        XCTAssertTrue(registry.beginShutdown().isEmpty)
        XCTAssertTrue(token.isCancelled)
        XCTAssertFalse(token.complete())
        await registry.waitForQuiescence()
        registry.finishShutdown()
        loop.run()

        XCTAssertEqual(callbackState.withLockedValue { $0.count }, 1)
        XCTAssertTrue(callbackState.withLockedValue { $0.ranOnEventLoop })
    }

    func testCompletionCancellationAndCallbackRegistrationAreDeduplicated() async throws {
        let registry = ProxyConnectionRegistry()
        let loop = EmbeddedEventLoop()
        let callbackCount = NIOLockedValueBox(0)
        registry.startAccepting()
        let token = try XCTUnwrap(registry.beginSetup())
        token.onCancellation(on: loop) {
            callbackCount.withLockedValue { $0 += 1 }
        }

        XCTAssertTrue(token.complete())
        XCTAssertFalse(token.complete())
        XCTAssertFalse(token.cancel())
        XCTAssertFalse(token.isCancelled)
        loop.run()
        XCTAssertEqual(callbackCount.withLockedValue { $0 }, 0)

        XCTAssertTrue(registry.beginShutdown().isEmpty)
        await registry.waitForQuiescence()
        registry.finishShutdown()
    }

    func testCallbackRegisteredAfterCancellationRunsExactlyOnce() async throws {
        let registry = ProxyConnectionRegistry()
        let loop = EmbeddedEventLoop()
        let callbackCount = NIOLockedValueBox(0)
        registry.startAccepting()
        let token = try XCTUnwrap(registry.beginSetup())

        XCTAssertTrue(token.cancel())
        XCTAssertFalse(token.cancel())
        token.onCancellation(on: loop) {
            callbackCount.withLockedValue { $0 += 1 }
        }
        loop.run()

        XCTAssertTrue(token.isCancelled)
        XCTAssertEqual(callbackCount.withLockedValue { $0 }, 1)
        XCTAssertTrue(registry.beginShutdown().isEmpty)
        await registry.waitForQuiescence()
        registry.finishShutdown()
    }

    func testConcurrentCompletionAndCancellationChooseOneTerminalTransition() async throws {
        let registry = ProxyConnectionRegistry()
        let successes = NIOLockedValueBox(0)
        registry.startAccepting()
        let token = try XCTUnwrap(registry.beginSetup())

        DispatchQueue.concurrentPerform(iterations: 100) { iteration in
            let succeeded = iteration.isMultiple(of: 2) ? token.complete() : token.cancel()
            if succeeded {
                successes.withLockedValue { $0 += 1 }
            }
        }

        XCTAssertEqual(successes.withLockedValue { $0 }, 1)
        XCTAssertFalse(token.complete())
        XCTAssertFalse(token.cancel())
        XCTAssertTrue(registry.beginShutdown().isEmpty)
        await registry.waitForQuiescence()
        registry.finishShutdown()
    }

    func testLateCompletionIsRejectedSoCallerCanCloseProducedChannel() async throws {
        let registry = ProxyConnectionRegistry()
        let loop = EmbeddedEventLoop()
        registry.startAccepting()
        let token = try XCTUnwrap(registry.beginSetup())
        let channel = try connectedChannel(loop: loop)

        XCTAssertTrue(registry.beginShutdown().isEmpty)
        if !token.complete() {
            channel.close(promise: nil)
        }
        loop.run()

        XCTAssertFalse(channel.isActive)
        await registry.waitForQuiescence()
        registry.finishShutdown()
    }

    func testChannelOwnershipLateRegistrationAndRestartSemanticsRemainUnchanged() async throws {
        let registry = ProxyConnectionRegistry()
        let loop = EmbeddedEventLoop()
        registry.startAccepting()
        let activeChannel = try connectedChannel(loop: loop)
        XCTAssertTrue(registry.register(activeChannel))

        let shutdownChannels = registry.beginShutdown()
        XCTAssertEqual(shutdownChannels.count, 1)
        shutdownChannels.forEach { $0.close(promise: nil) }
        let lateChannel = try connectedChannel(loop: loop)
        XCTAssertFalse(registry.register(lateChannel))
        loop.run()

        XCTAssertFalse(activeChannel.isActive)
        XCTAssertFalse(lateChannel.isActive)
        await registry.waitForQuiescence()
        registry.finishShutdown()

        registry.startAccepting()
        let restartedToken = try XCTUnwrap(registry.beginSetup())
        XCTAssertTrue(restartedToken.complete())
        XCTAssertTrue(registry.beginShutdown().isEmpty)
        await registry.waitForQuiescence()
        registry.finishShutdown()
    }

    private func connectedChannel(loop: EmbeddedEventLoop) throws -> EmbeddedChannel {
        let channel = EmbeddedChannel(loop: loop)
        let address = try SocketAddress(ipAddress: "127.0.0.1", port: 443)
        try channel.connect(to: address).wait()
        return channel
    }
}
