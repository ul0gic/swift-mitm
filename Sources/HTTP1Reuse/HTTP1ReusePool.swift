import Foundation
import NIOConcurrencyHelpers
import NIOCore

public struct HTTP1ReusePolicy: Sendable {
    public let maximumIdleConnections: Int
    public let idleTimeoutSeconds: Int
    public let maximumExchanges: Int

    public init(maximumIdleConnections: Int = 32, idleTimeoutSeconds: Int = 30) throws {
        guard (1...256).contains(maximumIdleConnections), (1...60).contains(idleTimeoutSeconds) else {
            throw HTTP1ReuseError.invalidConfiguration
        }
        self.maximumIdleConnections = maximumIdleConnections
        self.idleTimeoutSeconds = idleTimeoutSeconds
        maximumExchanges = 100
    }
}

public enum HTTP1ReuseError: Error, Sendable {
    case invalidConfiguration
}

struct HTTP1ReuseKey: Hashable, Sendable {
    let downstreamID: UUID
    let authority: String
    let scheme: String
    let credentialScope: [HTTPHeaderField]
}

struct ReusedHTTP1Connection: Sendable {
    let channel: Channel
    let completedExchanges: Int
}

final class HTTP1ReusePool: Sendable {
    private struct Entry {
        let id: UUID
        let key: HTTP1ReuseKey
        let channel: Channel
        let completedExchanges: Int
        var expiry: Scheduled<Void>?
    }

    let policy: HTTP1ReusePolicy
    private let entries = NIOLockedValueBox([UUID: Entry]())

    init(policy: HTTP1ReusePolicy) { self.policy = policy }

    func take(key: HTTP1ReuseKey) -> ReusedHTTP1Connection? {
        guard let entry = entries.withLockedValue({ $0.removeValue(forKey: key.downstreamID) }) else { return nil }
        entry.expiry?.cancel()
        guard entry.key == key, entry.channel.isActive else {
            entry.channel.close(promise: nil)
            return nil
        }
        return ReusedHTTP1Connection(channel: entry.channel, completedExchanges: entry.completedExchanges)
    }

    @discardableResult
    func put(channel: Channel, key: HTTP1ReuseKey, completedExchanges: Int) -> Bool {
        guard channel.isActive, completedExchanges < policy.maximumExchanges else { return false }
        let entry = Entry(id: UUID(), key: key, channel: channel, completedExchanges: completedExchanges, expiry: nil)
        let inserted = entries.withLockedValue { entries in
            guard entries[key.downstreamID] == nil, entries.count < policy.maximumIdleConnections else { return false }
            entries[key.downstreamID] = entry
            return true
        }
        guard inserted else { return false }
        let entryID = entry.id
        let expiry = channel.eventLoop.scheduleTask(in: .seconds(Int64(policy.idleTimeoutSeconds))) { [weak self] in
            if let self { self.expire(downstreamID: key.downstreamID, entryID: entryID) }
        }
        let retained = entries.withLockedValue { entries in
            guard var current = entries[key.downstreamID], current.id == entryID else { return false }
            current.expiry = expiry
            entries[key.downstreamID] = current
            return true
        }
        if !retained { expiry.cancel() }
        channel.closeFuture.whenComplete { [weak self] _ in
            expiry.cancel()
            self?.forget(downstreamID: key.downstreamID, entryID: entryID)
        }
        return true
    }

    func close(downstreamID: UUID) {
        let entry = entries.withLockedValue { $0.removeValue(forKey: downstreamID) }
        entry?.expiry?.cancel()
        entry?.channel.close(promise: nil)
    }

    func closeAll() {
        let channels = entries.withLockedValue { entries in
            for entry in entries.values { entry.expiry?.cancel() }
            let channels = entries.values.map(\.channel)
            entries.removeAll()
            return channels
        }
        for channel in channels { channel.close(promise: nil) }
    }

    private func forget(downstreamID: UUID, entryID: UUID) {
        entries.withLockedValue { entries in
            if entries[downstreamID]?.id == entryID { entries.removeValue(forKey: downstreamID) }
        }
    }

    private func expire(downstreamID: UUID, entryID: UUID) {
        let channel = entries.withLockedValue { entries -> Channel? in
            guard entries[downstreamID]?.id == entryID else { return nil }
            return entries.removeValue(forKey: downstreamID)?.channel
        }
        channel?.close(promise: nil)
    }
}

final class HTTP1IdleGuard: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = NIOAny

    func channelRead(context: ChannelHandlerContext, data: NIOAny) { context.close(promise: nil) }
    func errorCaught(context: ChannelHandlerContext, error: Error) { context.close(promise: nil) }
}
