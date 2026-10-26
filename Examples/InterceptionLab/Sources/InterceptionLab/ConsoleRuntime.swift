import Darwin
import Dispatch
import Foundation

struct StopSignals {
    let stream: AsyncStream<Int32>
    let continuation: AsyncStream<Int32>.Continuation
    let sources: [any DispatchSourceSignal]

    func cancel() {
        for source in sources { source.cancel() }
        continuation.finish()
    }
}

func stopSignals() -> StopSignals {
    let pair = AsyncStream<Int32>.makeStream(bufferingPolicy: .bufferingNewest(1))
    signal(SIGINT, SIG_IGN)
    signal(SIGTERM, SIG_IGN)
    let sources = [SIGINT, SIGTERM].map { number in
        let source = DispatchSource.makeSignalSource(signal: number, queue: .global())
        source.setEventHandler { pair.continuation.yield(number) }
        source.resume()
        return source
    }
    return StopSignals(stream: pair.stream, continuation: pair.continuation, sources: sources)
}

func writeError(_ value: String) {
    do { try FileHandle.standardError.write(contentsOf: Data((value + "\n").utf8)) }
    catch { exit(1) }
}

