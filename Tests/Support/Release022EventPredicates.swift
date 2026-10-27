import NIOHTTP2

import SwiftMITM

func release022IsResponseEnd(_ event: CaptureEvent) -> Bool {
    if case .responseEnd = event {
        true
    } else {
        false
    }
}

func release022IsTerminalHeader(_ frame: HTTP2Frame.FramePayload) -> Bool {
    if case let .headers(headers) = frame {
        headers.endStream
    } else {
        false
    }
}
