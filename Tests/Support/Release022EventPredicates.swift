import NIOHTTP2

import SwiftMITM

func release022IsResponseEnd(_ event: CaptureEvent) -> Bool {
    if case .responseEnd = event {
        true
    } else {
        false
    }
}

func release022IsRequestEnd(_ event: CaptureEvent) -> Bool {
    if case .requestEnd = event {
        true
    } else {
        false
    }
}

func release022IsConnectionFailure(_ event: CaptureEvent) -> Bool {
    if case .connectionFailure = event {
        true
    } else {
        false
    }
}

func release022IsWebSocketOpen(_ event: CaptureEvent) -> Bool {
    if case .webSocketOpen = event {
        true
    } else {
        false
    }
}

func release022IsWebSocketClose(_ event: CaptureEvent) -> Bool {
    if case .webSocketClose = event {
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

func release022IsTerminalData(_ frame: HTTP2Frame.FramePayload) -> Bool {
    if case let .data(data) = frame {
        data.endStream
    } else {
        false
    }
}
