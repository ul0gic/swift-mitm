// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Apple Inc. and the SwiftNIO project authors

import NIOCore
import NIOHTTP1

struct HTTPServerRequestMethodQueue {

    private var first: HTTPMethod?

    private var overflow: CircularBuffer<HTTPMethod>?

    mutating func append(_ method: HTTPMethod) {
        if self.first == nil {
            self.first = method
        } else {
            if self.overflow == nil {
                self.overflow = CircularBuffer(initialCapacity: 4)
            }
            self.overflow!.append(method)
        }
    }

    mutating func popFirst() -> HTTPMethod? {
        guard let method = self.first else {
            return nil
        }
        self.first = self.overflow?.popFirst()
        return method
    }
}
