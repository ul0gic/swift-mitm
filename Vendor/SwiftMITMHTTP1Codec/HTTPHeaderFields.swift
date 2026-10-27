// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2022 Apple Inc. and the SwiftNIO project authors

import NIOCore
import NIOHTTP1

extension String {

    fileprivate var isValidHeaderFieldValue: Bool {
        let fastResult = self.utf8.withContiguousStorageIfAvailable { ptr in
            ptr.allSatisfy { $0.isValidHeaderFieldValueByte }
        }
        if let fastResult = fastResult {
            return fastResult
        } else {
            return self.utf8._isValidHeaderFieldValue_slowPath
        }
    }

    fileprivate var isValidHeaderFieldName: Bool {
        let fastResult = self.utf8.withContiguousStorageIfAvailable { ptr in
            ptr.allSatisfy { $0.isValidHeaderFieldNameByte }
        }
        if let fastResult = fastResult {
            return fastResult
        } else {
            return self.utf8._isValidHeaderFieldName_slowPath
        }
    }
}

extension String.UTF8View {

    @inline(never)
    fileprivate var _isValidHeaderFieldName_slowPath: Bool {
        self.allSatisfy { $0.isValidHeaderFieldNameByte }
    }

    @inline(never)
    fileprivate var _isValidHeaderFieldValue_slowPath: Bool {
        self.allSatisfy { $0.isValidHeaderFieldValueByte }
    }
}

extension UInt8 {

    @inline(__always)
    fileprivate var isValidHeaderFieldNameByte: Bool {
        switch self {
        case UInt8(ascii: "0")...UInt8(ascii: "9"),
            UInt8(ascii: "a")...UInt8(ascii: "z"),
            UInt8(ascii: "A")...UInt8(ascii: "Z"),
            UInt8(ascii: "!"), UInt8(ascii: "#"),
            UInt8(ascii: "$"), UInt8(ascii: "%"),
            UInt8(ascii: "&"), UInt8(ascii: "'"),
            UInt8(ascii: "*"), UInt8(ascii: "+"),
            UInt8(ascii: "-"), UInt8(ascii: "."),
            UInt8(ascii: "^"), UInt8(ascii: "_"),
            UInt8(ascii: "`"), UInt8(ascii: "|"),
            UInt8(ascii: "~"):

            return true

        default:
            return false
        }
    }

    @inline(__always)
    fileprivate var isValidHeaderFieldValueByte: Bool {
        switch self {
        case UInt8(ascii: "\t"):

            return true
        case 0...0x1f, 0x7F:

            return false
        default:

            return true
        }
    }
}

extension HTTPHeaders {

    var areValidToSend: Bool {
        for (name, value) in self {
            if !name.isValidHeaderFieldName {
                return false
            }

            if !value.isValidHeaderFieldValue {
                return false
            }
        }

        return true
    }
}

extension ByteBuffer {
    mutating func writeBytePreservingHeaders(_ headers: HTTPHeaders) {
        for header in headers {
            self.writeString(header.name)
            self.writeStaticString(": ")
            self.writeBytes(header.value.unicodeScalars.map { UInt8($0.value) })
            self.writeStaticString("\r\n")
        }
        self.writeStaticString("\r\n")
    }
}
