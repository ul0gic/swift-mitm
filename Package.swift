// swift-tools-version: 6.4
import PackageDescription

let package = Package(
    name: "SwiftMITM",
    platforms: [.macOS(.v26)],
    products: [
        .library(name: "SwiftMITM", targets: ["SwiftMITM"]),
        .library(name: "SwiftMITMInspection", targets: ["SwiftMITMInspection"])
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.104.0"),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", from: "2.29.0"),
        .package(url: "https://github.com/apple/swift-nio-http2.git", from: "1.44.0"),
        .package(url: "https://github.com/apple/swift-certificates.git", from: "1.5.0"),
        .package(url: "https://github.com/apple/swift-crypto.git", from: "4.0.0"),
        .package(url: "https://github.com/apple/swift-protobuf.git", exact: "1.32.0")
    ],
    targets: [
        .target(
            name: "SwiftMITMInspection",
            dependencies: [
                "SwiftMITM",
                .product(name: "Crypto", package: "swift-crypto"),
                .product(name: "SwiftProtobuf", package: "swift-protobuf"),
                .product(name: "NIOConcurrencyHelpers", package: "swift-nio")
            ],
            path: "Inspection/Sources/SwiftMITMInspection"
        ),
        .target(
            name: "SwiftMITM",
            dependencies: [
                .target(name: "SwiftMITMHTTP1Codec"),
                .product(name: "Crypto", package: "swift-crypto"),
                .product(name: "NIOConcurrencyHelpers", package: "swift-nio"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
                .product(name: "NIOSSL", package: "swift-nio-ssl"),
                .product(name: "NIOHTTP2", package: "swift-nio-http2"),
                .product(name: "NIOHPACK", package: "swift-nio-http2"),
                .product(name: "NIOTLS", package: "swift-nio"),
                .product(name: "X509", package: "swift-certificates")
            ],
            path: "Sources"
        ),
        .target(
            name: "CSwiftMITMLLHTTP",
            path: "Vendor/CSwiftMITMLLHTTP",
            exclude: ["LICENSE.txt", "LICENSE-SwiftNIO.txt", "NOTICE-SwiftNIO.txt", "UPSTREAM.md"],
            publicHeadersPath: "include",
            cSettings: [.define("_GNU_SOURCE"), .define("LLHTTP_STRICT_MODE")]
        ),
        .target(
            name: "SwiftMITMHTTP1Codec",
            dependencies: [
                .target(name: "CSwiftMITMLLHTTP"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio")
            ],
            path: "Vendor/SwiftMITMHTTP1Codec",
            exclude: ["LICENSE.txt", "NOTICE-SwiftNIO.txt", "UPSTREAM.md"]
        ),
        .testTarget(
            name: "SwiftMITMTests",
            dependencies: [
                "SwiftMITM",
                "SwiftMITMInspection",
                .product(name: "Crypto", package: "swift-crypto"),
                .product(name: "NIOConcurrencyHelpers", package: "swift-nio"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOEmbedded", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
                .product(name: "NIOHTTP2", package: "swift-nio-http2"),
                .product(name: "NIOHPACK", package: "swift-nio-http2"),
                .product(name: "NIOSSL", package: "swift-nio-ssl"),
                .product(name: "NIOTLS", package: "swift-nio"),
                .product(name: "X509", package: "swift-certificates")
            ],
            path: "Tests",
            exclude: ["Clients"]
        )
    ]
)
