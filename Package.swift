// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "SwiftMITM",
    platforms: [.macOS(.v14)],
    products: [
        .library(name: "SwiftMITM", targets: ["SwiftMITM"])
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.76.0"),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", from: "2.29.0"),
        .package(url: "https://github.com/apple/swift-nio-http2.git", from: "1.44.0"),
        .package(url: "https://github.com/apple/swift-certificates.git", from: "1.5.0"),
        .package(url: "https://github.com/apple/swift-crypto.git", from: "4.0.0")
    ],
    targets: [
        .target(
            name: "SwiftMITM",
            dependencies: [
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
            ]
        ),
        .testTarget(
            name: "SwiftMITMTests",
            dependencies: [
                "SwiftMITM",
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
            ]
        )
    ]
)
