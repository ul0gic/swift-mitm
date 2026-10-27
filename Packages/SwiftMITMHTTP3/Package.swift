// swift-tools-version: 6.4
import PackageDescription

let package = Package(
    name: "SwiftMITMHTTP3",
    platforms: [.macOS(.v27)],
    products: [
        .library(name: "SwiftMITMHTTP3", targets: ["SwiftMITMHTTP3"]),
        .executable(name: "HTTP3DependencyProbe", targets: ["HTTP3DependencyProbe"]),
        .executable(name: "HTTP3TransportProbe", targets: ["HTTP3TransportProbe"])
    ],
    dependencies: [
        .package(path: "../.."),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.104.0"),
        .package(url: "https://github.com/apple/swift-nio-extras.git", from: "1.22.0"),
        .package(url: "https://github.com/apple/swift-http-types.git", from: "1.8.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.5.0"),
        .package(url: "https://github.com/apple/swift-certificates.git", from: "1.5.0"),
        .package(url: "https://github.com/apple/swift-asn1.git", from: "1.1.0"),
        .package(
            url: "https://github.com/apple/swift-nio-quic-helpers.git",
            revision: "e456909e3f6b157a0f1d410e77fe8a96da5dab69"
        ),
        .package(
            url: "https://github.com/ul0gic/swift-nio-quic.git",
            revision: "bb944a30aa05cd370fdfb3127c033b7ff3fbc125"
        ),
        .package(
            url: "https://github.com/ul0gic/swift-nio-http3.git",
            revision: "e05d6396d52bc18b1587f810fb02d82d938076ef"
        )
    ],
    targets: [
        .target(
            name: "SwiftMITMHTTP3",
            dependencies: [
                .product(name: "SwiftMITM", package: "swift-mitm"),
                .product(name: "X509", package: "swift-certificates"),
                .product(name: "SwiftASN1", package: "swift-asn1"),
                .product(name: "NIOQUICHelpers", package: "swift-nio-quic-helpers"),
                .product(name: "NIOQUIC", package: "swift-nio-quic"),
                .product(name: "NIOHTTP3", package: "swift-nio-http3"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOHTTPTypes", package: "swift-nio-extras"),
                .product(name: "HTTPTypes", package: "swift-http-types"),
                .product(name: "Logging", package: "swift-log")
            ]
        ),
        .executableTarget(
            name: "HTTP3DependencyProbe",
            dependencies: [
                .product(name: "SwiftMITM", package: "swift-mitm"),
                .product(name: "NIOQUIC", package: "swift-nio-quic"),
                .product(name: "NIOHTTP3", package: "swift-nio-http3")
            ]
        ),
        .executableTarget(
            name: "HTTP3TransportProbe",
            dependencies: [
                .product(name: "SwiftMITM", package: "swift-mitm"),
                .product(name: "NIOQUICHelpers", package: "swift-nio-quic-helpers"),
                .product(name: "NIOQUIC", package: "swift-nio-quic"),
                .product(name: "NIOHTTP3", package: "swift-nio-http3"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOHTTPTypes", package: "swift-nio-extras"),
                .product(name: "HTTPTypes", package: "swift-http-types"),
                .product(name: "Logging", package: "swift-log")
            ]
        )
    ]
)
