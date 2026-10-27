// swift-tools-version: 6.4
import PackageDescription

let package = Package(
    name: "SwiftMITMHTTP3",
    platforms: [.macOS(.v27)],
    products: [
        .executable(name: "HTTP3DependencyProbe", targets: ["HTTP3DependencyProbe"]),
        .executable(name: "HTTP3TransportProbe", targets: ["HTTP3TransportProbe"])
    ],
    dependencies: [
        .package(path: "../.."),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.104.0"),
        .package(url: "https://github.com/apple/swift-nio-extras.git", from: "1.22.0"),
        .package(url: "https://github.com/apple/swift-http-types.git", from: "1.8.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.5.0"),
        .package(
            url: "https://github.com/apple/swift-nio-quic-helpers.git",
            revision: "e456909e3f6b157a0f1d410e77fe8a96da5dab69"
        ),
        .package(
            url: "https://github.com/ul0gic/swift-nio-quic.git",
            revision: "b82a67556a5d9d3acd9c00348eb17f0817d4627f"
        ),
        .package(
            url: "https://github.com/ul0gic/swift-nio-http3.git",
            revision: "1fc55937e2f6ef62ebc156d0b91475766d8ef777"
        )
    ],
    targets: [
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
