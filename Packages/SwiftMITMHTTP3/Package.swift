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
            url: "https://github.com/apple/swift-nio-quic.git",
            revision: "11b231126952b3ef408c4e1ab95d14e16af5d867"
        ),
        .package(
            url: "https://github.com/apple/swift-nio-http3.git",
            revision: "e2c54cdbf50dc64bf9e80861922f8d9400d2acce"
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
