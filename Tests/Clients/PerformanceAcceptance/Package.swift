// swift-tools-version: 6.4
import PackageDescription

let package = Package(
    name: "PerformanceAcceptance",
    platforms: [.macOS(.v26)],
    products: [.executable(name: "PerformanceAcceptance", targets: ["PerformanceAcceptance"])],
    dependencies: [
        .package(name: "SwiftMITM", path: "../../.."),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.104.0"),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", from: "2.29.0"),
        .package(url: "https://github.com/apple/swift-nio-http2.git", from: "1.44.0")
    ],
    targets: [
        .executableTarget(
            name: "PerformanceAcceptance",
            dependencies: [
                .product(name: "SwiftMITM", package: "SwiftMITM"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOConcurrencyHelpers", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
                .product(name: "NIOTLS", package: "swift-nio"),
                .product(name: "NIOSSL", package: "swift-nio-ssl"),
                .product(name: "NIOHTTP2", package: "swift-nio-http2")
            ]
        )
    ]
)
