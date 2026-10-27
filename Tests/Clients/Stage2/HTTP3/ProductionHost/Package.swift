// swift-tools-version: 6.4
import PackageDescription

let package = Package(
    name: "HTTP3ProductionHost",
    platforms: [.macOS(.v27)],
    products: [.executable(name: "HTTP3ProductionHost", targets: ["HTTP3ProductionHost"])],
    dependencies: [
        .package(path: "../../../../.."),
        .package(path: "../../../../../Packages/SwiftMITMHTTP3"),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.104.0"),
        .package(url: "https://github.com/apple/swift-log.git", exact: "1.16.2")
    ],
    targets: [
        .executableTarget(
            name: "HTTP3ProductionHost",
            dependencies: [
                .product(name: "SwiftMITM", package: "swift-mitm"),
                .product(name: "SwiftMITMHTTP3", package: "swiftmitmhttp3"),
                .product(name: "NIOConcurrencyHelpers", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "Logging", package: "swift-log")
            ]
        )
    ]
)
