// swift-tools-version: 6.4
import PackageDescription

let package = Package(
    name: "SwiftMITMHTTP3",
    platforms: [.macOS(.v27)],
    products: [
        .executable(name: "HTTP3DependencyProbe", targets: ["HTTP3DependencyProbe"])
    ],
    dependencies: [
        .package(path: "../.."),
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
        )
    ]
)
