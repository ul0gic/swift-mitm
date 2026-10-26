// swift-tools-version: 6.4
import PackageDescription

let package = Package(
    name: "InterceptionLab",
    platforms: [.macOS(.v26)],
    products: [.executable(name: "InterceptionLab", targets: ["InterceptionLab"])],
    dependencies: [.package(path: "../..")],
    targets: [
        .executableTarget(
            name: "InterceptionLab",
            dependencies: [
                .product(name: "SwiftMITM", package: "swift-mitm"),
                .product(name: "SwiftMITMInspection", package: "swift-mitm")
            ]
        )
    ]
)
