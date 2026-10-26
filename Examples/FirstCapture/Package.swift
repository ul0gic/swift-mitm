// swift-tools-version: 6.4
import PackageDescription

let package = Package(
    name: "FirstCapture",
    platforms: [.macOS(.v26)],
    products: [.executable(name: "FirstCapture", targets: ["FirstCapture"])],
    dependencies: [.package(path: "../..")],
    targets: [
        .executableTarget(
            name: "FirstCapture",
            dependencies: [.product(name: "SwiftMITM", package: "swift-mitm")]
        )
    ]
)
