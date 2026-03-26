// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "ProprDBSwiftSystem",
    platforms: [
        .macOS(.v15),
        .iOS(.v18),
    ],
    products: [
        .library(name: "GeneratedSystem", targets: ["GeneratedSystem"]),
    ],
    dependencies: [
        .package(path: "../../swift/ProprDBSwiftRuntime"),
        .package(url: "https://github.com/apple/swift-protobuf.git", from: "1.31.0"),
    ],
    targets: [
        .target(
            name: "GeneratedSystem",
            dependencies: [
                .product(name: "ProprDBSwiftRuntime", package: "ProprDBSwiftRuntime"),
                .product(name: "SwiftProtobuf", package: "swift-protobuf"),
            ],
            swiftSettings: [
                .swiftLanguageMode(.v6),
            ]
        ),
        .testTarget(
            name: "GeneratedSystemTests",
            dependencies: ["GeneratedSystem"]
        ),
    ]
)
