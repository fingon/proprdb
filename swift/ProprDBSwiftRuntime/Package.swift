// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "ProprDBSwiftRuntime",
    platforms: [
        .macOS(.v15),
        .iOS(.v18),
    ],
    products: [
        .library(name: "ProprDBSwiftRuntime", targets: ["ProprDBSwiftRuntime"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-log.git", from: "1.6.0"),
        .package(url: "https://github.com/apple/swift-protobuf.git", from: "1.31.0"),
    ],
    targets: [
        .systemLibrary(name: "CSQLite"),
        .target(
            name: "ProprDBSwiftRuntime",
            dependencies: [
                "CSQLite",
                .product(name: "Logging", package: "swift-log"),
                .product(name: "SwiftProtobuf", package: "swift-protobuf"),
            ],
            swiftSettings: [
                .swiftLanguageMode(.v6),
            ]
        ),
    ]
)
