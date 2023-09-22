// swift-tools-version:5.8

import PackageDescription

let package = Package(
    name: "PostgresClientKit",
    platforms: [ .iOS(.v16), .macOS(.v13) ],    
    products: [
        .library(
            name: "PostgresClientKit",
            targets: ["PostgresClientKit"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", .upToNextMajor(from: "2.0.0")),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", .upToNextMajor(from: "2.0.0")),
    ],
    targets: [
        .target(
            name: "PostgresClientKit",
            dependencies: [
                .product(name: "NIO", package: "swift-nio"),
                .product(name: "NIOSSL", package: "swift-nio-ssl"),
            ],
            path: "Sources"),
        .testTarget(
            name: "PostgresClientKitTests",
            dependencies: ["PostgresClientKit"]),
    ]
)

// EOF
