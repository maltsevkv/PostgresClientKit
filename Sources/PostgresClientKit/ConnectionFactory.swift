//
//  ConnectionFactory.swift
//  PostgresClientKit
//
//  Copyright 2023 David Pitfield and the PostgresClientKit contributors
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

import NIO
import NIOSSL

public protocol ConnectionFactory { // FIXME: doc
    
    func createChannel() async throws -> Channel

    var ssl: Bool { get }

    var sslEnabler: (Channel) async throws -> Void { get }

    var database: String  { get }
    
    var applicationName: String { get }
}

extension ConnectionFactory {
    
    public func connect(user: String,
                        credential: Credential,
                        delegate: ConnectionDelegate? = nil) async throws -> Connection {
        
        let channel: Channel
        
        do {
            channel = try await createChannel()
        } catch {
            Postgres.logger.severe("Unable to create channel: \(error)")
            throw PostgresError.socketError(cause: error)
        }

        return try await Connection(
            channel: channel,
            ssl: ssl,
            sslEnabler: sslEnabler,
            database: database,
            applicationName: applicationName,
            user: user,
            credential: credential,
            delegate: delegate)
    }
}

public class DefaultConnectionFactory: ConnectionFactory { // FIXME: can this be a struct?
        
        //
    // MARK: Basic configuration
    //
    
    /// The hostname or IP address of the Postgres server.  Defaults to `localhost`.
    public var host = "localhost"
    
    /// The port number of the Postgres server.  Defaults to `5432`.
    public var port = 5432
    
    /// Whether to use SSL/TLS to connect to the Postgres server.  Defaults to `true`.
    public var ssl = true
    
    /// The name of the database on the Postgres server.  Defaults to `postgres`.
    public var database = "postgres"
    
    /// The Postgres `application_name` parameter.  Included in the `pg_stat_activity` view and
    /// displayed by pgAdmin.  Defaults to `PostgresClientKit`.
    public var applicationName = "PostgresClientKit"
    
    
    //
    // MARK: SwiftNIO channel customization
    //
    
    public var clientBootstrap: ClientBootstrap

    public var sslContext: NIOSSLContext
    
    public var sslServerName: String? = nil
    
    public var sslEnabler: (Channel) async throws -> Void
    
    
    //
    // MARK: Initialization
    //
    
    private let eventLoopGroup: EventLoopGroup
    
    public init(eventLoopGroup: EventLoopGroup) throws {
        
        self.eventLoopGroup = eventLoopGroup
        
        clientBootstrap = ClientBootstrap(group: eventLoopGroup)
            .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .channelOption(ChannelOptions.maxMessagesPerRead, value: 1) // required for backpressure
            .channelInitializer { channel in
                channel.pipeline.addHandler(
                    AsyncAwaitChannelHandler(unconsumedBytesHighWatermark: 2048,
                                             unconsumedBytesLowWatermark: 1024))
            }
        
        sslContext = try NIOSSLContext(configuration: .makeClientConfiguration())
        
        sslEnabler = { _ in } // required before referencing self

        sslEnabler = { (channel) in
            try await channel.eventLoop.submit {
                let sslHandler = try NIOSSLClientHandler(
                    context: self.sslContext,
                    serverHostname: self.sslServerName ?? self.host)
                try channel.pipeline.syncOperations.addHandler(sslHandler, position: .first)
            }.get()
        }
    }
    
    
    //
    // MARK: ConnectionFactory conformance
    //

    public func createChannel() async throws -> Channel {
        Postgres.logger.fine("Creating channel to port \(port) on host \(host)")
        return try await clientBootstrap.connect(host: host, port: port).get()
    }
}

// EOF
