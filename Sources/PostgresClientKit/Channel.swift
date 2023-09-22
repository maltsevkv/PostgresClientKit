//
//  Channel.swift
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

import Foundation
import NIO
import NIOSSL

internal extension Channel {
    
    /// Asynchronously reads from this channel.
    ///
    /// - Returns: the read data, or nil if EOF
    func asyncRead() async throws -> ByteBuffer? {
        let handler = try await pipeline.handler(type: AsyncAwaitChannelHandler.self).get()
        return try await handler.asyncRead()
    }
    
    /// Asynchronously writes to this channel.
    ///
    /// - Parameter buffer: the data to write
    func asyncWrite(_ buffer: ByteBuffer) async throws {
        let handler = try await pipeline.handler(type: AsyncAwaitChannelHandler.self).get()
        try await handler.asyncWrite(buffer)
    }
}

/// A ChannelHandler that provides an async/await interface for a channel.
///
/// Backpressure is applied to throttle the incoming data to the rate at which it is consumed
/// through the async/await interface.
public class AsyncAwaitChannelHandler: ChannelDuplexHandler {
    public typealias InboundIn = ByteBuffer
    public typealias OutboundIn = ByteBuffer
    
    /// Used to synchronize access to the channel and pendingError properties.
    /// (All other mutable properties are accessed only within the event loop.)
    private let semaphore = DispatchSemaphore(value: 1) // FIXME: replace with actor?
    
    /// The channel to which this handler belongs.
    private weak var channel: Channel? {
        get {
            semaphore.wait()
            defer { semaphore.signal() }
            return _channel
        }
        set {
            semaphore.wait()
            defer { semaphore.signal() }
            _channel = newValue
        }
    }
    
    /// Backing store for the channel property.
    private weak var _channel: Channel? = nil
    
    /// Handles incoming data or errors from SwiftNIO.
    private var channelReadHandler: ((Result<ByteBuffer, Error>) -> Void)!
    
    /// An async iterator over incoming data.
    private var asyncReadIterator: AsyncThrowingStream<ByteBuffer, Error>.Iterator!
    
    /// Whether SwiftNIO has indicated there is incoming data waiting to be read.
    private var pendingRead = false
    
    /// An error reported by SwiftNIO but not yet thrown by asyncRead/asyncWrite.
    private var pendingError: Error?  {
        get {
            semaphore.wait()
            defer { semaphore.signal() }
            return _pendingError
        }
        set {
            semaphore.wait()
            defer { semaphore.signal() }
            _pendingError = newValue
        }
    }
    
    /// Backing store for the pendingError property.
    private var _pendingError: Error? = nil

    /// The number of incoming bytes read from SwiftNIO but not yet returned by the async iterator.
    /// In other words, the number of bytes buffered by that iterator's underlying async stream.
    private var unconsumedBytesCount = 0
    
    /// An upper limit on the number of incoming bytes to buffer, above which we'll pause asking
    /// SwiftNIO for more data.
    private let unconsumedBytesHighWatermark: Int
    
    /// A lower limit on the number of incoming bytes to buffer, below which we'll resume asking
    /// SwiftNIO for more data.
    private let unconsumedBytesLowWatermark: Int
    
    /// Initializes the handler.
    ///
    /// - Parameters:
    ///   - unconsumedBytesHighWatermark: an upper limit on the number of incoming bytes to buffer,
    ///         above which we'll pause asking SwiftNIO for more data
    ///   - unconsumedBytesLowWatermark: a lower limit on the number of incoming bytes to buffer,
    ///         below which we'll resume asking SwiftNIO for more data
    public init(unconsumedBytesHighWatermark: Int = 2048, unconsumedBytesLowWatermark: Int = 1024) {
        
        self.unconsumedBytesHighWatermark = unconsumedBytesHighWatermark
        self.unconsumedBytesLowWatermark = unconsumedBytesLowWatermark
        
        asyncReadIterator = AsyncThrowingStream() { continuation in
            
            channelReadHandler = { result in
                continuation.yield(with: result)
            }
            
            continuation.onTermination = { [weak channel] _ in
                channel?.close(mode: .all, promise: nil)
            }
        }.makeAsyncIterator()
    }
    
    
    //
    // MARK: ChannelInboundHandler conformance
    //
    
    public func channelActive(context: ChannelHandlerContext) {
        channel = context.channel
    }
    
    public func channelInactive(context: ChannelHandlerContext) {
        channel = nil
    }
    
    public func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        
        assert(unconsumedBytesCount <= unconsumedBytesHighWatermark,
               "channelRead: already above high watermark " +
               "(ensure ChannelOptions.maxMessagesPerRead == 1)")
        
        let buffer = unwrapInboundIn(data)
        unconsumedBytesCount += buffer.readableBytes
        channelReadHandler(.success(buffer))
    }
    
    public func errorCaught(context: ChannelHandlerContext, error: Error) {
        pendingError = error                // to cause the error to be reported by asyncWrite
        channelReadHandler(.failure(error)) // to cause the error to be reported by asyncRead
    }
    
    
    //
    // MARK: ChannelOutboundHandler conformance
    //
    
    public func read(context: ChannelHandlerContext) {
        if unconsumedBytesCount > unconsumedBytesHighWatermark {
            pendingRead = true
        } else {
            context.read()
        }
    }
    
    
    //
    // MARK: async/await interface
    //
    
    fileprivate func asyncRead() async throws -> ByteBuffer? {
        
        guard let channel else { return nil }
        
        let buffer = try await asyncReadIterator.next()
        
        if let buffer {
            try await channel.eventLoop.submit { // get back onto the EventLoop thread
                self.unconsumedBytesCount -= buffer.readableBytes
                
                if self.unconsumedBytesCount <= self.unconsumedBytesLowWatermark && self.pendingRead {
                    self.pendingRead = false
                    channel.read()
                }
            }.get()
        }

        return buffer
    }
    
    fileprivate func asyncWrite(_ buffer: ByteBuffer) async throws {
        
        if let pendingError {
            throw pendingError
        }

        do {
            try await channel?.writeAndFlush(buffer)
        } catch {
            throw pendingError ?? error // an error reported on the event loop takes precedence
        }
    }
}

public extension TLSConfiguration {
    
    /// Makes a default TLSConfiguration with the specified certificate verification behavior.
    ///
    /// - Parameter certificateVerification: the certificate verification behavior
    /// - Returns: the TLSConfiguration
    static func makeClientConfiguration(
        certificateVerification: CertificateVerification) -> TLSConfiguration {
            var tlsConfiguration = TLSConfiguration.makeClientConfiguration()
            tlsConfiguration.certificateVerification = certificateVerification
            return tlsConfiguration
        }
}

// EOF
    