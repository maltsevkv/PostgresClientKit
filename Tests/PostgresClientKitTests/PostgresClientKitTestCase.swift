//
//  PostgresClientKitTestCase.swift
//  PostgresClientKit
//
//  Copyright 2019 David Pitfield and the PostgresClientKit contributors
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

import PostgresClientKit
import NIO
import NIOSSL
import XCTest

/// A base class for testing PostgresClientKit.
class PostgresClientKitTestCase: XCTestCase {
    
    //
    // MARK: Localization
    //
    
    /// The en_US_POSIX locale.
    let enUsPosixLocale = Locale(identifier: "en_US_POSIX")
    
    /// The UTC/GMT time zone.
    let utcTimeZone = TimeZone(secondsFromGMT: 0)!
    
    /// The PST/PDT time zone.
    let pacificTimeZone = TimeZone.init(identifier: "America/Los_Angeles")!
    
    #if os(Linux) // temporary workaround for https://bugs.swift.org/browse/SR-10515
    
        /// A calendar based on the `en_US_POSIX` locale and the UTC/GMT time zone.
        var enUsPosixUtcCalendar: Calendar {
            _enUsPosixUtcCalendar.timeZone = utcTimeZone
            return _enUsPosixUtcCalendar
        }
    
        private var _enUsPosixUtcCalendar: Calendar = {
            var calendar = Calendar(identifier: .gregorian)
            calendar.locale = Locale(identifier: "en_US_POSIX")
            calendar.timeZone = TimeZone(secondsFromGMT: 0)!    
            return calendar
        }()
    
    #else
    
        /// A calendar based on the `en_US_POSIX` locale and the UTC/GMT time zone.
        lazy var enUsPosixUtcCalendar: Calendar = {
            var calendar = Calendar(identifier: .gregorian)
            calendar.locale = enUsPosixLocale
            calendar.timeZone = utcTimeZone
            return calendar
        }()
    
    #endif
    
    /// Temporary workaround for https://bugs.swift.org/browse/SR-11569.
    func isValidDate(_ dc: DateComponents) -> Bool {
        
        var calendar = dc.calendar ?? enUsPosixUtcCalendar
        
        if let timeZone = dc.timeZone {
            calendar.timeZone = timeZone
        }
        
        return dc.isValidDate(in: calendar)
    }
    

    //
    // MARK: Connections
    //
    
    /// The SwiftNIO `EventLoopGroup` used for testing.
    private static let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    
    // An SSLContext that disables certificate verification to allow self-signed certificates.
    private static let sslContext = try! NIOSSLContext(
        configuration: .makeClientConfiguration(certificateVerification: .none))
    
    /// Creates a default `ConnectionFactory`.
    ///
    /// The returned `ConnectionFactory` can be further customized by the caller.  However,
    /// because `ConnectionFactory` is a class, this method should be called each time a
    /// distinct `ConnectionFactory` is required.
    /// - Returns: the `ConnnectionFactory`
    static func createConnectionFactory() throws -> DefaultConnectionFactory {
        let environment = TestEnvironment.current
        let factory = try! DefaultConnectionFactory(eventLoopGroup: Self.eventLoopGroup)
        factory.host = environment.postgresHost
        factory.port = environment.postgresPort
        factory.ssl = true // the default
        factory.database = environment.postgresDatabase
        factory.sslContext = Self.sslContext
        factory.sslServerName = environment.postgresSSLServerName
        return factory
    }
    
    /// A `ConnectionFactory` that creates SSL/TLS-encrypted connections.
    static let encryptedConnectionFactory: ConnectionFactory = {
        let factory = try! createConnectionFactory()
        factory.ssl = true // the default
        return factory
    }()
    
    /// A `ConnectionFactory` that creates unencrypted connections.
    static let unencryptedConnectionFactory: ConnectionFactory = {
        let factory = try! createConnectionFactory()
        factory.ssl = false
        return factory
    }()
    
    /// Creates a connection for Terry, authenticating by `Credential.trust`.
    ///
    /// - Parameters:
    ///   - ssl: whether to encrypt the connection using SSL/TLS
    ///   - delegate: the delegate for the connection, or nil for none
    ///
    /// - Returns: the connection
    func terryConnection(ssl: Bool = true,
                         delegate: ConnectionDelegate? = nil) async throws -> Connection {
        
        let environment = TestEnvironment.current
        
        let connectionFactory = ssl ?
            Self.encryptedConnectionFactory : Self.unencryptedConnectionFactory
        
        return try await connectionFactory.connect(user: environment.terryUsername,
                                                   credential: .trust,
                                                   delegate: delegate)
    }

    /// Creates a connection for Charlie, authenticating by `Credential.cleartextPassword`.
    ///
    /// - Parameters:
    ///   - ssl: whether to encrypt the connection using SSL/TLS
    ///   - delegate: the delegate for the connection, or nil for none
    ///
    /// - Returns: the connection
    func charlieConnection(ssl: Bool = true,
                           delegate: ConnectionDelegate? = nil) async throws -> Connection {
        
        let environment = TestEnvironment.current
        
        let connectionFactory = ssl ?
            Self.encryptedConnectionFactory : Self.unencryptedConnectionFactory
        
        return try await connectionFactory.connect(
            user: environment.charlieUsername,
            credential: .cleartextPassword(password: environment.charliePassword),
            delegate: delegate)
    }
    
    /// Creates a connection for Mary, authenticating by `Credential.md5Password`.
    ///
    /// - Parameters:
    ///   - ssl: whether to encrypt the connection using SSL/TLS
    ///   - delegate: the delegate for the connection, or nil for none
    ///
    /// - Returns: the connection
    func maryConnection(ssl: Bool = true,
                           delegate: ConnectionDelegate? = nil) async throws -> Connection {
        
        let environment = TestEnvironment.current
        
        let connectionFactory = ssl ?
            Self.encryptedConnectionFactory : Self.unencryptedConnectionFactory
        
        return try await connectionFactory.connect(
            user: environment.maryUsername,
            credential: .md5Password(password: environment.maryPassword),
            delegate: delegate)
    }
    
    /// Creates a connection for Sally, authenticating by `Credential.scramSHA256`.
    ///
    /// - Parameters:
    ///   - ssl: whether to encrypt the connection using SSL/TLS
    ///   - delegate: the delegate for the connection, or nil for none
    ///
    /// - Returns: the connection
    func sallyConnection(ssl: Bool = true,
                         delegate: ConnectionDelegate? = nil) async throws -> Connection {
        
        let environment = TestEnvironment.current
        
        let connectionFactory = ssl ?
            Self.encryptedConnectionFactory : Self.unencryptedConnectionFactory
        
        return try await connectionFactory.connect(
            user: environment.sallyUsername,
            credential: .scramSHA256(password: environment.sallyPassword),
            delegate: delegate)
    }


    //
    // MARK: Test data
    //
    
    /// Creates (or re-creates) the `weather` table from the Postgres tutorial and populates it
    /// with three rows.
    ///
    /// - SeeAlso: https://www.postgresql.org/docs/12/tutorial-table.html
    /// - SeeAlso: https://www.postgresql.org/docs/12/tutorial-populate.html
    func createWeatherTable() async throws {
        
        let connection = try await terryConnection()
        defer { connection.closeAbruptly() }
        
        var statement = try connection.prepareStatement(text: "DROP TABLE IF EXISTS weather CASCADE")
        defer { statement.close() }
        try statement.execute()
        
        statement = try connection.prepareStatement(text: """
            CREATE TABLE weather (
                city            varchar(80),
                temp_lo         int,           -- low temperature
                temp_hi         int,           -- high temperature
                prcp            real,          -- precipitation
                date            date)
            """)
        defer { statement.close() }
        try statement.execute()
        
        statement = try connection.prepareStatement(text:
            "INSERT INTO weather (city, temp_lo, temp_hi, prcp, date) VALUES ($1, $2, $3, $4, $5)")
        defer { statement.close() }
        try statement.execute(parameterValues: [ "San Francisco", 46, 50, 0.25, "1994-11-27" ])
        try statement.execute(parameterValues: [ "San Francisco", 43, 57, 0.0, "1994-11-29" ])
        try statement.execute(parameterValues: [ "Hayward", 37, 54, nil, "1994-11-29" ])
    }
    

    //
    // MARK: Assertions
    //
    
    
    /// Asserts two values are equal.
    func XCTAssertEqualAsync<T>(
        _ expression1: @escaping @autoclosure () async throws -> T,
        _ expression2: @escaping @autoclosure () async throws -> T,
        _ message: @escaping @autoclosure () -> String = "",
        file: StaticString = #filePath,
        line: UInt = #line
    ) async throws where T : Equatable {
        let value1: T = try await expression1()
        let value2: T = try await expression2()
        XCTAssertEqual(value1, value2, message(), file: file, line: line)
    }
    
    /// Asserts that an expression throws an error.
    func XCTAssertThrowsErrorAsync<T>(
        _ expression: @autoclosure () async throws -> T,
        _ message: @autoclosure () -> String = "",
        file: StaticString = #filePath,
        line: UInt = #line,
        _ errorHandler: (_ error: Error) -> Void = { _ in }
    ) async {
        do {
            let value = try await expression()
            XCTAssertThrowsError(value, message(), file: file, line: line, errorHandler)
        } catch {
            XCTAssertThrowsError(try { throw error }(), message(), file: file, line: line, errorHandler)
        }
    }
    
    /// Asserts that an expression doesn't throw an error.
    func XCTAssertNoThrowAsync<T>(
        _ expression: @autoclosure () async throws -> T,
        _ message: @autoclosure () -> String = "",
        file: StaticString = #filePath,
        line: UInt = #line
    ) async {
        do {
            _ = try await expression()
        } catch {
            XCTAssertNoThrow(try { throw error }(), message(), file: file, line: line)
        }
    }
    
    /// Asserts two values are either both `nil` or both non-`nil`.
    func XCTAssertBothNilOrBothNotNil<T>(_ value1: T?, _ value2: T?,
                                         _ message: String = "XCTAssertBothNilOrBothNotNil",
                                         file: StaticString = #file, line: UInt = #line) {
        XCTAssert(
            (value1 == nil && value2 == nil) ||
            (value1 != nil && value2 != nil),
            "\(message): \(String(describing: value1)) and \(String(describing: value2))",
            file: file, line: line)
    }
    
    /// Two `Date` instances are "approximately equal" if their `timeSinceReferenceDate` values,
    /// rounded to millisecond precision, are equal.
    ///
    /// The PostgresClientKit tests use this definition for two reasons:
    ///
    /// - `DateFormatter` retains only millisecond precision (truncating additional digits in
    ///   converting strings to dates, and rounding in converting from dates to string).
    ///
    /// - Because `Date` is implemented on a `Double`, lossless conversion between `Date`
    ///   and `DateComponents` (whose `nanoseconds` property is an `Int`) is not possible for
    ///   some date values.
    @nonobjc func XCTAssertApproximatelyEqual(_ date1: Date, _ date2: Date,
                                              _ message: String = "XCTAssertApproximatelyEqual",
                                              file: StaticString = #file, line: UInt = #line) {
        
        let milliseconds1 = (date1.timeIntervalSinceReferenceDate * 1000.0).rounded()
        let milliseconds2 = (date2.timeIntervalSinceReferenceDate * 1000.0).rounded()
        
        XCTAssertEqual(
            milliseconds1, milliseconds2,
            "\(message): \(date1) and \(date2)",
            file: file, line: line)
    }
    
    /// Two `DateComponent` instances are "approximately equal" if each of the following conditions
    /// are met:
    ///
    /// - their `calendar`, `timeZone`, and `era` properties are equal
    ///
    /// - the properties for their other components are either both `nil` or both non-`nil`
    ///
    /// - calling `Calendar.date(from:)` on them produces two `Date` instances that are themselves
    ///   "approximately equal"
    @nonobjc func XCTAssertApproximatelyEqual(_ dc1: DateComponents,
                                              _ dc2: DateComponents,
                                              file: StaticString = #file, line: UInt = #line) {
        
        XCTAssertEqual(
            dc1.calendar, dc2.calendar,
            "DateComponents.calendar",
            file: file, line: line)
        
        XCTAssertEqual(
            dc1.timeZone, dc2.timeZone,
            "DateComponents.timeZone",
            file: file, line: line)
        
        XCTAssertEqual(
            dc1.era, dc2.era,
            "DateComponents.era",
            file: file, line: line)
        
        XCTAssertBothNilOrBothNotNil(
            dc1.year, dc2.year,
            "DateComponents.year",
            file: file, line: line)
        
        XCTAssertBothNilOrBothNotNil(
            dc1.yearForWeekOfYear, dc2.yearForWeekOfYear,
            "DateComponents.yearForWeekOfYear",
            file: file, line: line)
        
        XCTAssertBothNilOrBothNotNil(
            dc1.quarter, dc2.quarter,
            "DateComponents.quarter",
            file: file, line: line)
        
        XCTAssertBothNilOrBothNotNil(
            dc1.month, dc2.month,
            "DateComponents.month",
            file: file, line: line)
        
        XCTAssertBothNilOrBothNotNil(
            dc1.weekOfMonth, dc2.weekOfMonth,
            "DateComponents.weekOfMonth",
            file: file, line: line)
        
        XCTAssertBothNilOrBothNotNil(
            dc1.weekOfYear, dc2.weekOfYear,
            "DateComponents.weekOfYear",
            file: file, line: line)
        
        XCTAssertBothNilOrBothNotNil(
            dc1.weekday, dc2.weekday,
            "DateComponents.weekday",
            file: file, line: line)
        
        XCTAssertBothNilOrBothNotNil(
            dc1.weekdayOrdinal, dc2.weekdayOrdinal,
            "DateComponents.weekdayOrdinal",
            file: file, line: line)
        
        XCTAssertBothNilOrBothNotNil(
            dc1.day, dc2.day,
            "DateComponents.day",
            file: file, line: line)
        
        XCTAssertBothNilOrBothNotNil(
            dc1.hour, dc2.hour,
            "DateComponents.hour",
            file: file, line: line)
        
        XCTAssertBothNilOrBothNotNil(
            dc1.minute, dc2.minute,
            "DateComponents.minute",
            file: file, line: line)
        
        XCTAssertBothNilOrBothNotNil(
            dc1.second, dc2.second,
            "DateComponents.second",
            file: file, line: line)
        
        XCTAssertBothNilOrBothNotNil(
            dc1.nanosecond, dc2.nanosecond,
            "DateComponents.nanosecond",
            file: file, line: line)
        
        let date1 = enUsPosixUtcCalendar.date(from: dc1)
        let date2 = enUsPosixUtcCalendar.date(from: dc2)
        
        if let date1 = date1, let date2 = date2 {
            XCTAssertApproximatelyEqual(date1, date2, "DateComponents", file: file, line: line)
        } else {
            XCTAssertBothNilOrBothNotNil(date1, date2, "DateComponents", file: file, line: line)
        }
    }
    
    
    //
    // MARK: Expectations // FIXME: revisit async testing
    //
    
    /// Sets an expectation.
    ///
    /// - Parameter description: describes the expectation
    /// - Returns: the expectation
    func expect(_ description: String) -> XCTestExpectation {
        return expectation(description: description)
    }
    
    /// Waits up to 2.0 seconds for all expectations to be fulfilled.
    func waitForExpectations(_ expectations: [XCTestExpectation]) async {
        await fulfillment(of: expectations, timeout: 2.0)
    }
}

// EOF
