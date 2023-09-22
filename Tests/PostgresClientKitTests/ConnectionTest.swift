//
//  ConnectionTest.swift
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
import XCTest

/// Tests Connection.
class ConnectionTest: PostgresClientKitTestCase {
    
    func testCreateConnection() async throws {
        
        let environment = TestEnvironment.current
        
        // Network error
        var connectionFactory = try Self.createConnectionFactory()
        connectionFactory.host = "256.0.0.0"
        await XCTAssertThrowsErrorAsync(
            try await connectionFactory.connect(
                user: environment.terryUsername, credential: .trust)
        ) { error in
            guard case PostgresError.socketError = error else {
                return XCTFail(String(describing: error))
            }
        }
        
        // Non-SSL
        connectionFactory = try Self.createConnectionFactory()
        connectionFactory.ssl = false
        await XCTAssertNoThrowAsync(
            try await connectionFactory.connect(
                user: environment.terryUsername, credential: .trust
            ).close())
        
        // SSL
        connectionFactory = try Self.createConnectionFactory()
        connectionFactory.ssl = true // (the default)
        await XCTAssertNoThrowAsync(
            try await connectionFactory.connect(
                user: environment.terryUsername, credential: .trust
            ).close())

        // Authenticate: trust required, trust supplied
        await XCTAssertNoThrowAsync(
            try await Self.encryptedConnectionFactory.connect(
                user: environment.terryUsername,
                credential: .trust
            ).close())

        // Authenticate: trust required, cleartextPassword supplied
        await XCTAssertThrowsErrorAsync(
            try await Self.encryptedConnectionFactory.connect(
                user: environment.terryUsername,
                credential: .cleartextPassword(password: "wrong-credential-type"))
        ) { error in
            guard case PostgresError.trustCredentialRequired = error else {
                return XCTFail(String(describing: error))
            }
        }
        
        // Authenticate: trust required, md5Password supplied
        await XCTAssertThrowsErrorAsync(
            try await Self.encryptedConnectionFactory.connect(
                user: environment.terryUsername,
                credential: .md5Password(password: "wrong-credential-type"))
        ) { error in
            guard case PostgresError.trustCredentialRequired = error else {
                return XCTFail(String(describing: error))
            }
        }

        // Authenticate: trust required, scramSHA256 supplied
        await XCTAssertThrowsErrorAsync(
            try await Self.encryptedConnectionFactory.connect(
                user: environment.terryUsername,
                credential: .scramSHA256(password: "wrong-credential-type"))
        ) { error in
            guard case PostgresError.trustCredentialRequired = error else {
                return XCTFail(String(describing: error))
            }
        }

        // Authenticate: cleartextPassword required, trust supplied
        await XCTAssertThrowsErrorAsync(
            try await Self.encryptedConnectionFactory.connect(
                user: environment.charlieUsername,
                credential: .trust)
        ) { error in
            guard case PostgresError.cleartextPasswordCredentialRequired = error else {
                return XCTFail(String(describing: error))
            }
        }
        
        // Authenticate: cleartextPassword required, cleartextPassword supplied
        await XCTAssertNoThrowAsync(
            try await Self.encryptedConnectionFactory.connect(
                user: environment.charlieUsername,
                credential: .cleartextPassword(password: environment.charliePassword)
            ).close())
        
        // Authenticate: cleartextPassword required, cleartextPassword supplied, incorrect password
        await XCTAssertThrowsErrorAsync(
            try await Self.encryptedConnectionFactory.connect(
                user: environment.charlieUsername,
                credential: .cleartextPassword(password: "wrong-password"))
        ) { error in
            guard case PostgresError.sqlError = error else {
                return XCTFail(String(describing: error))
            }
        }
        
        // Authenticate: cleartextPassword required, md5Password supplied
        await XCTAssertThrowsErrorAsync(
            try await Self.encryptedConnectionFactory.connect(
                user: environment.charlieUsername,
                credential: .md5Password(password: "wrong-credential-type"))
        ) { error in
            guard case PostgresError.cleartextPasswordCredentialRequired = error else {
                return XCTFail(String(describing: error))
            }
        }
        
        // Authenticate: cleartextPassword required, scramSHA256 supplied
        await XCTAssertThrowsErrorAsync(
            try await Self.encryptedConnectionFactory.connect(
                user: environment.charlieUsername,
                credential: .scramSHA256(password: "wrong-credential-type"))
        ) { error in
            guard case PostgresError.cleartextPasswordCredentialRequired = error else {
                return XCTFail(String(describing: error))
            }
        }

        // Authenticate: md5Password required, trust supplied
        await XCTAssertThrowsErrorAsync(
            try await Self.encryptedConnectionFactory.connect(
                user: environment.maryUsername,
                credential: .trust)
        ) { error in
            guard case PostgresError.md5PasswordCredentialRequired = error else {
                return XCTFail(String(describing: error))
            }
        }
        
        // Authenticate: md5Password required, cleartextPassword supplied
        await XCTAssertThrowsErrorAsync(
            try await Self.encryptedConnectionFactory.connect(
                user: environment.maryUsername,
                credential: .cleartextPassword(password: "wrong-credential-type"))
        ) { error in
            guard case PostgresError.md5PasswordCredentialRequired = error else {
                return XCTFail(String(describing: error))
            }
        }
        
        // Authenticate: md5Password required, md5Password supplied
        await XCTAssertNoThrowAsync(
            try await Self.encryptedConnectionFactory.connect(
                user: environment.maryUsername,
                credential: .md5Password(password: environment.maryPassword)
            ).close())
        
        // Authenticate: md5Password required, md5Password supplied, incorrect password
        await XCTAssertThrowsErrorAsync(
            try await Self.encryptedConnectionFactory.connect(
                user: environment.maryUsername,
                credential: .md5Password(password: "wrong-password"))
        ) { error in
            guard case PostgresError.sqlError = error else {
                return XCTFail(String(describing: error))
            }
        }
        
        // Authenticate: md5Password required, scramSHA256 supplied
        await XCTAssertThrowsErrorAsync(
            try await Self.encryptedConnectionFactory.connect(
                user: environment.maryUsername,
                credential: .scramSHA256(password: "wrong-credential-type"))
        ) { error in
            guard case PostgresError.md5PasswordCredentialRequired = error else {
                return XCTFail(String(describing: error))
            }
        }
        
        // Authenticate: scramSHA256 required, trust supplied
        await XCTAssertThrowsErrorAsync(
            try await Self.encryptedConnectionFactory.connect(
                user: environment.sallyUsername,
                credential: .trust)
        ) { error in
            guard case PostgresError.scramSHA256CredentialRequired = error else {
                return XCTFail(String(describing: error))
            }
        }
        
        // Authenticate: scramSHA256 required, cleartextPassword supplied
        await XCTAssertThrowsErrorAsync(
            try await Self.encryptedConnectionFactory.connect(
                user: environment.sallyUsername,
                credential: .cleartextPassword(password: "wrong-credential-type"))
        ) { error in
            guard case PostgresError.scramSHA256CredentialRequired = error else {
                return XCTFail(String(describing: error))
            }
        }
        
        // Authenticate: scramSHA256 required, md5Password supplied
        await XCTAssertThrowsErrorAsync(
            try await Self.encryptedConnectionFactory.connect(
                user: environment.sallyUsername,
                credential: .md5Password(password: "wrong-credential-type"))
        ) { error in
            guard case PostgresError.scramSHA256CredentialRequired = error else {
                return XCTFail(String(describing: error))
            }
        }

        // Authenticate: scramSHA256 required, scramSHA256 supplied
        await XCTAssertNoThrowAsync(
            try await Self.encryptedConnectionFactory.connect(
                user: environment.sallyUsername,
                credential: .scramSHA256(password: environment.sallyPassword)
            ).close())
        
        // Authenticate: scramSHA256 required, scramSHA256 supplied, incorrect password
        await XCTAssertThrowsErrorAsync(
            try await Self.encryptedConnectionFactory.connect(
                user: environment.sallyUsername,
                credential: .scramSHA256(password: "wrong-password"))
        ) { error in
            guard case PostgresError.sqlError = error else {
                return XCTFail(String(describing: error))
            }
        }
    }
    
    func testApplicationName() async {
        
        do {
            let applicationName = "Test-\(Int.random(in: Int.min...Int.max))"
            
            let connectionFactory = try Self.createConnectionFactory()
            connectionFactory.applicationName = applicationName
            
            let connection = try await connectionFactory.connect(
                user: TestEnvironment.current.terryUsername, credential: .trust)
            
            let text = "SELECT COUNT(*) FROM pg_stat_activity WHERE application_name = $1"
            let statement = try connection.prepareStatement(text: text)
            let cursor = try statement.execute(parameterValues: [ applicationName ])
            let firstRow = try cursor.next()!.get()
            let count = try firstRow.columns[0].int()
            XCTAssertEqual(count, 1)
        } catch {
            XCTFail(String(describing: error))
        }
    }
    
    func testConnectionLifecycle() async {
        
        do {
            let connection1 = try await maryConnection()
            let connection2 = try await maryConnection()
            
            // Each connection has a unique id
            XCTAssertNotEqual(connection1.id, connection2.id)
            
            // The description property is the id value
            XCTAssertEqual(connection1.id, connection1.description)
            
            // No delegate by default
            XCTAssertNil(connection1.delegate)
            
            // Connections are initially open
            XCTAssertFalse(connection1.isClosed)
            XCTAssertFalse(connection2.isClosed)
            
            // Connections can be independently closed
            await connection1.close()
            XCTAssertTrue(connection1.isClosed)
            XCTAssertFalse(connection2.isClosed)
            
            // close() is idempotent
            await connection1.close()
            XCTAssertTrue(connection1.isClosed)
            XCTAssertFalse(connection2.isClosed)
            
            await connection2.close()
            XCTAssertTrue(connection1.isClosed)
            XCTAssertTrue(connection2.isClosed)
            
            // closeAbruptly() forces the connection to close.
            let connection3 = try await maryConnection()
            connection3.closeAbruptly()
            XCTAssertTrue(connection3.isClosed)
            connection3.closeAbruptly()
            XCTAssertTrue(connection3.isClosed)
            await connection3.close()
            XCTAssertTrue(connection3.isClosed)
        } catch {
            XCTFail(String(describing: error))
        }
    }
    
    func testTransactions() async {
        
        do {
            func countWeatherRows(_ connection: Connection) throws -> Int {
                
                let text = "SELECT COUNT(*) FROM weather"
                let statement = try connection.prepareStatement(text: text)
                let cursor = try statement.execute()
                let firstRow = try cursor.next()!.get()
                let count = try firstRow.columns[0].int()
                
                return count
            }
            
            func resetTestData(_ connection: Connection) async throws {
                
                try await createWeatherTable()
                
                let statement = try connection.prepareStatement(text: """
                    CREATE OR REPLACE FUNCTION testWeather(deleteCity VARCHAR, selectDate VARCHAR)
                        RETURNS SETOF weather
                        LANGUAGE SQL
                    AS $$
                        DELETE FROM weather WHERE city = deleteCity;
                        SELECT * FROM weather WHERE date = CAST(selectDate AS DATE);
                    $$;
                    """)
                
                try statement.execute()
            }
            
            let performer = try await terryConnection()
            let observer = try await terryConnection()
            
            
            //
            // Implicit transactions
            //
            
            // If there are no rows in the result, the transaction is implicitly committed upon
            // successful completion of Statement.execute.
            do {
                try await resetTestData(performer)
                XCTAssertEqual(try countWeatherRows(observer), 3)
                let text = "DELETE FROM weather"
                let statement = try performer.prepareStatement(text: text)
                let cursor = try statement.execute()
                XCTAssertEqual(try countWeatherRows(observer), 0)
                cursor.close()
                XCTAssertEqual(try countWeatherRows(observer), 0)
            }
            
            // If there are no rows in the result, the transaction is implicitly committed upon
            // successful completion of Statement.execute
            do {
                try await resetTestData(performer)
                XCTAssertEqual(try countWeatherRows(observer), 3)
                let text = "SELECT * FROM testWeather($1, $2)"
                let statement = try performer.prepareStatement(text: text)
                let cursor = try statement.execute(
                    parameterValues: [ "Hayward", "2000-01-01" ]) // delete 1 row, return 0 rows
                XCTAssertEqual(try countWeatherRows(observer), 2)
                for row in cursor { _ = try row.get() } // retrieve all rows
                XCTAssertEqual(try countWeatherRows(observer), 2)
                cursor.close()
                XCTAssertEqual(try countWeatherRows(observer), 2)
            }
            
            // If there are one or more rows in the result, the transaction is implicitly committed
            // after the final row has been retrieved
            do {
                try await resetTestData(performer)
                XCTAssertEqual(try countWeatherRows(observer), 3)
                let text = "SELECT * FROM testWeather($1, $2)"
                let statement = try performer.prepareStatement(text: text)
                let cursor = try statement.execute(
                    parameterValues: [ "Hayward", "1994-11-29" ]) // delete 1 row, return 1 row
                XCTAssertEqual(try countWeatherRows(observer), 3)
                for row in cursor { _ = try row.get() } // retrieve all rows
                XCTAssertEqual(try countWeatherRows(observer), 2)
                cursor.close()
                XCTAssertEqual(try countWeatherRows(observer), 2)
            }
            
            // If there are one or more rows in the result, the transaction is also implicitly
            // committed when the cursor is closed
            do {
                try await resetTestData(performer)
                XCTAssertEqual(try countWeatherRows(observer), 3)
                let text = "SELECT * FROM testWeather($1, $2)"
                let statement = try performer.prepareStatement(text: text)
                let cursor = try statement.execute(
                    parameterValues: [ "Hayward", "1994-11-29" ]) // delete 1 row, return 1 row
                XCTAssertEqual(try countWeatherRows(observer), 3)
                cursor.close()
                XCTAssertEqual(try countWeatherRows(observer), 2)
            }

            // If the statement fails, it is implicitly rolled back
            do {
                try await resetTestData(performer)
                XCTAssertEqual(try countWeatherRows(observer), 3)
                let text = "SELECT * FROM testWeather($1, $2)"
                let statement = try performer.prepareStatement(text: text)
                let operation = { try statement.execute(
                    parameterValues: [ "Hayward", "invalid-date" ]) } // delete 1 row, then fail
                
                XCTAssertThrowsError(try operation()) { error in
                    guard case PostgresError.sqlError = error else {
                        return XCTFail(String(describing: error))
                    }
                }
                
                XCTAssertEqual(try countWeatherRows(observer), 3)
            }
            
            
            //
            // Explicit transactions
            //
            
            // beginTransaction() closes any open cursor
            do {
                let text = "SELECT * FROM weather"
                let statement = try performer.prepareStatement(text: text)
                let cursor = try statement.execute()
                XCTAssertFalse(cursor.isClosed)
                try performer.beginTransaction()
                XCTAssertTrue(cursor.isClosed)
                try performer.rollbackTransaction()
            }
            
            // commitTransaction() closes any open cursor
            do {
                let text = "SELECT * FROM weather"
                let statement = try performer.prepareStatement(text: text)
                let cursor = try statement.execute()
                XCTAssertFalse(cursor.isClosed)
                try performer.commitTransaction()
                XCTAssertTrue(cursor.isClosed)
            }
            
            // rollbackTransaction() closes any open cursor
            do {
                let text = "SELECT * FROM weather"
                let statement = try performer.prepareStatement(text: text)
                let cursor = try statement.execute()
                XCTAssertFalse(cursor.isClosed)
                try performer.rollbackTransaction()
                XCTAssertTrue(cursor.isClosed)
            }
            
            // beginTransaction() + commitTransaction()
            do {
                try await resetTestData(performer)
                XCTAssertEqual(try countWeatherRows(performer), 3)
                XCTAssertEqual(try countWeatherRows(observer), 3)
                try performer.beginTransaction()
                let text = "DELETE FROM weather"
                let statement = try performer.prepareStatement(text: text)
                let cursor = try statement.execute()
                XCTAssertEqual(try countWeatherRows(performer), 0)
                XCTAssertEqual(try countWeatherRows(observer), 3)
                cursor.close()
                statement.close()
                XCTAssertEqual(try countWeatherRows(performer), 0)
                XCTAssertEqual(try countWeatherRows(observer), 3)
                try performer.commitTransaction()
                XCTAssertEqual(try countWeatherRows(performer), 0)
                XCTAssertEqual(try countWeatherRows(observer), 0)
            }
            
            // beginTransaction() + rollbackTransaction()
            do {
                try await resetTestData(performer)
                XCTAssertEqual(try countWeatherRows(performer), 3)
                XCTAssertEqual(try countWeatherRows(observer), 3)
                try performer.beginTransaction()
                let text = "DELETE FROM weather"
                let statement = try performer.prepareStatement(text: text)
                let cursor = try statement.execute()
                XCTAssertEqual(try countWeatherRows(performer), 0)
                XCTAssertEqual(try countWeatherRows(observer), 3)
                cursor.close()
                statement.close()
                XCTAssertEqual(try countWeatherRows(performer), 0)
                XCTAssertEqual(try countWeatherRows(observer), 3)
                try performer.rollbackTransaction()
                XCTAssertEqual(try countWeatherRows(performer), 3)
                XCTAssertEqual(try countWeatherRows(observer), 3)
            }
            
            // Closing a connection rolls back any explicit transaction
            do {
                try await resetTestData(performer)
                XCTAssertEqual(try countWeatherRows(performer), 3)
                XCTAssertEqual(try countWeatherRows(observer), 3)
                try performer.beginTransaction()
                let text = "DELETE FROM weather"
                let statement = try performer.prepareStatement(text: text)
                let cursor = try statement.execute()
                XCTAssertEqual(try countWeatherRows(performer), 0)
                XCTAssertEqual(try countWeatherRows(observer), 3)
                cursor.close()
                statement.close()
                XCTAssertEqual(try countWeatherRows(performer), 0)
                XCTAssertEqual(try countWeatherRows(observer), 3)
                await performer.close()
                XCTAssertEqual(try countWeatherRows(observer), 3)
            }
        } catch {
            XCTFail(String(describing: error))
        }
    }
    
    func testErrorRecovery() async {
        do {
            let connection = try await terryConnection()
            var text = "invalid-text"

            XCTAssertThrowsError(try connection.prepareStatement(text: text)) { error in
                guard case PostgresError.sqlError = error else {
                    return XCTFail(String(describing: error))
                }
            }
            
            XCTAssertFalse(connection.isClosed)
            
            text = "SELECT $1"
            let statement = try connection.prepareStatement(text: text)
            
            XCTAssertThrowsError(try statement.execute()) { error in
                guard case PostgresError.sqlError = error else {
                    return XCTFail(String(describing: error))
                }
            }
            
            XCTAssertFalse(connection.isClosed)

            let cursor = try statement.execute(parameterValues: [ 123 ])
            let row = cursor.next()
            XCTAssertNotNil(row)
            XCTAssertEqual(try row?.get().columns[0].int(), 123)
            
            XCTAssertFalse(connection.isClosed)

            await connection.close()
            XCTAssertTrue(connection.isClosed)
        } catch {
            XCTFail(String(describing: error))
        }
    }
}

// EOF
