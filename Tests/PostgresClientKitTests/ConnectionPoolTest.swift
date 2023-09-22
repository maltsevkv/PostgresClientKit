//
//  ConnectionPoolTest.swift
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

/// Tests ConnectionPool.
class ConnectionPoolTest: PostgresClientKitTestCase {
    
    func test() async {
        
        //
        // Initializer, public properties
        //
        
        do {
            class Delegate: ConnectionDelegate { }
            
            let connectionPoolConfiguration = ConnectionPoolConfiguration()
            let connectionDelegate = Delegate()
            
            let connectionPool = ConnectionPool(
                connectionPoolConfiguration: connectionPoolConfiguration,
                connectionFactory: Self.encryptedConnectionFactory,
                user: TestEnvironment.current.terryUsername,
                credential: .trust,
                connectionDelegate: connectionDelegate)
            
            XCTAssertEqual(connectionPool.connectionPoolConfiguration, connectionPoolConfiguration)
// FIXME            XCTAssertEqual(connectionPool.connectionConfiguration, connectionConfiguration)
            XCTAssertTrue(connectionPool.connectionDelegate === connectionDelegate)

            connectionPool.checkMetrics(successfulRequests: 0,
                                        unsuccessfulRequestsTooBusy: 0,
                                        unsuccessfulRequestsTimedOut: 0,
                                        unsuccessfulRequestsError: 0,
                                        minimumPendingRequests: 0,
                                        maximumPendingRequests: 0,
                                        connectionsAtStartOfPeriod: 0,
                                        connectionsAtEndOfPeriod: 0,
                                        connectionsCreated: 0,
                                        allocatedConnectionsClosedByRequestor: 0,
                                        allocatedConnectionsTimedOut: 0)
            
            let connection = await acquireConnections(connectionPool: connectionPool, count: 1).first!
            XCTAssertTrue(connection.delegate === connectionDelegate)
        }

        
        //
        // Mutation of connectionPoolConfiguration
        //
        
        await withConnectionPool { connectionPool in
            
            XCTAssertEqual(connectionPool.connectionPoolConfiguration.metricsLoggingInterval, 3600)
            
            connectionPool.connectionPoolConfiguration.metricsLoggingInterval = nil
            XCTAssertNil(connectionPool.connectionPoolConfiguration.metricsLoggingInterval)
            
            await acquireConnections(connectionPool: connectionPool, count: 3)
            
            connectionPool.checkMetrics(successfulRequests: 3,
                                        unsuccessfulRequestsTooBusy: 0,
                                        unsuccessfulRequestsTimedOut: 0,
                                        unsuccessfulRequestsError: 0,
                                        minimumPendingRequests: 0,
                                        maximumPendingRequests: 1,
                                        connectionsAtStartOfPeriod: 0,
                                        connectionsAtEndOfPeriod: 3,
                                        connectionsCreated: 3,
                                        allocatedConnectionsClosedByRequestor: 0,
                                        allocatedConnectionsTimedOut: 0)
            
            connectionPool.connectionPoolConfiguration.metricsLoggingInterval = 1
            XCTAssertEqual(connectionPool.connectionPoolConfiguration.metricsLoggingInterval, 1)
            
            // Wait for the metrics be logged (and reset!).
            try await Task.sleep(for: .seconds(1.1))
            
            // Check that the reset took place.
            connectionPool.checkMetrics(successfulRequests: 0,
                                        unsuccessfulRequestsTooBusy: 0,
                                        unsuccessfulRequestsTimedOut: 0,
                                        unsuccessfulRequestsError: 0,
                                        minimumPendingRequests: 0,
                                        maximumPendingRequests: 0,
                                        connectionsAtStartOfPeriod: 3,
                                        connectionsAtEndOfPeriod: 3,
                                        connectionsCreated: 0,
                                        allocatedConnectionsClosedByRequestor: 0,
                                        allocatedConnectionsTimedOut: 0)
        }
        
        
        //
        // acquireConnection: success
        //
        
        await withConnectionPool { connectionPool in
            
            let expectation = expect("acquireConnection: success")
            
            connectionPool.acquireConnection { result in
                do {
                    let connection = try result.get()
                    connectionPool.releaseConnection(connection)
                } catch {
                    XCTFail(String(describing: error))
                }
                
                expectation.fulfill()
            }
            
            await waitForExpectations([expectation])
            
            connectionPool.checkMetrics(successfulRequests: 1,
                                        unsuccessfulRequestsTooBusy: 0,
                                        unsuccessfulRequestsTimedOut: 0,
                                        unsuccessfulRequestsError: 0,
                                        minimumPendingRequests: 0,
                                        maximumPendingRequests: 1,
                                        connectionsAtStartOfPeriod: 0,
                                        connectionsAtEndOfPeriod: 1,
                                        connectionsCreated: 1,
                                        allocatedConnectionsClosedByRequestor: 0,
                                        allocatedConnectionsTimedOut: 0)
        }


        //
        // acquireConnection: connection pool closed
        //

        await withConnectionPool { connectionPool in
            
            connectionPool.close()
            
            let expectation = expect("acquireConnection: connection pool closed")

            connectionPool.acquireConnection { result in
                guard case .failure(PostgresError.connectionPoolClosed) = result else {
                    return XCTFail("\(result)")
                }
                
                expectation.fulfill()
            }

            await waitForExpectations([expectation])
            
            connectionPool.checkMetrics(successfulRequests: 0,
                                        unsuccessfulRequestsTooBusy: 0,
                                        unsuccessfulRequestsTimedOut: 0,
                                        unsuccessfulRequestsError: 0,
                                        minimumPendingRequests: 0,
                                        maximumPendingRequests: 0,
                                        connectionsAtStartOfPeriod: 0,
                                        connectionsAtEndOfPeriod: 0,
                                        connectionsCreated: 0,
                                        allocatedConnectionsClosedByRequestor: 0,
                                        allocatedConnectionsTimedOut: 0)
        }
        
        
        //
        // acquireConnection: too many pending requests
        //
        
        await withConnectionPool { connectionPool in
            
            connectionPool.connectionPoolConfiguration.maximumConnections = 5
            await acquireConnections(connectionPool: connectionPool, count: 5)
            
            connectionPool.connectionPoolConfiguration.maximumPendingRequests = 2
            requestConnections(connectionPool: connectionPool, count: 2)
            
            let expectation = expect("acquireConnection: too many pending requests")
            
            connectionPool.acquireConnection { result in
                guard case .failure(PostgresError.tooManyRequestsForConnections) = result else {
                    return XCTFail("\(result)")
                }
                
                expectation.fulfill()
            }
            
            await waitForExpectations([expectation])
            
            connectionPool.checkMetrics(successfulRequests: 5,
                                        unsuccessfulRequestsTooBusy: 1,
                                        unsuccessfulRequestsTimedOut: 0,
                                        unsuccessfulRequestsError: 0,
                                        minimumPendingRequests: 0,
                                        maximumPendingRequests: 2,
                                        connectionsAtStartOfPeriod: 0,
                                        connectionsAtEndOfPeriod: 5,
                                        connectionsCreated: 5,
                                        allocatedConnectionsClosedByRequestor: 0,
                                        allocatedConnectionsTimedOut: 0)
        }
        
        
        //
        // acquireConnection: request timed out
        //
        
        await withConnectionPool { connectionPool in
            
            connectionPool.connectionPoolConfiguration.maximumConnections = 5
            await acquireConnections(connectionPool: connectionPool, count: 5)
            
            connectionPool.connectionPoolConfiguration.pendingRequestTimeout = 1
            
            let expectation = expect("acquireConnection: request timed out")
            
            connectionPool.acquireConnection { result in
                guard case .failure(PostgresError.timedOutAcquiringConnection) = result else {
                    return XCTFail("\(result)")
                }
                
                expectation.fulfill()
            }
            
            await waitForExpectations([expectation])
            
            connectionPool.checkMetrics(successfulRequests: 5,
                                        unsuccessfulRequestsTooBusy: 0,
                                        unsuccessfulRequestsTimedOut: 1,
                                        unsuccessfulRequestsError: 0,
                                        minimumPendingRequests: 0,
                                        maximumPendingRequests: 1,
                                        connectionsAtStartOfPeriod: 0,
                                        connectionsAtEndOfPeriod: 5,
                                        connectionsCreated: 5,
                                        allocatedConnectionsClosedByRequestor: 0,
                                        allocatedConnectionsTimedOut: 0)
        }
        
        
        //
        // acquireConnection: error
        //
        
        await withConnectionPool(user: "") { connectionPool in
            
            let expectation = expect("acquireConnection: error")
            
            connectionPool.acquireConnection { result in
                guard case .failure(PostgresError.sqlError) = result else {
                    return XCTFail("\(result)")
                }
                
                expectation.fulfill()
            }
            
            await waitForExpectations([expectation])
            
            connectionPool.checkMetrics(successfulRequests: 0,
                                        unsuccessfulRequestsTooBusy: 0,
                                        unsuccessfulRequestsTimedOut: 0,
                                        unsuccessfulRequestsError: 1,
                                        minimumPendingRequests: 0,
                                        maximumPendingRequests: 1,
                                        connectionsAtStartOfPeriod: 0,
                                        connectionsAtEndOfPeriod: 0,
                                        connectionsCreated: 0,
                                        allocatedConnectionsClosedByRequestor: 0,
                                        allocatedConnectionsTimedOut: 0)
        }
        
        
        //
        // acquireConnection: requests serviced in FIFO order
        //
        
        await withConnectionPool { connectionPool in
            
            connectionPool.connectionPoolConfiguration.maximumConnections = 5
            var connections = await acquireConnections(connectionPool: connectionPool, count: 5)
            
            var status = [String]()
            
            // Submit request A
            let expectationA = expect("acquireConnection: requests served in FIFO order request A")
            
            connectionPool.acquireConnection { result in
                do {
                    _ = try result.get()
                    status.append("acquired connection for request A")
                } catch {
                    XCTFail(String(describing: error))
                }
                
                expectationA.fulfill()
            }
            
            // Submit request B
            let expectationB = expect("acquireConnection: requests served in FIFO order request B")
            
            connectionPool.acquireConnection { result in
                do {
                    _ = try result.get()
                    status.append("acquired connection for request B")
                } catch {
                    XCTFail(String(describing: error))
                }
                
                expectationB.fulfill()
            }
            
            XCTAssertTrue(status.isEmpty)

            // Release a connection allowing request A to be fulfilled
            connectionPool.releaseConnection(connections.removeFirst())
            await fulfillment(of: [ expectationA ], timeout: 1.0)
            
            XCTAssertEqual(status, [ "acquired connection for request A" ])
            
            // Release a connection allowing request B to be fulfilled
            connectionPool.releaseConnection(connections.removeFirst())
            await fulfillment(of: [ expectationB ], timeout: 1.0)
            
            XCTAssertEqual(status, [ "acquired connection for request A",
                                     "acquired connection for request B"])
            
            connectionPool.checkMetrics(successfulRequests: 7,
                                        unsuccessfulRequestsTooBusy: 0,
                                        unsuccessfulRequestsTimedOut: 0,
                                        unsuccessfulRequestsError: 0,
                                        minimumPendingRequests: 0,
                                        maximumPendingRequests: 2,
                                        connectionsAtStartOfPeriod: 0,
                                        connectionsAtEndOfPeriod: 5,
                                        connectionsCreated: 5,
                                        allocatedConnectionsClosedByRequestor: 0,
                                        allocatedConnectionsTimedOut: 0)
        }
        
        
        //
        // acquireConnection: connections allocated in LRU order
        //
        
        await withConnectionPool { connectionPool in
            
            connectionPool.connectionPoolConfiguration.maximumConnections = 5
            let connections = await acquireConnections(connectionPool: connectionPool, count: 5)
            
            for connection in connections.reversed() {
                connectionPool.releaseConnection(connection)
            }
            
            let moreConnections = await acquireConnections(connectionPool: connectionPool, count: 5)
            
            XCTAssertEqual(connections.reversed().map { $0.id },
                           moreConnections.map { $0.id })
            
            connectionPool.checkMetrics(successfulRequests: 10,
                                        unsuccessfulRequestsTooBusy: 0,
                                        unsuccessfulRequestsTimedOut: 0,
                                        unsuccessfulRequestsError: 0,
                                        minimumPendingRequests: 0,
                                        maximumPendingRequests: 1,
                                        connectionsAtStartOfPeriod: 0,
                                        connectionsAtEndOfPeriod: 5,
                                        connectionsCreated: 5,
                                        allocatedConnectionsClosedByRequestor: 0,
                                        allocatedConnectionsTimedOut: 0)
        }
        
        
        //
        // releaseConnection: success
        //
        
        await withConnectionPool { connectionPool in
            
            let connection = await acquireConnections(connectionPool: connectionPool, count: 1).first!
            XCTAssertFalse(connection.isClosed)
            
            connectionPool.releaseConnection(connection)
            try await Task.sleep(for: .seconds(0.1)) // let any async socket close complete
            XCTAssertFalse(connection.isClosed)

            connectionPool.checkMetrics(successfulRequests: 1,
                                        unsuccessfulRequestsTooBusy: 0,
                                        unsuccessfulRequestsTimedOut: 0,
                                        unsuccessfulRequestsError: 0,
                                        minimumPendingRequests: 0,
                                        maximumPendingRequests: 1,
                                        connectionsAtStartOfPeriod: 0,
                                        connectionsAtEndOfPeriod: 1,
                                        connectionsCreated: 1,
                                        allocatedConnectionsClosedByRequestor: 0,
                                        allocatedConnectionsTimedOut: 0)
        }
        
        
        //
        // releaseConnection: not in pool
        //
        
        await withConnectionPool { connectionPool in
            
            let connection: Connection
                
            do {
                connection = try await terryConnection()
                XCTAssertFalse(connection.isClosed)
                
                connectionPool.releaseConnection(connection) // should log a warning
                try await Task.sleep(for: .seconds(0.1)) // let any async socket close complete
                XCTAssertTrue(connection.isClosed)
            } catch {
                return XCTFail(String(describing: error))
            }
            
            connectionPool.checkMetrics(successfulRequests: 0,
                                        unsuccessfulRequestsTooBusy: 0,
                                        unsuccessfulRequestsTimedOut: 0,
                                        unsuccessfulRequestsError: 0,
                                        minimumPendingRequests: 0,
                                        maximumPendingRequests: 0,
                                        connectionsAtStartOfPeriod: 0,
                                        connectionsAtEndOfPeriod: 0,
                                        connectionsCreated: 0,
                                        allocatedConnectionsClosedByRequestor: 0,
                                        allocatedConnectionsTimedOut: 0)
        }

        
        //
        // releaseConnection: pool non-forcibly closed
        //
        
        await withConnectionPool { connectionPool in
            
            let connection = await acquireConnections(connectionPool: connectionPool, count: 1).first!
            XCTAssertFalse(connection.isClosed)
            
            connectionPool.close(force: false)
            try await Task.sleep(for: .seconds(0.1)) // let any async socket close complete
            XCTAssertFalse(connection.isClosed)
            
            connectionPool.releaseConnection(connection)
            try await Task.sleep(for: .seconds(0.1)) // let any async socket close complete
            XCTAssertTrue(connection.isClosed)

            connectionPool.checkMetrics(successfulRequests: 1,
                                        unsuccessfulRequestsTooBusy: 0,
                                        unsuccessfulRequestsTimedOut: 0,
                                        unsuccessfulRequestsError: 0,
                                        minimumPendingRequests: 0,
                                        maximumPendingRequests: 1,
                                        connectionsAtStartOfPeriod: 0,
                                        connectionsAtEndOfPeriod: 0,
                                        connectionsCreated: 1,
                                        allocatedConnectionsClosedByRequestor: 0,
                                        allocatedConnectionsTimedOut: 0)
        }
        
        
        //
        // releaseConnection: pool forcibly closed
        //
        
        await withConnectionPool { connectionPool in
            
            let connection = await acquireConnections(connectionPool: connectionPool, count: 1).first!
            XCTAssertFalse(connection.isClosed)
            
            connectionPool.close(force: true)
            try await Task.sleep(for: .seconds(0.1)) // let any async socket close complete
            XCTAssertTrue(connection.isClosed)
            
            connectionPool.releaseConnection(connection)
            try await Task.sleep(for: .seconds(0.1)) // let any async socket close complete
            XCTAssertTrue(connection.isClosed)

            connectionPool.checkMetrics(successfulRequests: 1,
                                        unsuccessfulRequestsTooBusy: 0,
                                        unsuccessfulRequestsTimedOut: 0,
                                        unsuccessfulRequestsError: 0,
                                        minimumPendingRequests: 0,
                                        maximumPendingRequests: 1,
                                        connectionsAtStartOfPeriod: 0,
                                        connectionsAtEndOfPeriod: 0,
                                        connectionsCreated: 1,
                                        allocatedConnectionsClosedByRequestor: 0,
                                        allocatedConnectionsTimedOut: 0)
        }
        
        
        //
        // releaseConnection: allocation timed out
        //
        
        await withConnectionPool { connectionPool in
            
            connectionPool.connectionPoolConfiguration.allocatedConnectionTimeout = 1
            
            let connection = await acquireConnections(connectionPool: connectionPool, count: 1).first!
            XCTAssertFalse(connection.isClosed)

            try await Task.sleep(for: .seconds(1.1))
            XCTAssertTrue(connection.isClosed)

            connectionPool.releaseConnection(connection)
            try await Task.sleep(for: .seconds(0.1)) // let any async socket close complete
            XCTAssertTrue(connection.isClosed)
            
            connectionPool.checkMetrics(successfulRequests: 1,
                                        unsuccessfulRequestsTooBusy: 0,
                                        unsuccessfulRequestsTimedOut: 0,
                                        unsuccessfulRequestsError: 0,
                                        minimumPendingRequests: 0,
                                        maximumPendingRequests: 1,
                                        connectionsAtStartOfPeriod: 0,
                                        connectionsAtEndOfPeriod: 0,
                                        connectionsCreated: 1,
                                        allocatedConnectionsClosedByRequestor: 0,
                                        allocatedConnectionsTimedOut: 1)
        }
        
        
        //
        // releaseConnection: already released
        //
        
        await withConnectionPool { connectionPool in
            
            let connection = await acquireConnections(connectionPool: connectionPool, count: 1).first!
            XCTAssertFalse(connection.isClosed)
            
            connectionPool.releaseConnection(connection)
            try await Task.sleep(for: .seconds(0.1)) // let any async socket close complete
            XCTAssertFalse(connection.isClosed)

            connectionPool.releaseConnection(connection) // should log a warning
            try await Task.sleep(for: .seconds(0.1)) // let any async socket close complete
            XCTAssertTrue(connection.isClosed)
            
            connectionPool.checkMetrics(successfulRequests: 1,
                                        unsuccessfulRequestsTooBusy: 0,
                                        unsuccessfulRequestsTimedOut: 0,
                                        unsuccessfulRequestsError: 0,
                                        minimumPendingRequests: 0,
                                        maximumPendingRequests: 1,
                                        connectionsAtStartOfPeriod: 0,
                                        connectionsAtEndOfPeriod: 0,
                                        connectionsCreated: 1,
                                        allocatedConnectionsClosedByRequestor: 0,
                                        allocatedConnectionsTimedOut: 0)
        }
        
        
        //
        // releaseConnection: already closed by requestor
        //
        
        await withConnectionPool { connectionPool in
            
            let connection = await acquireConnections(connectionPool: connectionPool, count: 1).first!
            XCTAssertFalse(connection.isClosed)
            
            await connection.close()
            XCTAssertTrue(connection.isClosed)

            connectionPool.releaseConnection(connection)
            try await Task.sleep(for: .seconds(0.1)) // let any async socket close complete
            XCTAssertTrue(connection.isClosed)
            
            connectionPool.checkMetrics(successfulRequests: 1,
                                        unsuccessfulRequestsTooBusy: 0,
                                        unsuccessfulRequestsTimedOut: 0,
                                        unsuccessfulRequestsError: 0,
                                        minimumPendingRequests: 0,
                                        maximumPendingRequests: 1,
                                        connectionsAtStartOfPeriod: 0,
                                        connectionsAtEndOfPeriod: 0,
                                        connectionsCreated: 1,
                                        allocatedConnectionsClosedByRequestor: 1,
                                        allocatedConnectionsTimedOut: 0)
        }


        //
        // releaseConnection: explicit transaction was committed
        //
        
        await withConnectionPool { connectionPool in
            
            let connection = await acquireConnections(connectionPool: connectionPool, count: 1).first!
            XCTAssertFalse(connection.isClosed)
            
            do {
                try connection.beginTransaction()
                try connection.commitTransaction()
            } catch {
                XCTFail(String(describing: error))
            }
            
            connectionPool.releaseConnection(connection)
            try await Task.sleep(for: .seconds(0.1)) // let any async socket close complete
            XCTAssertFalse(connection.isClosed)
            
            connectionPool.checkMetrics(successfulRequests: 1,
                                        unsuccessfulRequestsTooBusy: 0,
                                        unsuccessfulRequestsTimedOut: 0,
                                        unsuccessfulRequestsError: 0,
                                        minimumPendingRequests: 0,
                                        maximumPendingRequests: 1,
                                        connectionsAtStartOfPeriod: 0,
                                        connectionsAtEndOfPeriod: 1,
                                        connectionsCreated: 1,
                                        allocatedConnectionsClosedByRequestor: 0,
                                        allocatedConnectionsTimedOut: 0)
        }
        
        
        //
        // releaseConnection: explicit transaction was rolled back
        //
        
        await withConnectionPool { connectionPool in
            
            let connection = await acquireConnections(connectionPool: connectionPool, count: 1).first!
            XCTAssertFalse(connection.isClosed)
            
            do {
                try connection.beginTransaction()
                try connection.rollbackTransaction()
            } catch {
                XCTFail(String(describing: error))
            }
            
            connectionPool.releaseConnection(connection)
            try await Task.sleep(for: .seconds(0.1)) // let any async socket close complete
            XCTAssertFalse(connection.isClosed)
            
            connectionPool.checkMetrics(successfulRequests: 1,
                                        unsuccessfulRequestsTooBusy: 0,
                                        unsuccessfulRequestsTimedOut: 0,
                                        unsuccessfulRequestsError: 0,
                                        minimumPendingRequests: 0,
                                        maximumPendingRequests: 1,
                                        connectionsAtStartOfPeriod: 0,
                                        connectionsAtEndOfPeriod: 1,
                                        connectionsCreated: 1,
                                        allocatedConnectionsClosedByRequestor: 0,
                                        allocatedConnectionsTimedOut: 0)
        }
        
        
        //
        // releaseConnection: explicit transaction was not committed or rolled back
        //
        
        await withConnectionPool { connectionPool in
            
            let connection = await acquireConnections(connectionPool: connectionPool, count: 1).first!
            XCTAssertFalse(connection.isClosed)
            
            do {
                try connection.beginTransaction()
            } catch {
                XCTFail(String(describing: error))
            }
            
            connectionPool.releaseConnection(connection) // should log a warning
            try await Task.sleep(for: .seconds(0.1)) // let any async socket close complete
            XCTAssertTrue(connection.isClosed)
            
            connectionPool.checkMetrics(successfulRequests: 1,
                                        unsuccessfulRequestsTooBusy: 0,
                                        unsuccessfulRequestsTimedOut: 0,
                                        unsuccessfulRequestsError: 0,
                                        minimumPendingRequests: 0,
                                        maximumPendingRequests: 1,
                                        connectionsAtStartOfPeriod: 0,
                                        connectionsAtEndOfPeriod: 0,
                                        connectionsCreated: 1,
                                        allocatedConnectionsClosedByRequestor: 0,
                                        allocatedConnectionsTimedOut: 0)
        }
        
        
        //
        // withConnection: success
        //
        
        await withConnectionPool { connectionPool in
            
            let expectation = expect("withConnection: success")
            
            connectionPool.withConnection { result in
                do {
                    _ = try result.get()
                } catch {
                    XCTFail(String(describing: error))
                }
                
                expectation.fulfill()
            }
            
            await waitForExpectations([expectation])
            
            // Check that the above connection was released.
            await acquireConnections(connectionPool: connectionPool, count: 1) // should reuse that connection
            
            connectionPool.checkMetrics(successfulRequests: 2,
                                        unsuccessfulRequestsTooBusy: 0,
                                        unsuccessfulRequestsTimedOut: 0,
                                        unsuccessfulRequestsError: 0,
                                        minimumPendingRequests: 0,
                                        maximumPendingRequests: 1,
                                        connectionsAtStartOfPeriod: 0,
                                        connectionsAtEndOfPeriod: 1,
                                        connectionsCreated: 1,
                                        allocatedConnectionsClosedByRequestor: 0,
                                        allocatedConnectionsTimedOut: 0)
        }

        //
        // withConnection: failure
        //
        
        await withConnectionPool { connectionPool in
            
            connectionPool.connectionPoolConfiguration.maximumConnections = 5
            await acquireConnections(connectionPool: connectionPool, count: 5)
            
            connectionPool.connectionPoolConfiguration.maximumPendingRequests = 2
            requestConnections(connectionPool: connectionPool, count: 2)
            
            let expectation = expect("withConnection: failure")
            
            connectionPool.withConnection { result in
                guard case .failure(PostgresError.tooManyRequestsForConnections) = result else {
                    return XCTFail("\(result)")
                }
                
                expectation.fulfill()
            }
            
            await waitForExpectations([expectation])
            
            connectionPool.checkMetrics(successfulRequests: 5,
                                        unsuccessfulRequestsTooBusy: 1,
                                        unsuccessfulRequestsTimedOut: 0,
                                        unsuccessfulRequestsError: 0,
                                        minimumPendingRequests: 0,
                                        maximumPendingRequests: 2,
                                        connectionsAtStartOfPeriod: 0,
                                        connectionsAtEndOfPeriod: 5,
                                        connectionsCreated: 5,
                                        allocatedConnectionsClosedByRequestor: 0,
                                        allocatedConnectionsTimedOut: 0)
        }
        
        
        //
        // close(force: false)
        //
        
        await withConnectionPool { connectionPool in
            
            let connections = await acquireConnections(connectionPool: connectionPool, count: 5)
            
            XCTAssertFalse(connectionPool.isClosed)
            connectionPool.close(force: false)
            XCTAssertTrue(connectionPool.isClosed)
            connectionPool.close(force: true) // should have no effect
            XCTAssertTrue(connectionPool.isClosed)

            connectionPool.checkMetrics(successfulRequests: 5,
                                        unsuccessfulRequestsTooBusy: 0,
                                        unsuccessfulRequestsTimedOut: 0,
                                        unsuccessfulRequestsError: 0,
                                        minimumPendingRequests: 0,
                                        maximumPendingRequests: 1,
                                        connectionsAtStartOfPeriod: 0,
                                        connectionsAtEndOfPeriod: 5,
                                        connectionsCreated: 5,
                                        allocatedConnectionsClosedByRequestor: 0,
                                        allocatedConnectionsTimedOut: 0)
            
            try await Task.sleep(for: .seconds(0.1)) // let any async socket close complete
            
            for connection in connections {
                XCTAssertFalse(connection.isClosed)
                connectionPool.releaseConnection(connection)
                try await Task.sleep(for: .seconds(0.1)) // let any async socket close complete
                XCTAssertTrue(connection.isClosed)
            }
            
            connectionPool.checkMetrics(successfulRequests: 5,
                                        unsuccessfulRequestsTooBusy: 0,
                                        unsuccessfulRequestsTimedOut: 0,
                                        unsuccessfulRequestsError: 0,
                                        minimumPendingRequests: 0,
                                        maximumPendingRequests: 1,
                                        connectionsAtStartOfPeriod: 0,
                                        connectionsAtEndOfPeriod: 0,
                                        connectionsCreated: 5,
                                        allocatedConnectionsClosedByRequestor: 0,
                                        allocatedConnectionsTimedOut: 0)
            
        }
        
        
        //
        // close(force: true)
        //
        
        await withConnectionPool { connectionPool in
            
            let connections = await acquireConnections(connectionPool: connectionPool, count: 5)
            
            XCTAssertFalse(connectionPool.isClosed)
            connectionPool.close(force: true)
            XCTAssertTrue(connectionPool.isClosed)
            connectionPool.close(force: true) // should have no effect
            XCTAssertTrue(connectionPool.isClosed)

            connectionPool.checkMetrics(successfulRequests: 5,
                                        unsuccessfulRequestsTooBusy: 0,
                                        unsuccessfulRequestsTimedOut: 0,
                                        unsuccessfulRequestsError: 0,
                                        minimumPendingRequests: 0,
                                        maximumPendingRequests: 1,
                                        connectionsAtStartOfPeriod: 0,
                                        connectionsAtEndOfPeriod: 0,
                                        connectionsCreated: 5,
                                        allocatedConnectionsClosedByRequestor: 0,
                                        allocatedConnectionsTimedOut: 0)
            
            try await Task.sleep(for: .seconds(0.1)) // let any async socket close complete
            
            for connection in connections {
                XCTAssertTrue(connection.isClosed)
            }
        }
    }
    
    
    /// Creates a standardized connection pool, performs the specified operation, and then forcibly
    /// closes the connection pool.
    ///
    /// - Parameters:
    ///   - user: the Postgres user, or nil for a default
    ///   - credential: the credential to use to authenticate to the Postgres server, or nil for
    ///         a default
    ///   - operation: the operation to perform
    func withConnectionPool(
        user: String? = nil,
        credential: Credential? = nil,
        _ operation: (ConnectionPool) async throws -> Void) async {
        
            do {
                let connectionPoolConfiguration = ConnectionPoolConfiguration()
                
                let connectionPool = ConnectionPool(
                    connectionPoolConfiguration: connectionPoolConfiguration,
                    connectionFactory: Self.encryptedConnectionFactory,
                    user: user ?? TestEnvironment.current.terryUsername,
                    credential: credential ?? .trust)
                
                try await operation(connectionPool)
                connectionPool.close(force: true)
            } catch {
                XCTFail(String(describing: error))
            }
    }
    
    /// Acquires and returns the specified number of connections.
    ///
    /// - Parameters:
    ///   - connectionPool: the connection pool
    ///   - count: the number of connections to acquire
    /// - Returns: the connections
    @discardableResult func acquireConnections(connectionPool: ConnectionPool,
                                               count: Int) async -> [Connection] {
        
        var connections = [Connection]()
        
        for _ in 0..<count {
            let expectation = expect("acquireConnections")
            
            connectionPool.acquireConnection { result in
                do {
                    let connection = try result.get()
                    connections.append(connection)
                    expectation.fulfill()
                } catch {
                    Postgres.logger.info("<<< \(error) >>>")
                }
            }
            
            await waitForExpectations([expectation])
        }
        
        assert(connections.count == count)
        
        return connections
    }
    
    /// Requests the specified number of connections, without waiting for them to be allocated.
    ///
    /// - Parameters:
    ///   - connectionPool: the connection pool
    ///   - count: the number of connections to request
    func requestConnections(connectionPool: ConnectionPool, count: Int) {
        
        for _ in 0..<count {
            connectionPool.acquireConnection { result in
                do {
                    _ = try result.get()
                } catch {
                    Postgres.logger.info("<<< \(error) >>>")
                }
            }
        }
    }
}
        
fileprivate extension ConnectionPool {
    
    
    /// Computes the current performance metrics and verifies they have the specified values.
    func checkMetrics(successfulRequests: Int,
                      unsuccessfulRequestsTooBusy: Int,
                      unsuccessfulRequestsTimedOut: Int,
                      unsuccessfulRequestsError: Int,
                      minimumPendingRequests: Int,
                      maximumPendingRequests: Int,
                      connectionsAtStartOfPeriod: Int,
                      connectionsAtEndOfPeriod: Int,
                      connectionsCreated: Int,
                      allocatedConnectionsClosedByRequestor: Int,
                      allocatedConnectionsTimedOut: Int,
                      file: StaticString = #file, line: UInt = #line) {
        
        let metrics = computeMetrics(reset: false)
        
        XCTAssertEqual(metrics.successfulRequests,
                       successfulRequests,
                       "successfulRequests", file: file, line: line)
        
        XCTAssertEqual(metrics.unsuccessfulRequestsTooBusy,
                       unsuccessfulRequestsTooBusy,
                       "unsuccessfulRequestsTooBusy", file: file, line: line)
        
        XCTAssertEqual(metrics.unsuccessfulRequestsTimedOut,
                       unsuccessfulRequestsTimedOut,
                       "unsuccessfulRequestsTimedOut", file: file, line: line)
        
        XCTAssertEqual(metrics.unsuccessfulRequestsError,
                       unsuccessfulRequestsError,
                       "unsuccessfulRequestsError", file: file, line: line)

        XCTAssertEqual(metrics.minimumPendingRequests,
                       minimumPendingRequests,
                       "maximumPendingRequests", file: file, line: line)
        
        XCTAssertEqual(metrics.maximumPendingRequests,
                       maximumPendingRequests,
                       "maximumPendingRequests", file: file, line: line)
        
        XCTAssertEqual(metrics.connectionsAtStartOfPeriod,
                       connectionsAtStartOfPeriod,
                       "connectionsAtStartOfPeriod", file: file, line: line)
        
        XCTAssertEqual(metrics.connectionsAtEndOfPeriod,
                       connectionsAtEndOfPeriod,
                       "connectionsAtEndOfPeriod", file: file, line: line)
        
        XCTAssertEqual(metrics.connectionsCreated,
                       connectionsCreated,
                       "connectionsCreated", file: file, line: line)
        
        XCTAssertEqual(metrics.allocatedConnectionsClosedByRequestor,
                       allocatedConnectionsClosedByRequestor,
                       "allocatedConnectionsClosedByRequestor", file: file, line: line)
        
        XCTAssertEqual(metrics.allocatedConnectionsTimedOut,
                       allocatedConnectionsTimedOut,
                       "allocatedConnectionsTimedOut", file: file, line: line)
    }
}

extension ConnectionPoolConfiguration: Equatable {
    
    public static func == (lhs: ConnectionPoolConfiguration,
                           rhs: ConnectionPoolConfiguration) -> Bool {
        
        return lhs.maximumConnections == rhs.maximumConnections &&
            lhs.maximumPendingRequests == rhs.maximumPendingRequests &&
            lhs.pendingRequestTimeout == rhs.pendingRequestTimeout &&
            lhs.allocatedConnectionTimeout == rhs.allocatedConnectionTimeout &&
            lhs.dispatchQueue === rhs.dispatchQueue &&
            lhs.metricsLoggingInterval == rhs.metricsLoggingInterval &&
            lhs.metricsResetWhenLogged == rhs.metricsResetWhenLogged
    }
}

// EOF
