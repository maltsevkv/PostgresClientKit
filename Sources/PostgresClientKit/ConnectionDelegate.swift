//
//  ConnectionDelegate.swift
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


/// A delegate for `Connection` instances.
public protocol ConnectionDelegate: AnyObject {
    
    /// Called upon a notification event from the Postgres server.
    ///
    /// - SeeAlso: [Postgres: `NOTIFY` command](https://www.postgresql.org/docs/11/sql-notify.html)
    ///
    /// - Parameters:
    ///   - connection: the `Connection`
    ///   - notice: the notification event
    func connection(_ connection: Connection,
                    didReceiveNotice notice: Notice)
    
    /// Called upon a change in the value of certain Postgres server parameters.
    ///
    /// - SeeAlso: [Postgres: Message Flow - Asynchronous
    ///     Opererations](https://www.postgresql.org/docs/11/protocol-flow.html#PROTOCOL-ASYNC)
    ///
    /// - Parameters:
    ///   - connection: the `Connection`
    ///   - parameterStatus: the parameter name and new value
    func connection(_ connection: Connection,
                    didReceiveParameterStatus parameterStatus: (name: String, value: String))
}

// EOF
