//
//  Concurrency.swift
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

internal class UnsafeTask<T> { // FIXME: remove; "unsafe" because blocks the invoking thread
    
    private let semaphore = DispatchSemaphore(value: 0)
    private var result: T?
    
    init(block: @escaping () async -> T) {
        Task {
            result = await block()
            semaphore.signal()
        }
    }

    func get() -> T {
        semaphore.wait()
        return result!
    }
}

internal class ThrowingUnsafeTask<T> { // FIXME: remove; "unsafe" because blocks the invoking thread
    
    private let semaphore = DispatchSemaphore(value: 0)
    private var result: Result<T, Error>?
    
    init(block: @escaping () async throws -> T) {
        Task {
            do {
                result = .success(try await block())
            } catch {
                result = .failure(error)
            }
            
            semaphore.signal()
        }
    }

    func get() throws -> T {
        semaphore.wait()
        return try result!.get()
    }
}

// EOF
