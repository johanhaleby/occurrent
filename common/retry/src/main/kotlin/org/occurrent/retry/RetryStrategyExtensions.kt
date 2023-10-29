/*
 *
 *  Copyright 2023 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.retry


/**
 * A kotlin extension function that makes it easier to execute the retry strategy with a "Supplier".
 *
 * The reasons for this is that when just doing this from kotlin:
 *
 * ```
 * val string = retryStrategy.execute { "hello" }
 * ```
 *
 * This will return `Unit` and not the "hello" string that you would expect.
 * This is because execute in the example above delegates to org.occurrent.retry.RetryStrategy.execute(java.lang.Runnable)
 * and not org.occurrent.retry.RetryStrategy.execute(java.util.function.Supplier<T>) which one would expect.
 * Thus, you can use this function instead to avoid specifying the `Supplier` SAM explicitly.
 *
 * I.e. instead of doing:
 *
 * ```kotlin
 * val string : String = retryStrategy.execute(Supplier { "hello" })
 * ```
 *
 * you can do:
 *
 * ```kotlin
 * val string : String = retryStrategy.exec { "hello" }
 * ```
 *
 * after having imported `org.occurrent.retry.exec`.
 */
fun <T> RetryStrategy.exec(fn: (RetryInfo) -> T): T = execute { retryInfo -> fn(retryInfo)}