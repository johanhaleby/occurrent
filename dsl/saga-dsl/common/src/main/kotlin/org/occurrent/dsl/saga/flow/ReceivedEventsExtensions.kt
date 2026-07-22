/*
 * Copyright 2026 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.dsl.saga.flow

// Reified Kotlin equivalents of the Class-taking ReceivedEvents members, so a flow reaction can write
// received.initiating<OrderPlaced>() instead of received.initiating(OrderPlaced::class.java). The single type
// parameter and the ReceivedEvents<in T> receiver are what let the explicit type argument bind to the extension
// rather than shadowing to the member (the same shape the query DSL uses for its reified queryOne).

/** The initiating event cast to [T]. Throws [ClassCastException] if it is not of that type. */
inline fun <reified T : Any> ReceivedEvents<in T>.initiating(): T = initiating(T::class.java)

/** The first received event of type [T], or `null` if none was received. */
inline fun <reified T : Any> ReceivedEvents<in T>.first(): T? = first(T::class.java).orElse(null)

/** Whether any event of type [T] has been received. */
inline fun <reified T : Any> ReceivedEvents<in T>.any(): Boolean = first(T::class.java).isPresent

/** All received events of type [T], in arrival order. */
inline fun <reified T : Any> ReceivedEvents<in T>.all(): List<T> = all(T::class.java)

/** How many events of type [T] have been received. */
inline fun <reified T : Any> ReceivedEvents<in T>.count(): Int = count(T::class.java)
