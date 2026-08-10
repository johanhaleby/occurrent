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

package org.occurrent.dsl.subscription.reactor

import org.occurrent.subscription.api.reactor.SubscriptionModelCapability

/**
 * Kotlin-idiomatic counterpart to [SubscriptionModelCapability.capability], returning a nullable [T] instead of the
 * Java `Optional<T>` and inferring the capability from the type argument instead of a [Class] parameter.
 */
inline fun <reified T : SubscriptionModelCapability> SubscriptionModelCapability.capability(): T? = capability(T::class.java).orElse(null)

/**
 * Kotlin-idiomatic counterpart to [SubscriptionModelCapability.hasCapability], inferring the capability from the type
 * argument instead of a [Class] parameter.
 */
inline fun <reified T : SubscriptionModelCapability> SubscriptionModelCapability.hasCapability(): Boolean = hasCapability(T::class.java)
