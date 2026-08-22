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

package org.occurrent.dsl.dcb.blocking

import org.occurrent.dsl.dcb.DcbEventMetadata
import org.occurrent.eventstore.api.dcb.DcbCriteria
import org.occurrent.subscription.DcbStartAt
import org.occurrent.subscription.api.blocking.SubscriptionHandle

/**
 * Kotlin-idiomatic sugar over [DcbSubscriptions], the canonical class-based DCB subscription entry-point. This is a
 * thin wrapper that always forwards to [DcbSubscriptions.subscribeWithMetadata] with an explicit `waitUntilStarted`
 * argument, so the Kotlin default here (`true`) is preserved regardless of what the Java 3-arg overloads on
 * [DcbSubscriptions] resolve to when called without that argument.
 */
@JvmName("subscribeDcb")
fun <E : Any> DcbSubscriptions<E>.subscribeDcb(
    subscriptionId: String,
    criteria: DcbCriteria = DcbCriteria.all(),
    startAt: DcbStartAt? = null,
    waitUntilStarted: Boolean = true,
    fn: (E) -> Unit
): SubscriptionHandle = subscribeWithMetadata(subscriptionId, criteria, startAt, waitUntilStarted) { _, event -> fn(event) }

/**
 * Kotlin-idiomatic sugar over [DcbSubscriptions], including DCB metadata in the callback. See [subscribeDcb] for the
 * `waitUntilStarted` default behavior.
 */
@JvmName("subscribeDcbWithMetadata")
fun <E : Any> DcbSubscriptions<E>.subscribeDcbWithMetadata(
    subscriptionId: String,
    criteria: DcbCriteria = DcbCriteria.all(),
    startAt: DcbStartAt? = null,
    waitUntilStarted: Boolean = true,
    fn: (DcbEventMetadata, E) -> Unit
): SubscriptionHandle = subscribeWithMetadata(subscriptionId, criteria, startAt, waitUntilStarted, fn)
