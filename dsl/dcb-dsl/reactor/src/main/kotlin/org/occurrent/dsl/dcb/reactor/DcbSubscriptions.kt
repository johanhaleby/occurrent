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

package org.occurrent.dsl.dcb.reactor

import org.occurrent.dsl.dcb.DcbEventMetadata
import org.occurrent.eventstore.api.dcb.DcbCriteria
import org.occurrent.subscription.DcbStartAt
import org.occurrent.subscription.api.reactor.SubscriptionHandle
import reactor.core.publisher.Mono

/**
 * Kotlin-idiomatic sugar over [DcbSubscriptions], the canonical class-based reactive DCB subscription entry-point,
 * mirroring the blocking counterpart in `org.occurrent.dsl.dcb.blocking`. Unlike the blocking DSL there is no
 * `waitUntilStarted` flag here: [SubscriptionHandle] is returned immediately and exposes its own
 * [SubscriptionHandle.waitUntilStarted] returning a `Mono<Void>` that the caller can compose into their own reactive
 * chain, mirroring the equivalent decision in the regular subscription DSL's reactor module.
 */
@JvmName("subscribeDcb")
fun <E : Any> DcbSubscriptions<E>.subscribeDcb(
    subscriptionId: String,
    criteria: DcbCriteria = DcbCriteria.all(),
    startAt: DcbStartAt? = null,
    fn: (E) -> Mono<Void>
): SubscriptionHandle = subscribeWithMetadata(subscriptionId, criteria, startAt) { _, event -> fn(event) }

/**
 * Kotlin-idiomatic sugar over [DcbSubscriptions], including DCB metadata in the callback. See [subscribeDcb] for why
 * there is no `waitUntilStarted` parameter.
 */
@JvmName("subscribeDcbWithMetadata")
fun <E : Any> DcbSubscriptions<E>.subscribeDcbWithMetadata(
    subscriptionId: String,
    criteria: DcbCriteria = DcbCriteria.all(),
    startAt: DcbStartAt? = null,
    fn: (DcbEventMetadata, E) -> Mono<Void>
): SubscriptionHandle = subscribeWithMetadata(subscriptionId, criteria, startAt, fn)
