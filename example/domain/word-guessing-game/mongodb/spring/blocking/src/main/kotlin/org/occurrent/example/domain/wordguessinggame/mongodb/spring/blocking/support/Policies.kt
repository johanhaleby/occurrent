/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.support

import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.condition.Condition.eq
import org.occurrent.condition.Condition.or
import org.occurrent.example.domain.wordguessinggame.event.GameEvent
import org.occurrent.example.domain.wordguessinggame.event.eventType
import org.occurrent.filter.Filter.type
import org.occurrent.subscription.OccurrentSubscriptionFilter.filter
import org.occurrent.subscription.api.blocking.Subscription
import org.occurrent.subscription.api.blocking.SubscriptionModel
import org.springframework.stereotype.Component
import kotlin.reflect.KClass


/**
 * Just a convenience utility that makes it easier, and more consistent, to create policies.
 */
@Component
class Policies(val subscriptionModel: SubscriptionModel, val cloudEventConverter: CloudEventConverter<GameEvent>) {

    /**
     * Create a new policy that is invoked after a specific domain event is written to the event store
     */
    final inline fun <reified T : GameEvent> newPolicy(policyId: String, crossinline fn: (T) -> Unit): Subscription = subscriptionModel.subscribe(policyId, filter(type(T::class.eventType()))) { cloudEvent ->
        val event = cloudEventConverter.toDomainEvent(cloudEvent) as T
        
        fn(event)
    }.apply {
        waitUntilStarted()
    }

    fun newPolicy(policyId: String, vararg gameEventTypes: KClass<out GameEvent>, fn: (GameEvent) -> Unit): Subscription {
        val filter = filter(type(or(gameEventTypes.map { e -> eq(e.eventType()) })))
        val subscription = subscriptionModel.subscribe(policyId, filter) { cloudEvent ->
            val event = cloudEventConverter.toDomainEvent(cloudEvent)
            fn(event)
        }
        subscription.waitUntilStarted()
        return subscription
    }
}

