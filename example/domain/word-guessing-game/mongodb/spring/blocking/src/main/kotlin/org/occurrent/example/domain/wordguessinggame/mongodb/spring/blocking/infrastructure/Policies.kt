package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure

import org.occurrent.example.domain.wordguessinggame.event.DomainEvent
import org.occurrent.example.domain.wordguessinggame.event.eventType
import org.occurrent.filter.Filter
import org.occurrent.subscription.OccurrentSubscriptionFilter
import org.occurrent.subscription.api.blocking.Subscription
import org.occurrent.subscription.util.blocking.BlockingSubscriptionWithAutomaticPositionPersistence
import org.springframework.stereotype.Component


/**
 * Just a convenience utility that makes it easier, and more consistent, to create policies.
 */
@Component
class Policies(val subscriptions: BlockingSubscriptionWithAutomaticPositionPersistence, val cloudEventConverter: CloudEventConverter) {

    // Helper function that creates simple policies using a specific event type
    final inline fun <reified T : DomainEvent> newPolicy(policyId: String, crossinline fn: (T) -> Unit): Subscription = subscriptions.subscribe(policyId, OccurrentSubscriptionFilter.filter(Filter.type(T::class.eventType()))) { cloudEvent ->
        val event = cloudEventConverter.toDomainEvent(cloudEvent) as T
        fn(event)
    }.apply {
        waitUntilStarted()
    }
}

