package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure

import org.occurrent.example.domain.wordguessinggame.event.DomainEvent
import org.occurrent.example.domain.wordguessinggame.event.eventType
import org.occurrent.filter.Filter.type
import org.occurrent.subscription.OccurrentSubscriptionFilter.filter
import org.occurrent.subscription.api.blocking.Subscription
import org.occurrent.subscription.util.blocking.BlockingSubscriptionWithAutomaticPositionPersistence
import org.springframework.stereotype.Component


/**
 * Just a convenience utility that makes it easier, and more consistent, to create policies.
 */
@Component
class Policies(val subscriptions: BlockingSubscriptionWithAutomaticPositionPersistence, val cloudEventConverter: CloudEventConverter) {

    /**
     * Create a new policy that is invoked after a specific domain event is written to the event store
     */
    final inline fun <reified T : DomainEvent> newPolicy(policyId: String, crossinline fn: (T) -> Unit): Subscription = subscriptions.subscribe(policyId, filter(type(T::class.eventType()))) { cloudEvent ->
        val event = cloudEventConverter.toDomainEvent(cloudEvent) as T
        fn(event)
    }.apply {
        waitUntilStarted()
    }
}

