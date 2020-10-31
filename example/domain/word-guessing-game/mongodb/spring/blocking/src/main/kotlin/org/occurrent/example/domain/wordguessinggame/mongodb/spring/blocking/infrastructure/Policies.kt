package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure

import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.condition.Condition.eq
import org.occurrent.condition.Condition.or
import org.occurrent.example.domain.wordguessinggame.event.DomainEvent
import org.occurrent.example.domain.wordguessinggame.event.eventType
import org.occurrent.filter.Filter.type
import org.occurrent.subscription.OccurrentSubscriptionFilter.filter
import org.occurrent.subscription.api.blocking.BlockingSubscription
import org.occurrent.subscription.api.blocking.Subscription
import org.springframework.stereotype.Component
import kotlin.reflect.KClass


/**
 * Just a convenience utility that makes it easier, and more consistent, to create policies.
 */
@Component
class Policies(val subscriptions: BlockingSubscription, val cloudEventConverter: CloudEventConverter<DomainEvent>) {

    /**
     * Create a new policy that is invoked after a specific domain event is written to the event store
     */
    final inline fun <reified T : DomainEvent> newPolicy(policyId: String, crossinline fn: (T) -> Unit): Subscription = subscriptions.subscribe(policyId, filter(type(T::class.eventType()))) { cloudEvent ->
        val event = cloudEventConverter.toDomainEvent(cloudEvent) as T
        fn(event)
    }.apply {
        waitUntilStarted()
    }

    fun newPolicy(policyId: String, vararg domainEventTypes: KClass<out DomainEvent>, fn: (DomainEvent) -> Unit): Subscription {
        val filter = filter(type(or(domainEventTypes.map { e -> eq(e.eventType()) })))
        val subscription = subscriptions.subscribe(policyId, filter) { cloudEvent ->
            val event = cloudEventConverter.toDomainEvent(cloudEvent)
            fn(event)
        }
        subscription.waitUntilStarted()
        return subscription
    }
}

