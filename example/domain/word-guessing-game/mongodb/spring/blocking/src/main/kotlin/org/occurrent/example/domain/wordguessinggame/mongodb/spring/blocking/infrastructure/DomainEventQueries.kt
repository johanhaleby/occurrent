package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure

import org.occurrent.eventstore.api.blocking.EventStoreQueries
import org.occurrent.example.domain.wordguessinggame.event.DomainEvent
import org.occurrent.filter.Filter
import org.springframework.stereotype.Component
import kotlin.streams.asSequence


/**
 * Simple wrapper around [EventStoreQueries] that provides an interface that deals with domain events instead of cloud events
 */
@Component
class DomainEventQueries(private val eventStoreQueries: EventStoreQueries, private val cloudEventConverter: CloudEventConverter) {

    @Suppress("UNCHECKED_CAST")
    fun <T : DomainEvent> query(filter: Filter): Sequence<T> {
        return eventStoreQueries.query(filter).asSequence()
                .map(cloudEventConverter::toDomainEvent)
                .map { it as T }
    }

    fun <T : DomainEvent> queryOne(filter: Filter): T {
        return query<T>(filter).first()
    }
}