package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure

import org.occurrent.eventstore.api.blocking.EventStoreQueries
import org.occurrent.example.domain.wordguessinggame.event.DomainEvent
import org.occurrent.example.domain.wordguessinggame.writemodel.GameId
import org.occurrent.filter.Filter.streamId
import org.springframework.stereotype.Component
import kotlin.streams.asSequence


/**
 * Simple wrapper around [EventStoreQueries] that provides an interface that deals with domain events instead of cloud events
 */
@Component
class DomainEventQueries(private val eventStoreQueries: EventStoreQueries, private val cloudEventConverter: CloudEventConverter) {

    fun forGame(gameId: GameId): Sequence<DomainEvent> =
            eventStoreQueries.query(streamId(gameId.toString())).asSequence()
                    .map(cloudEventConverter::toDomainEvent)
}