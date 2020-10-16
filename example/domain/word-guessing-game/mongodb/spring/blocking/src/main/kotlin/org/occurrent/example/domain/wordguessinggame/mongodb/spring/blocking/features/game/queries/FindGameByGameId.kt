package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.queries

import org.occurrent.eventstore.api.blocking.EventStoreQueries
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure.CloudEventConverter
import org.occurrent.example.domain.wordguessinggame.readmodel.game.GameReadModel
import org.occurrent.example.domain.wordguessinggame.readmodel.game.GameReadModelStateMachine
import org.occurrent.filter.Filter.streamId
import org.springframework.stereotype.Component
import java.util.*
import kotlin.streams.asSequence

@Component
class FindGameByGameId(private val eventStoreQueries: EventStoreQueries, private val cloudEventConverter: CloudEventConverter) {

    fun queryFor(gameId: UUID): GameReadModel? =
            eventStoreQueries.query(streamId(gameId.toString())).asSequence()
                    .map(cloudEventConverter::toDomainEvent)
                    .fold(GameReadModelStateMachine(), GameReadModelStateMachine::applyEvent)
                    .state
}