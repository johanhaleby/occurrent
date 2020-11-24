package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.queries

import org.occurrent.example.domain.wordguessinggame.event.GameEvent
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure.DomainEventQueries
import org.occurrent.example.domain.wordguessinggame.readmodel.GameReadModel
import org.occurrent.example.domain.wordguessinggame.readmodel.rehydrateToGameReadModel
import org.occurrent.filter.Filter.subject
import org.springframework.stereotype.Component
import java.util.*

@Component
class FindGameByIdQuery(private val domainEvents: DomainEventQueries) {

    fun execute(gameId: UUID): GameReadModel? =
            // We use subject to retrieve _all_ game events from different streams
            domainEvents.query<GameEvent>(subject(gameId.toString())).rehydrateToGameReadModel()
}