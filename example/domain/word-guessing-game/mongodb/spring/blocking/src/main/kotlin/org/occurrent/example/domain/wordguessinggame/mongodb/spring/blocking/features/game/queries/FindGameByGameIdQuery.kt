package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.queries

import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure.DomainEventQueries
import org.occurrent.example.domain.wordguessinggame.readmodel.game.AssembleGameReadModelFromDomainEvents
import org.occurrent.example.domain.wordguessinggame.readmodel.game.GameReadModel
import org.springframework.stereotype.Component
import java.util.*

@Component
class FindGameByGameIdQuery(private val domainEvents: DomainEventQueries) {

    fun execute(gameId: UUID): GameReadModel? =
            domainEvents.forGame(gameId)
                    .fold(AssembleGameReadModelFromDomainEvents(), AssembleGameReadModelFromDomainEvents::applyEvent)
                    .gameReadModel
}