package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.queries

import org.occurrent.example.domain.wordguessinggame.event.DomainEvent
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure.DomainEventQueries
import org.occurrent.example.domain.wordguessinggame.readmodel.AssembleGameReadModelFromDomainEvents
import org.occurrent.example.domain.wordguessinggame.readmodel.GameReadModel
import org.occurrent.filter.Filter.subject
import org.springframework.stereotype.Component
import java.util.*

@Component
class FindGameByIdQuery(private val domainEvents: DomainEventQueries) {

    fun execute(gameId: UUID): GameReadModel? =
            domainEvents.query<DomainEvent>(subject(gameId.toString())) // We use subject to retrieve _all_ game events from different streams
                    .fold(AssembleGameReadModelFromDomainEvents(), AssembleGameReadModelFromDomainEvents::applyEvent)
                    .gameReadModel
}