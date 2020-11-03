package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.policy

import org.occurrent.example.domain.wordguessinggame.event.GameWasLost
import org.occurrent.example.domain.wordguessinggame.event.GameWasStarted
import org.occurrent.example.domain.wordguessinggame.event.GameWasWon
import org.occurrent.example.domain.wordguessinggame.event.eventType
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.persistence.toDTO
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure.DomainEventQueries
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure.Policies
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure.loggerFor
import org.occurrent.example.domain.wordguessinggame.readmodel.LostGameOverview
import org.occurrent.example.domain.wordguessinggame.readmodel.WonGameOverview
import org.occurrent.filter.Filter.streamId
import org.occurrent.filter.Filter.type
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.core.MongoOperations

@Configuration
class EndedGameOverviewPolicy {
    private val log = loggerFor<EndedGameOverviewPolicy>()

    @Autowired
    lateinit var policies: Policies

    @Autowired
    lateinit var mongo: MongoOperations

    @Autowired
    lateinit var domainEventQueries: DomainEventQueries

    @Bean
    fun whenGameIsEndedThenAddGameToGameEndedOverview() = policies.newPolicy("WhenGameIsEndedThenAddGameToGameEndedOverview", GameWasWon::class, GameWasLost::class) { e ->
        log.info("${e::class.eventType()} - will update ended games overview")
        val gameId = e.gameId
        val gameWasStarted = domainEventQueries.queryOne<GameWasStarted>(streamId(gameId.toString()).and(type(GameWasStarted::class.eventType())))
        val endedGameOverview = when (e) {
            is GameWasWon -> WonGameOverview(gameId, gameWasStarted.category, gameWasStarted.startedBy, gameWasStarted.timestamp, e.timestamp, gameWasStarted.wordToGuess, e.winnerId)
            is GameWasLost -> LostGameOverview(gameId, gameWasStarted.category, gameWasStarted.startedBy, gameWasStarted.timestamp, e.timestamp, gameWasStarted.wordToGuess)
            else -> throw IllegalStateException("Internal error")
        }.toDTO()
        mongo.insert(endedGameOverview)
    }
}