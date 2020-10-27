package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.policy

import org.occurrent.example.domain.wordguessinggame.event.GameWasLost
import org.occurrent.example.domain.wordguessinggame.event.GameWasStarted
import org.occurrent.example.domain.wordguessinggame.event.GameWasWon
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.persistence.OngoingGameOverviewMongoDTO
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.persistence.toDTO
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure.Policies
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure.loggerFor
import org.occurrent.example.domain.wordguessinggame.readmodel.OngoingGameOverview
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.query.Criteria.where
import org.springframework.data.mongodb.core.query.Query.query
import org.springframework.data.mongodb.core.query.isEqualTo
import org.springframework.data.mongodb.core.remove

@Configuration
class OngoingGameOverviewPolicies {
    private val log = loggerFor<OngoingGameOverviewPolicies>()

    @Autowired
    lateinit var policies: Policies

    @Autowired
    lateinit var mongo: MongoOperations

    @Bean
    fun whenGameWasStartedThenAddGameToOngoingGamesOverview() = policies.newPolicy<GameWasStarted>("WhenGameWasStartedThenAddGameToOngoingGamesOverview") { gameWasStarted ->
        log.info("Adding game ${gameWasStarted.gameId} to ongoing games view")
        val ongoingGameOverview = gameWasStarted.run {
            OngoingGameOverview(gameId, category, startedBy, timestamp).toDTO()
        }
        mongo.insert(ongoingGameOverview)
    }

    @Bean
    fun whenGameIsEndedThenRemoveGameFromOngoingGamesOverview() = policies.newPolicy("WhenGameIsEndedThenRemoveGameFromOngoingGamesOverview", GameWasWon::class, GameWasLost::class) { e ->
        val gameId = when (e) {
            is GameWasWon -> e.gameId
            is GameWasLost -> e.gameId
            else -> throw IllegalStateException("Internal error")
        }
        log.info("Removing game $gameId from ongoing games view since ${e.type}")
        mongo.remove<OngoingGameOverviewMongoDTO>(query(where("_id").isEqualTo(gameId.toString())))
    }
}