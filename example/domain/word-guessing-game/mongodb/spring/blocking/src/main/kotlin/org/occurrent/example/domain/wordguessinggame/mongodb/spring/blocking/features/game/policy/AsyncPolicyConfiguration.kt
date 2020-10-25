package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.policy

import org.occurrent.example.domain.wordguessinggame.event.GameWasLost
import org.occurrent.example.domain.wordguessinggame.event.GameWasStarted
import org.occurrent.example.domain.wordguessinggame.event.GameWasWon
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.persistence.OngoingGameOverviewMongoDTO
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.persistence.toDTO
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure.Policies
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure.loggerFor
import org.occurrent.example.domain.wordguessinggame.policy.WhenGameWasWonThenSendEmailToWinnerPolicy
import org.occurrent.example.domain.wordguessinggame.readmodel.OngoingGameOverview
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.query.Query.query
import org.springframework.data.mongodb.core.query.isEqualTo
import org.springframework.data.mongodb.core.query.where

@Configuration
class AsyncPolicyConfiguration {
    private val log = loggerFor<AsyncPolicyConfiguration>()

    @Autowired
    lateinit var policies: Policies

    @Autowired
    lateinit var mongo: MongoOperations

    @Bean
    fun whenGameWasWonThenSendEmailToWinner() = policies.newPolicy<GameWasWon>(WhenGameWasWonThenSendEmailToWinnerPolicy::class.simpleName!!) { gameWasWon ->
        log.info("Sending email to player ${gameWasWon.winnerId} since he/she was a winner of game ${gameWasWon.gameId}")
    }

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
        mongo.remove(query(where(OngoingGameOverviewMongoDTO::gameId).isEqualTo(gameId)), "ongoingGames")
    }
}