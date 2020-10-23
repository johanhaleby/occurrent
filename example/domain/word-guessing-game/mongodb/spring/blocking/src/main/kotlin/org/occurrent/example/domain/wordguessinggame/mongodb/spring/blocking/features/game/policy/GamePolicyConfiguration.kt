package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.policy

import org.occurrent.example.domain.wordguessinggame.event.*
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.persistence.OngoingGameOverviewMongoDTO
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.persistence.toDTO
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure.DomainEventQueries
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure.ApplicationService
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure.Policies
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure.loggerFor
import org.occurrent.example.domain.wordguessinggame.policy.WhenGameWasWonThenSendEmailToWinnerPolicy
import org.occurrent.example.domain.wordguessinggame.readmodel.OngoingGameOverview
import org.occurrent.example.domain.wordguessinggame.writemodel.WordHintCharacterRevelation
import org.occurrent.example.domain.wordguessinggame.writemodel.WordHintData
import org.occurrent.filter.Filter.streamId
import org.occurrent.filter.Filter.type
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.query.Query.query
import org.springframework.data.mongodb.core.query.isEqualTo
import org.springframework.data.mongodb.core.query.where

@Configuration
class GamePolicyConfiguration {
    private val log = loggerFor<GamePolicyConfiguration>()

    @Autowired
    lateinit var policies: Policies

    @Autowired
    lateinit var mongo: MongoOperations

    @Autowired
    lateinit var applicationService: ApplicationService

    @Autowired
    lateinit var domainEventQueries: DomainEventQueries

    @Bean
    fun whenGameWasWonThenSendEmailToWinner() = policies.newPolicy<GameWasWon>(WhenGameWasWonThenSendEmailToWinnerPolicy::class.simpleName!!) { gameWasWon ->
        log.info("Sending email to player ${gameWasWon.winnerId} since he/she was a winner of game ${gameWasWon.gameId}")
    }

    @Bean
    fun whenGameWasStartedThenAddGameToOngoingGamesOverview() = policies.newPolicy<GameWasStarted>("WhenGameWasStartedThenAddGameToOngoingGamesOverview") { gameWasStarted ->
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
        mongo.remove(query(where(OngoingGameOverviewMongoDTO::gameId).isEqualTo(gameId)), "ongoingGames")
    }

    @Bean
    fun whenGameWasStartedThenRevealInitialCharactersInWordHint() = policies.newPolicy<GameWasStarted>("WhenGameWasStartedThenRevealInitialCharactersInWordHint") { gameWasStarted ->
        applicationService.execute("wordhint:${gameWasStarted.gameId}") { events ->
            if (events.toList().isEmpty()) {
                WordHintCharacterRevelation.revealInitialCharactersInWordHintWhenGameWasStarted(WordHintData(gameWasStarted.gameId, gameWasStarted.wordToGuess))
            } else {
                emptySequence()
            }
        }
    }

    @Bean
    fun whenPlayerGuessedTheWrongWordThenRevealCharacterInWordHint() = policies.newPolicy<PlayerGuessedTheWrongWord>("WhenPlayerGuessedTheWrongWordThenRevealCharacterInWordHint") { playerGuessedTheWrongWord ->
        val gameId = playerGuessedTheWrongWord.gameId
        applicationService.execute("wordhint:$gameId") { events ->
            val characterPositionsInWord = events.map { it as CharacterInWordHintWasRevealed }.map { it.characterPositionInWord }.toSet()
            val gameWasStarted = domainEventQueries.queryOne<GameWasStarted>(streamId(gameId.toString()).and(type(GameWasStarted::class.eventType())))
            val wordHintData = WordHintData(gameId, wordToGuess = gameWasStarted.wordToGuess, currentlyRevealedPositions = characterPositionsInWord)
            WordHintCharacterRevelation.revealInitialCharactersInWordHintWhenGameWasStarted(wordHintData)
        }
    }
}