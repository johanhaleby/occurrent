package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game

import org.occurrent.application.service.blocking.ApplicationService
import org.occurrent.application.service.blocking.execute
import org.occurrent.application.service.blocking.executePolicies
import org.occurrent.application.service.blocking.executePolicy
import org.occurrent.eventstore.api.WriteConditionNotFulfilledException
import org.occurrent.example.domain.wordguessinggame.event.GameEvent
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.policy.PointAwardingPolicy
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.policy.WordHintPolicies
import org.occurrent.example.domain.wordguessinggame.writemodel.*
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class GamePlayApplicationService constructor(private val applicationService: ApplicationService<GameEvent>,
                                             private val wordHintPolicies: WordHintPolicies, private val pointAwardingPolicy: PointAwardingPolicy) {

    @Transactional
    @Retryable(include = [WriteConditionNotFulfilledException::class], maxAttempts = 5, backoff = Backoff(delay = 100, multiplier = 2.0, maxDelay = 1000))
    fun startGame(gameId: GameId, startTime: Timestamp, startedBy: PlayerId, wordList: WordList) {
        applicationService.execute(gameId, { events ->
            startGame(events, gameId, startTime, startedBy, wordList, MaxNumberOfGuessesPerPlayer, MaxNumberOfGuessesTotal)
        }, executePolicy(wordHintPolicies::whenGameWasStartedThenRevealInitialCharactersInWordHint))
    }


    @Transactional
    @Retryable(include = [WriteConditionNotFulfilledException::class], maxAttempts = 5, backoff = Backoff(delay = 100, multiplier = 2.0, maxDelay = 1000))
    fun makeGuess(gameId: GameId, timeOfGuess: Timestamp, playerId: PlayerId, word: Word) {
        applicationService.execute(gameId, { events -> guessWord(events, timeOfGuess, playerId, word) },
                executePolicies(wordHintPolicies::whenPlayerGuessedTheWrongWordThenRevealCharacterInWordHint,
                        pointAwardingPolicy::whenPlayerGuessedTheRightWordThenAwardPointsToPlayerThatGuessedTheRightWord))
    }
}