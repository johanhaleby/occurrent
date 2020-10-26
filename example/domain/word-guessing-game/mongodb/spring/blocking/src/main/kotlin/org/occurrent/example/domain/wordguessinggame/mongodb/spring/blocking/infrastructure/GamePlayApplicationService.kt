package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure

import org.occurrent.application.service.blocking.ApplicationService
import org.occurrent.application.service.blocking.execute
import org.occurrent.application.service.blocking.executePolicy
import org.occurrent.eventstore.api.WriteConditionNotFulfilledException
import org.occurrent.example.domain.wordguessinggame.event.DomainEvent
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.policy.WordHintPolicies
import org.occurrent.example.domain.wordguessinggame.writemodel.*
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.transaction.annotation.Transactional

open class GamePlayApplicationService constructor(private val applicationService: ApplicationService<DomainEvent>,
                                                  private val wordHintPolicies: WordHintPolicies) {

    @Transactional
    @Retryable(include = [WriteConditionNotFulfilledException::class], maxAttempts = 5, backoff = Backoff(delay = 100, multiplier = 2.0, maxDelay = 1000))
    open fun startGame(gameId: GameId, startTime: Timestamp, startedBy: PlayerId, wordsToChooseFrom: WordsToChooseFrom) {
        applicationService.execute(gameId, { events ->
            startGame(events, gameId, startTime, startedBy, wordsToChooseFrom, MaxNumberOfGuessesPerPlayer, MaxNumberOfGuessesTotal)
        }, executePolicy(wordHintPolicies::whenGameWasStartedThenRevealInitialCharactersInWordHint))
    }


    @Transactional
    @Retryable(include = [WriteConditionNotFulfilledException::class], maxAttempts = 5, backoff = Backoff(delay = 100, multiplier = 2.0, maxDelay = 1000))
    open fun makeGuess(gameId: GameId, timeOfGuess: Timestamp, playerId: PlayerId, word: Word) {
        applicationService.execute(gameId, { events -> guessWord(events, timeOfGuess, playerId, word) },
                executePolicy(wordHintPolicies::whenPlayerGuessedTheWrongWordThenRevealCharacterInWordHint))
    }
}
