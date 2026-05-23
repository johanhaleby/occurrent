package org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.gameplay.usecases

import org.occurrent.application.service.blocking.dcb.DcbApplicationService
import org.occurrent.dsl.dcb.blocking.execute
import org.occurrent.dsl.decider.Decider
import org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException
import org.occurrent.example.domain.wordguessinggame.event.GameEvent
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.dcb.GameDcbQueries
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.gameplay.decider.WordGuessingGameCommand
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.gameplay.decider.WordGuessingGameState
import org.occurrent.example.domain.wordguessinggame.writemodel.GameId
import org.occurrent.example.domain.wordguessinggame.writemodel.PlayerId
import org.occurrent.example.domain.wordguessinggame.writemodel.Timestamp
import org.occurrent.example.domain.wordguessinggame.writemodel.Word
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.util.UUID

@Service
class MakeGuess(
    private val applicationService: DcbApplicationService<GameEvent>,
    private val decider: Decider<WordGuessingGameCommand, WordGuessingGameState, GameEvent>
) {

    @Transactional
    @Retryable(include = [DcbAppendConditionNotFulfilledException::class], maxAttempts = 5, backoff = Backoff(delay = 100, multiplier = 2.0, maxDelay = 1000))
    operator fun invoke(gameId: GameId, timeOfGuess: Timestamp, playerId: PlayerId, word: Word) {
        applicationService.execute(
            GameDcbQueries.gameplay(gameId),
            WordGuessingGameCommand.GuessWord(
                guessEventId = UUID.randomUUID(),
                guessesExhaustedEventId = UUID.randomUUID(),
                gameEndedEventId = UUID.randomUUID(),
                timestamp = timeOfGuess,
                playerId = playerId,
                word = word
            ),
            decider
        )
    }
}
