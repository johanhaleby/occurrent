package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.policy

import org.occurrent.application.service.blocking.ApplicationService
import org.occurrent.application.service.blocking.execute
import org.occurrent.example.domain.wordguessinggame.event.*
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure.DomainEventQueries
import org.occurrent.example.domain.wordguessinggame.writemodel.WordHintCharacterRevelation
import org.occurrent.example.domain.wordguessinggame.writemodel.WordHintData
import org.occurrent.filter.Filter.streamId
import org.occurrent.filter.Filter.type
import org.springframework.stereotype.Component

@Component
class WordHintPolicies(private val applicationService: ApplicationService<GameEvent>, private val domainEventQueries: DomainEventQueries) {

    fun whenGameWasStartedThenRevealInitialCharactersInWordHint(gameWasStarted: GameWasStarted) {
        applicationService.execute("wordhint:${gameWasStarted.gameId}") { events: Sequence<GameEvent> ->
            if (events.toList().isEmpty()) {
                WordHintCharacterRevelation.revealInitialCharactersInWordHintWhenGameWasStarted(WordHintData(gameWasStarted.gameId, gameWasStarted.wordToGuess))
            } else {
                emptySequence()
            }
        }
    }

    fun whenPlayerGuessedTheWrongWordThenRevealCharacterInWordHint(playerGuessedTheWrongWord: PlayerGuessedTheWrongWord) {
        val gameId = playerGuessedTheWrongWord.gameId
        val gameWasStarted = domainEventQueries.queryOne<GameWasStarted>(streamId(gameId.toString()).and(type(GameWasStarted::class.eventType())))
        applicationService.execute("wordhint:$gameId") { events: Sequence<GameEvent> ->
            val characterPositionsInWord = events.map { it as CharacterInWordHintWasRevealed }.map { it.characterPositionInWord }.toSet()
            val wordHintData = WordHintData(gameId, wordToGuess = gameWasStarted.wordToGuess, currentlyRevealedPositions = characterPositionsInWord)
            WordHintCharacterRevelation.revealCharacterInWordHintWhenPlayerGuessedTheWrongWord(wordHintData)
        }
    }
}