package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.policy

import org.occurrent.application.service.blocking.ApplicationService
import org.occurrent.application.service.blocking.execute
import org.occurrent.example.domain.wordguessinggame.event.GameEvent
import org.occurrent.example.domain.wordguessinggame.event.GameWasStarted
import org.occurrent.example.domain.wordguessinggame.event.PlayerGuessedTheRightWord
import org.occurrent.example.domain.wordguessinggame.event.PlayerGuessedTheWrongWord
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure.DomainEventQueries
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure.first
import org.occurrent.example.domain.wordguessinggame.writemodel.BasisForPointAwarding
import org.occurrent.example.domain.wordguessinggame.writemodel.PointAwarding
import org.occurrent.filter.Filter.streamId
import org.springframework.stereotype.Component

@Component
class PointAwardingPolicy(private val applicationService: ApplicationService<GameEvent>, private val domainEventQueries: DomainEventQueries) {

    fun whenPlayerGuessedTheRightWordThenAwardPointsToPlayerThatGuessedTheRightWord(playerGuessedTheRightWord: PlayerGuessedTheRightWord) {
        val gameId = playerGuessedTheRightWord.gameId
        val playerId = playerGuessedTheRightWord.playerId
        val eventsInGame = domainEventQueries.query<GameEvent>(streamId(gameId.toString())).toList()
        val gameWasStarted = eventsInGame.first<GameWasStarted>()
        val totalNumberGuessesForPlayerInGame = eventsInGame.count { event -> event is PlayerGuessedTheWrongWord && event.playerId == playerGuessedTheRightWord.playerId } + 1
        applicationService.execute("points:$gameId") { _: Sequence<GameEvent> ->
            val basis = BasisForPointAwarding(gameId, gameWasStarted.startedBy, playerId, totalNumberGuessesForPlayerInGame)
            PointAwarding.awardPointsToPlayerThatGuessedTheRightWord(basis)
        }
    }
}