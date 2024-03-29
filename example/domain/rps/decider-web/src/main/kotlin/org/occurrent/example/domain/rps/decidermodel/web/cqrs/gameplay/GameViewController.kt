/*
 *
 *  Copyright 2023 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.example.domain.rps.decidermodel.web.cqrs.gameplay

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL
import org.occurrent.dsl.subscription.blocking.EventMetadata
import org.occurrent.dsl.subscription.blocking.Subscriptions
import org.occurrent.dsl.view.materialized
import org.occurrent.dsl.view.updateView
import org.occurrent.dsl.view.view
import org.occurrent.example.domain.rps.decidermodel.*
import org.occurrent.example.domain.rps.decidermodel.web.common.loggerFor
import org.occurrent.example.domain.rps.decidermodel.web.cqrs.gameplay.GameStatus.*
import org.springframework.data.annotation.Id
import org.springframework.data.annotation.TypeAlias
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.findById
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

data class Move(val playerId: PlayerId, val handGesture: HandGesture)

enum class GameStatus {
    Initialized, Ongoing, Tied, Won
}

@Document("game")
@JsonInclude(NON_NULL)
sealed interface GameReadModel {
    @get:Id
    val gameId: GameId
    val status: GameStatus
    val streamVersion: Long

    @TypeAlias("Initialized")
    data class Initialized(override val gameId: GameId, val initializedBy: PlayerId, override val status: GameStatus, override val streamVersion: Long) : GameReadModel

    @TypeAlias("Ongoing")
    data class Ongoing(override val gameId: GameId, val firstMove: Move, val secondMove: Move? = null, override val status: GameStatus, override val streamVersion: Long) : GameReadModel

    sealed interface Ended : GameReadModel {
        val firstMove: Move
        val secondMove: Move

        @TypeAlias("Tied")
        data class Tied(override val gameId: GameId, override val firstMove: Move, override val secondMove: Move, override val status: GameStatus, override val streamVersion: Long) : Ended

        @TypeAlias("Won")
        data class Won(override val gameId: GameId, override val firstMove: Move, override val secondMove: Move, val winner: PlayerId, override val status: GameStatus, override val streamVersion: Long) : Ended
    }
}

@RestController
@RequestMapping(path = ["/games"], produces = [MediaType.APPLICATION_JSON_VALUE])
class GameViewController(private val mongoOperations: MongoOperations) {

    @GetMapping("/{gameId}")
    fun showGame(@PathVariable("gameId") gameId: GameId): GameReadModel? = mongoOperations.findById(gameId)
}

private val gameView = view<GameReadModel?, Pair<EventMetadata, GameEvent>>(
    initialState = null,
    updateState = { game, (metadata, e) ->
        when (e) {
            is NewGameInitiated -> GameReadModel.Initialized(e.gameId, e.playerId, Initialized, metadata.streamVersion)
            is GameStarted -> game
            is HandGestureShown -> when (game) {
                is GameReadModel.Initialized -> GameReadModel.Ongoing(
                    e.gameId, firstMove = Move(e.player, e.gesture),
                    status = Ongoing, streamVersion = metadata.streamVersion
                )

                is GameReadModel.Ongoing -> game.copy(secondMove = Move(e.player, e.gesture))
                else -> game
            }

            is GameEnded -> game
            is GameTied -> {
                val ongoingGame = game as GameReadModel.Ongoing
                GameReadModel.Ended.Tied(e.gameId, ongoingGame.firstMove, ongoingGame.secondMove!!, Tied, streamVersion = metadata.streamVersion)
            }

            is GameWon -> {
                val ongoingGame = game as GameReadModel.Ongoing
                GameReadModel.Ended.Won(e.gameId, ongoingGame.firstMove, ongoingGame.secondMove!!, e.winner, Won, streamVersion = metadata.streamVersion)
            }
        }
    }
)

@Component
private class UpdateGameViewWhenGamePlayed(subscriptions: Subscriptions<GameEvent>, mongoOperations: MongoOperations) {
    private val log = loggerFor<UpdateGameViewWhenGamePlayed>()

    init {
        subscriptions.updateView(
            viewName = "gameView",
            converter = { eventMetadata, gameEvent ->
                eventMetadata to gameEvent
            },
            materializedView = gameView.materialized(mongoOperations) { (_, e) -> e.gameId }, doBeforeUpdate = { (_, e) ->
                log.info("Updating game view for game ${e.gameId} based on ${e::class.simpleName}")
            })
    }
}