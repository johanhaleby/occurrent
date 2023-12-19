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

import org.occurrent.dsl.subscription.blocking.Subscriptions
import org.occurrent.dsl.view.MaterializedView
import org.occurrent.dsl.view.ViewStateRepository
import org.occurrent.dsl.view.updateView
import org.occurrent.dsl.view.view
import org.occurrent.example.domain.rps.decidermodel.*
import org.springframework.data.repository.CrudRepository
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

data class Move(val playerId: PlayerId, val handGesture: HandGesture)

sealed interface GameReadModel {
    data class Initialized(val initializedBy: PlayerId) : GameReadModel
    data class Ongoing(val firstMove: Move, val secondMove: Move? = null) : GameReadModel
    sealed interface Ended : GameReadModel {
        val firstPlayer: PlayerId
        val secondPlayer: PlayerId

        data class Tied(override val firstPlayer: PlayerId, override val secondPlayer: PlayerId) : Ended
        data class Won(override val firstPlayer: PlayerId, override val secondPlayer: PlayerId, val winner: PlayerId) : Ended
    }
}

@RestController
@RequestMapping(path = ["/games"], produces = [MediaType.APPLICATION_JSON_VALUE])
class GameViewController {

    @GetMapping("/{gameId}")
    fun game(@PathVariable("gameId") gameId: GameId): GameReadModel {
        TODO()
    }
}


private val gameView = view<GameReadModel?, GameEvent>(
    initialState = null,
    updateState = { game, e ->
        when (e) {
            is NewGameInitiated -> GameReadModel.Initialized(e.playerId)
            is GameStarted -> game
            is HandGestureShown -> when (game) {
                is GameReadModel.Initialized -> GameReadModel.Ongoing(firstMove = Move(e.player, e.gesture))
                is GameReadModel.Ongoing -> game.copy(secondMove = Move(e.player, e.gesture))
                else -> game
            }

            is GameEnded -> game
            is GameTied -> {
                val ongoingGame = game as GameReadModel.Ongoing
                GameReadModel.Ended.Tied(ongoingGame.firstMove.playerId, ongoingGame.secondMove!!.playerId)
            }

            is GameWon -> {
                val ongoingGame = game as GameReadModel.Ongoing
                GameReadModel.Ended.Won(ongoingGame.firstMove.playerId, ongoingGame.secondMove!!.playerId, e.winner)
            }
        }
    }
)

@Component
private interface GameRepository : CrudRepository<GameReadModel?, GameId>, ViewStateRepository<GameReadModel?, GameId>, MaterializedView<GameEvent> {
    override fun update(event: GameEvent) = updateFromRepository(event.gameId, event, gameView, this)
}

@Component
private class UpdateGameViewWhenGamePlayed(subscriptions: Subscriptions<GameEvent>, gameRepository: GameRepository) {
    init {
        subscriptions.updateView("gameView", gameRepository)
    }
}
