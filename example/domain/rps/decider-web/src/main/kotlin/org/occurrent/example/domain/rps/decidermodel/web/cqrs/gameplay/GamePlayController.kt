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

import org.occurrent.application.service.blocking.ApplicationService
import org.occurrent.dsl.decider.execute
import org.occurrent.example.domain.rps.decidermodel.*
import org.springframework.http.MediaType.APPLICATION_JSON_VALUE
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam


@Controller
@RequestMapping(path = ["/games"], produces = [APPLICATION_JSON_VALUE])
class GamePlayController(private val applicationService: ApplicationService<GameEvent>) {


    @PutMapping("{gameId}")
    fun initializeNewGame(@PathVariable("gameId") gameId: GameId, @RequestParam playerId: PlayerId): ResponseEntity<Unit> {
        val cmd = InitiateNewGame(gameId, Timestamp.now(), playerId)
        applicationService.execute(gameId, cmd, rps)
        return ResponseEntity.noContent().header("Location", "/games/$gameId").build()
    }

    @PutMapping("{gameId}")
    fun playGame(@PathVariable("gameId") gameId: GameId, @RequestParam playerId: PlayerId, @RequestParam handGesture: HandGesture): ResponseEntity<Unit> {
        val cmd = MakeHandGesture(gameId, Timestamp.now(), playerId, handGesture)
        applicationService.execute(gameId, cmd, rps)
        return ResponseEntity.noContent().header("Location", "/games/$gameId").build()
    }
}