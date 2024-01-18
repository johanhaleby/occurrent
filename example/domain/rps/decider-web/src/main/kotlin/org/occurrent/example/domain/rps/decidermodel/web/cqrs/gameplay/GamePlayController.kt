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
import org.occurrent.example.domain.rps.decidermodel.web.common.loggerFor
import org.springframework.hateoas.RepresentationModel
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.*

class PlayGameRepresentationModel : RepresentationModel<PlayGameRepresentationModel>()

@Controller
@RequestMapping(path = ["/games"])
class GamePlayController(private val applicationService: ApplicationService<GameEvent>) {
    private val log = loggerFor<GamePlayController>()


//    : ResponseEntity<EntityModel<PlayGameRepresentationModel>> {
    @PutMapping("{gameId}")
    fun initializeNewGame(@PathVariable("gameId") gameId: GameId, @RequestParam playerId: PlayerId) : ResponseEntity<Unit> {
        log.info("Initializing new game (gameId=$gameId)")

//        val self = linkTo(methodOn(GameViewController::class.java).showGame(gameId) as Any).withSelfRel()
//        val selfWithAffordances = self.andAffordance(afford(methodOn(GamePlayController::class.java).playGame(gameId, playerId, ROCK)))


//        val self = linkTo<GameViewController> { showGame(gameId) } withRel IanaLinkRelations.SELF
//        val selfWithAffordances = self andAffordances {
//            afford<GamePlayController> { playGame(gameId, playerId, ROCK) }
//        }
//
//        val model = EntityModel.of(PlayGameRepresentationModel())
//            .add(selfWithAffordances)

        val cmd = InitiateNewGame(gameId, Timestamp.now(), playerId)
        applicationService.execute(gameId, cmd, rps)

        return ResponseEntity.noContent().header("Location", "/games/$gameId").build()
//        return ResponseEntity.ok().header("Location", "/games/$gameId").body(model)
    }

    @PostMapping("{gameId}/play")
    fun playGame(@PathVariable("gameId") gameId: GameId, @RequestParam playerId: PlayerId, @RequestParam handGesture: HandGesture): ResponseEntity<Unit> {
        log.info("Playing game (gameId=$gameId)")
        val cmd = MakeHandGesture(gameId, Timestamp.now(), playerId, handGesture)
        val writeResult = applicationService.execute(gameId, cmd, rps)
        return ResponseEntity.noContent().header("Location", "/games/$gameId?version=${writeResult.newStreamVersion}").build()
    }
}