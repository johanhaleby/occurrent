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

package org.occurrent.example.domain.rps.decidermodel.web.welcome

import org.occurrent.condition.Condition.or
import org.occurrent.dsl.query.blocking.DomainEventQueries
import org.occurrent.dsl.query.blocking.queryForSequence
import org.occurrent.eventstore.api.SortBy
import org.occurrent.eventstore.api.SortBy.SortDirection.DESCENDING
import org.occurrent.example.domain.rps.decidermodel.GameEvent
import org.occurrent.example.domain.rps.decidermodel.GameTied
import org.occurrent.example.domain.rps.decidermodel.GameWon
import org.occurrent.filter.Filter.type
import org.springframework.stereotype.Controller
import org.springframework.ui.ModelMap
import org.springframework.web.bind.annotation.GetMapping
import java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME

@Controller
//class WelcomeController(private val domainEventQueries: DomainEventQueries<GameEvent>) {
class WelcomeController() {

    @GetMapping("/welcome")
    fun welcomePage(modelMap: ModelMap): String {
        data class GameOutcome(val id: String, val endedAt: String, val winner: String? = null)

//        val games = domainEventQueries.queryForSequence(
//            type(or(GameTied::class.simpleName, GameWon::class.simpleName)),
//            limit = 10, sortBy = SortBy.time(DESCENDING)
//        ).map { event: GameEvent ->
//            val gameId = event.gameId.toString()
//            val endedAt = event.timestamp.format(ISO_LOCAL_DATE_TIME)
//            when (event) {
//                is GameWon -> GameOutcome(gameId, endedAt, event.winner.toString())
//                is GameTied -> GameOutcome(gameId, endedAt)
//                else -> IllegalStateException("Invalid ${event::class.simpleName}")
//            }
//        }.toList()
        modelMap["games"] = emptyList<Any>()
        return "welcome"
    }
}