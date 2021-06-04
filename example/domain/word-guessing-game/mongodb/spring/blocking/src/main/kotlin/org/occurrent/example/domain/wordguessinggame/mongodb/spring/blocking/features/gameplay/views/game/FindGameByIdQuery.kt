/*
 * Copyright 2020 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.gameplay.views.game

import org.occurrent.example.domain.wordguessinggame.event.GameEvent
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.GameEventQueries
import org.occurrent.example.domain.wordguessinggame.readmodel.GameReadModel
import org.occurrent.example.domain.wordguessinggame.readmodel.assembleGameReadModelFromDomainEvents
import org.occurrent.filter.Filter.subject
import org.springframework.stereotype.Component
import java.util.*

@Component
class FindGameByIdQuery(private val gameEvents: GameEventQueries) {

    fun execute(gameId: UUID): GameReadModel? =
            // We use subject to retrieve _all_ game events from different streams
            gameEvents.query<GameEvent>(subject(gameId.toString()))
                    .assembleGameReadModelFromDomainEvents()
}