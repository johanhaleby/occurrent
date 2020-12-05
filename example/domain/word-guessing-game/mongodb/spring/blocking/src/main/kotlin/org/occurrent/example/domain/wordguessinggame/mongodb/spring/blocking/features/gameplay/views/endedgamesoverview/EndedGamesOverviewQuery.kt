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

package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.gameplay.views.endedgamesoverview

import org.occurrent.example.domain.wordguessinggame.readmodel.EndedGameOverview
import org.springframework.data.domain.Sort
import org.springframework.data.domain.Sort.Direction.DESC
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.stream
import org.springframework.stereotype.Component


@Component
class EndedGamesOverviewQuery(private val mongo: MongoOperations) {

    fun execute(numberOfGames: Int): Sequence<EndedGameOverview> =
        mongo.stream<EndedGameOverviewMongoDTO>(Query().with(Sort.by(DESC, "endedAt")).limit(numberOfGames))
            .asSequence()
            .map(EndedGameOverviewMongoDTO::toDomain)
}