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
import org.occurrent.example.domain.wordguessinggame.readmodel.LostGameOverview
import org.occurrent.example.domain.wordguessinggame.readmodel.WonGameOverview
import org.springframework.data.annotation.Id
import org.springframework.data.annotation.TypeAlias
import org.springframework.data.mongodb.core.mapping.Document
import java.util.*


@Document("endedGames")
@TypeAlias("EndedGameOverview")
internal data class EndedGameOverviewMongoDTO(
    @Id val gameId: String, val category: String, val startedBy: String, val startedAt: Date,
    val endedAt: Date, val wordToGuess: String, val winnerId: String? = null
)

internal fun EndedGameOverview.toDTO(): EndedGameOverviewMongoDTO {
    val winnerId = if (this is WonGameOverview) {
        winnerId.toString()
    } else {
        null
    }
    return EndedGameOverviewMongoDTO(gameId.toString(), category, startedBy.toString(), startedAt, endedAt, wordToGuess, winnerId)
}

internal fun EndedGameOverviewMongoDTO.toDomain(): EndedGameOverview = if (winnerId == null) {
    LostGameOverview(UUID.fromString(gameId), category, UUID.fromString(startedBy), startedAt, endedAt, wordToGuess)
} else {
    WonGameOverview(UUID.fromString(gameId), category, UUID.fromString(startedBy), startedAt, endedAt, wordToGuess, UUID.fromString(winnerId))
}