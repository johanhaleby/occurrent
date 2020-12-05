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

package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.gameplay.views.ongoinggamesoverview

import org.occurrent.example.domain.wordguessinggame.readmodel.OngoingGameOverview
import org.springframework.data.annotation.Id
import org.springframework.data.annotation.TypeAlias
import org.springframework.data.mongodb.core.mapping.Document
import java.util.*


@Document("ongoingGames")
@TypeAlias("OngoingGameOverview")
data class OngoingGameOverviewMongoDTO(@Id val gameId: String, val category: String, val startedBy: String, val startedAt: Date)

fun OngoingGameOverview.toDTO(): OngoingGameOverviewMongoDTO = OngoingGameOverviewMongoDTO(gameId.toString(), category, startedBy.toString(), startedAt)
fun OngoingGameOverviewMongoDTO.toDomain(): OngoingGameOverview = OngoingGameOverview(UUID.fromString(gameId), category, UUID.fromString(startedBy), startedAt)