package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.persistence

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