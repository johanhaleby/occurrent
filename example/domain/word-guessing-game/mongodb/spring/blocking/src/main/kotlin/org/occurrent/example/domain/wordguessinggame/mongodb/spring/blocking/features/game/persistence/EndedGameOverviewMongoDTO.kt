package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.persistence

import org.occurrent.example.domain.wordguessinggame.readmodel.EndedGameOverview
import org.occurrent.example.domain.wordguessinggame.readmodel.LostGameOverview
import org.occurrent.example.domain.wordguessinggame.readmodel.WonGameOverview
import org.springframework.data.annotation.Id
import org.springframework.data.annotation.TypeAlias
import org.springframework.data.mongodb.core.mapping.Document
import java.util.*

@Document("endedGames")
@TypeAlias("EndedGameOverview")
data class EndedGameOverviewMongoDTO(@Id val gameId: String, val category: String, val startedBy: String, val startedAt: Date,
                                     val endedAt: Date, val wordToGuess: String, val winnerId: String? = null)

fun EndedGameOverview.toDTO(): EndedGameOverviewMongoDTO {
    val winnerId = if (this is WonGameOverview) {
        winnerId.toString()
    } else {
        null
    }
    return EndedGameOverviewMongoDTO(gameId.toString(), category, startedBy.toString(), startedAt, endedAt, wordToGuess, winnerId)
}

fun EndedGameOverviewMongoDTO.toDomain(): EndedGameOverview = if (winnerId == null) {
    LostGameOverview(UUID.fromString(gameId), category, UUID.fromString(startedBy), startedAt, endedAt, wordToGuess)
} else {
    WonGameOverview(UUID.fromString(gameId), category, UUID.fromString(startedBy), startedAt, endedAt, wordToGuess, UUID.fromString(winnerId))
}