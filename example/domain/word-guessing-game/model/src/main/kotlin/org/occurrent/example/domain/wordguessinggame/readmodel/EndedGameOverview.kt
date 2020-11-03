package org.occurrent.example.domain.wordguessinggame.readmodel

import java.util.*

sealed class EndedGameOverview {
    abstract val gameId: UUID
    abstract val category: String
    abstract val startedBy: UUID
    abstract val startedAt: Date
    abstract val endedAt: Date
    abstract val wordToGuess: String
}

data class WonGameOverview(override val gameId: UUID, override val category: String, override val startedBy: UUID, override val startedAt: Date,
                           override val endedAt: Date, override val wordToGuess: String, val winnerId: UUID) : EndedGameOverview()

data class LostGameOverview(override val gameId: UUID, override val category: String, override val startedBy: UUID, override val startedAt: Date,
                            override val endedAt: Date, override val wordToGuess: String) : EndedGameOverview()