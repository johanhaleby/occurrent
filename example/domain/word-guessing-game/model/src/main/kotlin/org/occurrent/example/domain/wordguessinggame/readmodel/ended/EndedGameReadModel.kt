package org.occurrent.example.domain.wordguessinggame.readmodel.ended

import java.util.*

class EndedGameReadModel internal constructor(val gameId: UUID, val startedAt: Date, val endedAt: Date,
                                              val category: String, val numberOfGuesses: Int, val wordToGuess: String,
                                              val winner: UUID? = null)