package org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.features.gameplay.usecases

import org.occurrent.example.domain.wordguessinggame.writemodel.GameId
import org.occurrent.example.domain.wordguessinggame.writemodel.PlayerId
import org.occurrent.example.domain.wordguessinggame.writemodel.Timestamp
import org.occurrent.example.domain.wordguessinggame.writemodel.WordList

fun interface StartGame {
    operator fun invoke(gameId: GameId, startTime: Timestamp, startedBy: PlayerId, wordList: WordList)
}
