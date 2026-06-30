/*
 * Copyright 2026 Johan Haleby
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

package org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.features.dcb

import org.occurrent.application.service.dcb.TagGenerator
import org.occurrent.example.domain.wordguessinggame.event.CharacterInWordHintWasRevealed
import org.occurrent.example.domain.wordguessinggame.event.GameEvent
import org.occurrent.example.domain.wordguessinggame.event.GameWasLost
import org.occurrent.example.domain.wordguessinggame.event.GameWasStarted
import org.occurrent.example.domain.wordguessinggame.event.GameWasWon
import org.occurrent.example.domain.wordguessinggame.event.NumberOfGuessesWasExhaustedForPlayer
import org.occurrent.example.domain.wordguessinggame.event.PlayerGuessedTheRightWord
import org.occurrent.example.domain.wordguessinggame.event.PlayerGuessedTheWrongWord
import org.occurrent.example.domain.wordguessinggame.event.PlayerWasAwardedPointsForGuessingTheRightWord
import org.occurrent.example.domain.wordguessinggame.event.PlayerWasNotAwardedAnyPointsForGuessingTheRightWord

internal class GameEventTagGenerator : TagGenerator<GameEvent> {
    override fun tags(event: GameEvent): Set<String> = buildSet {
        add(GameDcbTags.game(event.gameId))
        add(when (event) {
            is GameWasStarted,
            is PlayerGuessedTheWrongWord,
            is NumberOfGuessesWasExhaustedForPlayer,
            is PlayerGuessedTheRightWord,
            is GameWasWon,
            is GameWasLost -> GameDcbTags.gameplay(event.gameId)
            is CharacterInWordHintWasRevealed -> GameDcbTags.wordHint(event.gameId)
            is PlayerWasAwardedPointsForGuessingTheRightWord,
            is PlayerWasNotAwardedAnyPointsForGuessingTheRightWord -> GameDcbTags.points(event.gameId)
        })
    }
}
