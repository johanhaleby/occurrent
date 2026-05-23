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

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.occurrent.eventstore.api.dcb.DcbQuery
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
import org.occurrent.example.domain.wordguessinggame.event.ReasonForNotBeingAwardedPoints.PlayerCreatedListOfWords
import org.occurrent.example.domain.wordguessinggame.event.eventType
import java.util.Date
import java.util.UUID

class GameDcbHelpersTest {

    private val gameId = UUID.fromString("6fb6a720-99f0-4c56-80b3-9b32c9bf9f01")
    private val playerId = UUID.fromString("83fdb5bc-25ea-4487-a79c-341c5f05a2b6")
    private val timestamp = Date(0)

    @Test
    fun `creates stable DCB tags for a game`() {
        assertThat(GameDcbTags.game(gameId)).isEqualTo("game:$gameId")
        assertThat(GameDcbTags.gameplay(gameId)).isEqualTo("gameplay:$gameId")
        assertThat(GameDcbTags.wordHint(gameId)).isEqualTo("wordhint:$gameId")
        assertThat(GameDcbTags.points(gameId)).isEqualTo("points:$gameId")
    }

    @Test
    fun `tags every game event with game tag and boundary tag`() {
        val tagGenerator = GameEventTagGenerator()

        assertThat(allEvents().associate { it::class.simpleName to tagGenerator.tags(it) })
                .containsEntry(GameWasStarted::class.simpleName, setOf("game:$gameId", "gameplay:$gameId"))
                .containsEntry(PlayerGuessedTheWrongWord::class.simpleName, setOf("game:$gameId", "gameplay:$gameId"))
                .containsEntry(NumberOfGuessesWasExhaustedForPlayer::class.simpleName, setOf("game:$gameId", "gameplay:$gameId"))
                .containsEntry(PlayerGuessedTheRightWord::class.simpleName, setOf("game:$gameId", "gameplay:$gameId"))
                .containsEntry(GameWasWon::class.simpleName, setOf("game:$gameId", "gameplay:$gameId"))
                .containsEntry(GameWasLost::class.simpleName, setOf("game:$gameId", "gameplay:$gameId"))
                .containsEntry(CharacterInWordHintWasRevealed::class.simpleName, setOf("game:$gameId", "wordhint:$gameId"))
                .containsEntry(PlayerWasAwardedPointsForGuessingTheRightWord::class.simpleName, setOf("game:$gameId", "points:$gameId"))
                .containsEntry(PlayerWasNotAwardedAnyPointsForGuessingTheRightWord::class.simpleName, setOf("game:$gameId", "points:$gameId"))
    }

    @Test
    fun `maps query helpers to intended tags and event types`() {
        assertSingleQueryItem(GameDcbQueries.allGameEvents(gameId), tags = setOf("game:$gameId"))
        assertSingleQueryItem(GameDcbQueries.gameplay(gameId), tags = setOf("gameplay:$gameId"))
        assertSingleQueryItem(
                GameDcbQueries.event(gameId, GameWasWon::class),
                types = setOf(GameWasWon::class.eventType()),
                tags = setOf("game:$gameId")
        )

        assertThat(GameDcbQueries.wordHintDecisionContext(gameId).items()).containsExactlyInAnyOrder(
                queryItem(types = setOf(GameWasStarted::class.eventType()), tags = setOf("game:$gameId")),
                queryItem(types = setOf(CharacterInWordHintWasRevealed::class.eventType()), tags = setOf("wordhint:$gameId"))
        )

        assertThat(GameDcbQueries.pointsDecisionContext(gameId).items()).containsExactlyInAnyOrder(
                queryItem(types = setOf(GameWasStarted::class.eventType()), tags = setOf("game:$gameId")),
                queryItem(types = setOf(PlayerGuessedTheWrongWord::class.eventType()), tags = setOf("gameplay:$gameId")),
                queryItem(types = setOf(PlayerWasAwardedPointsForGuessingTheRightWord::class.eventType()), tags = setOf("points:$gameId")),
                queryItem(types = setOf(PlayerWasNotAwardedAnyPointsForGuessingTheRightWord::class.eventType()), tags = setOf("points:$gameId"))
        )
    }

    private fun assertSingleQueryItem(query: DcbQuery, types: Set<String> = emptySet(), tags: Set<String>) {
        assertThat(query.matchAll()).isFalse()
        assertThat(query.items()).containsExactly(queryItem(types, tags))
    }

    private fun queryItem(types: Set<String> = emptySet(), tags: Set<String>) = org.occurrent.eventstore.api.dcb.DcbQueryItem(types, tags)

    private fun allEvents(): List<GameEvent> = listOf(
            GameWasStarted(UUID.randomUUID(), timestamp, gameId, playerId, "programming", "occurrent", 3, 10),
            PlayerGuessedTheWrongWord(UUID.randomUUID(), timestamp, gameId, playerId, "eventstore"),
            NumberOfGuessesWasExhaustedForPlayer(UUID.randomUUID(), timestamp, gameId, playerId),
            PlayerGuessedTheRightWord(UUID.randomUUID(), timestamp, gameId, playerId, "occurrent"),
            GameWasWon(UUID.randomUUID(), timestamp, gameId, playerId),
            GameWasLost(UUID.randomUUID(), timestamp, gameId),
            CharacterInWordHintWasRevealed(UUID.randomUUID(), timestamp, gameId, 'o', 1),
            PlayerWasAwardedPointsForGuessingTheRightWord(UUID.randomUUID(), timestamp, gameId, playerId, 5),
            PlayerWasNotAwardedAnyPointsForGuessingTheRightWord(UUID.randomUUID(), timestamp, gameId, playerId, PlayerCreatedListOfWords)
    )
}
