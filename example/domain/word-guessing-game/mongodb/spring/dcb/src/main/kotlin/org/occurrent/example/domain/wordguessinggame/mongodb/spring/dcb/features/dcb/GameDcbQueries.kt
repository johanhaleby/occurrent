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

import org.occurrent.eventstore.api.dcb.DcbQuery
import org.occurrent.example.domain.wordguessinggame.event.CharacterInWordHintWasRevealed
import org.occurrent.example.domain.wordguessinggame.event.GameEvent
import org.occurrent.example.domain.wordguessinggame.event.GameWasStarted
import org.occurrent.example.domain.wordguessinggame.event.PlayerGuessedTheWrongWord
import org.occurrent.example.domain.wordguessinggame.event.PlayerWasAwardedPointsForGuessingTheRightWord
import org.occurrent.example.domain.wordguessinggame.event.PlayerWasNotAwardedAnyPointsForGuessingTheRightWord
import org.occurrent.example.domain.wordguessinggame.event.eventType
import java.util.UUID
import kotlin.reflect.KClass

internal object GameDcbQueries {
    fun allGameEvents(gameId: UUID): DcbQuery = DcbQuery.tags(GameDcbTags.game(gameId))

    fun gameplay(gameId: UUID): DcbQuery = DcbQuery.tags(GameDcbTags.gameplay(gameId))

    fun wordHintBoundary(gameId: UUID): DcbQuery = DcbQuery.anyOf(
            DcbQuery.type(GameWasStarted::class.eventType()).tags(GameDcbTags.game(gameId)),
            DcbQuery.type(CharacterInWordHintWasRevealed::class.eventType()).tags(GameDcbTags.wordHint(gameId))
    )

    fun pointsBoundary(gameId: UUID): DcbQuery = DcbQuery.anyOf(
            DcbQuery.type(GameWasStarted::class.eventType()).tags(GameDcbTags.game(gameId)),
            DcbQuery.type(PlayerGuessedTheWrongWord::class.eventType()).tags(GameDcbTags.gameplay(gameId)),
            DcbQuery.type(PlayerWasAwardedPointsForGuessingTheRightWord::class.eventType()).tags(GameDcbTags.points(gameId)),
            DcbQuery.type(PlayerWasNotAwardedAnyPointsForGuessingTheRightWord::class.eventType()).tags(GameDcbTags.points(gameId))
    )

    fun event(gameId: UUID, type: KClass<out GameEvent>): DcbQuery =
            DcbQuery.type(type.eventType()).tags(GameDcbTags.game(gameId))

    inline fun <reified E : GameEvent> event(gameId: UUID): DcbQuery = event(gameId, E::class)
}
