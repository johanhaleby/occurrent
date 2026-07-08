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

package org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.dcb

import org.occurrent.eventstore.api.dcb.DcbCriteria
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
    fun allGameEvents(gameId: UUID): DcbCriteria = DcbCriteria.tags(GameDcbTags.game(gameId))

    fun gameplay(gameId: UUID): DcbCriteria = DcbCriteria.tags(GameDcbTags.gameplay(gameId))

    fun wordHintCriteria(gameId: UUID): DcbCriteria = DcbCriteria.anyOf(
            DcbCriteria.type(GameWasStarted::class.eventType()).tags(GameDcbTags.game(gameId)),
            DcbCriteria.type(CharacterInWordHintWasRevealed::class.eventType()).tags(GameDcbTags.wordHint(gameId))
    )

    fun pointsCriteria(gameId: UUID): DcbCriteria = DcbCriteria.anyOf(
            DcbCriteria.type(GameWasStarted::class.eventType()).tags(GameDcbTags.game(gameId)),
            DcbCriteria.type(PlayerGuessedTheWrongWord::class.eventType()).tags(GameDcbTags.gameplay(gameId)),
            DcbCriteria.type(PlayerWasAwardedPointsForGuessingTheRightWord::class.eventType()).tags(GameDcbTags.points(gameId)),
            DcbCriteria.type(PlayerWasNotAwardedAnyPointsForGuessingTheRightWord::class.eventType()).tags(GameDcbTags.points(gameId))
    )

    fun event(gameId: UUID, type: KClass<out GameEvent>): DcbCriteria =
            DcbCriteria.type(type.eventType()).tags(GameDcbTags.game(gameId))

    inline fun <reified E : GameEvent> event(gameId: UUID): DcbCriteria = event(gameId, E::class)
}
