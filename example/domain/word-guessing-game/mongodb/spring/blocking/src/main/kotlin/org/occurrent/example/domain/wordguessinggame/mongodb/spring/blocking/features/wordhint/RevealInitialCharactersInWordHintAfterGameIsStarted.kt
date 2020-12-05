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

package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.wordhint

import org.occurrent.application.service.blocking.ApplicationService
import org.occurrent.application.service.blocking.execute
import org.occurrent.example.domain.wordguessinggame.event.GameEvent
import org.occurrent.example.domain.wordguessinggame.event.GameWasStarted
import org.occurrent.example.domain.wordguessinggame.writemodel.WordHintCharacterRevelation
import org.occurrent.example.domain.wordguessinggame.writemodel.WordHintData
import org.springframework.stereotype.Component


@Component
class RevealInitialCharactersInWordHintAfterGameIsStarted(private val applicationService: ApplicationService<GameEvent>) {

    operator fun invoke(gameWasStarted: GameWasStarted) {
        applicationService.execute("wordhint:${gameWasStarted.gameId}") { events: Sequence<GameEvent> ->
            if (events.toList().isEmpty()) {
                WordHintCharacterRevelation.revealInitialCharactersInWordHintWhenGameWasStarted(WordHintData(gameWasStarted.gameId, gameWasStarted.wordToGuess))
            } else {
                emptySequence()
            }
        }
    }
}