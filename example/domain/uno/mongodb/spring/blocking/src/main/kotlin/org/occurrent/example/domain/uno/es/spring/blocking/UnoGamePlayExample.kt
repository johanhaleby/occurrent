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

package org.occurrent.example.domain.uno.es.spring.blocking

import org.occurrent.application.composition.command.partial
import org.occurrent.example.domain.uno.*
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import javax.annotation.PostConstruct


@Component
class UnoGamePlayExample(private val applicationService: UnoApplicationService) {
    private val log = loggerFor<UnoGamePlayExample>()

    @EventListener(ApplicationReadyEvent::class)
    fun playAGameOfUno() {
        log.info("Playing a game of UNO")

        val gameId = GameId.randomUUID()

        val commands = listOf(
            Uno::start.partial(gameId, Timestamp.now(), 4, Card.DigitCard(Digit.Three, Color.Red)),
            Uno::play.partial(Timestamp.now(), 0, Card.DigitCard(Digit.Three, Color.Blue)),
            Uno::play.partial(Timestamp.now(), 1, Card.DigitCard(Digit.Eight, Color.Blue)),
            Uno::play.partial(Timestamp.now(), 2, Card.DigitCard(Digit.Eight, Color.Yellow)),
            Uno::play.partial(Timestamp.now(), 0, Card.DigitCard(Digit.Four, Color.Green))
        )

        commands.forEach { command ->
            applicationService.execute(gameId, command)
        }
    }
}