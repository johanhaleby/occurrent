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
package org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.emailwinner

import org.occurrent.annotation.Subscription
import org.occurrent.example.domain.wordguessinggame.event.GameWasWon
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.support.loggerFor
import org.springframework.stereotype.Component

@Component
class SendEmailToWinner {
    private val log = loggerFor<SendEmailToWinner>()

    @Subscription(id = "WhenGameWasWonThenSendEmailToWinnerPolicy")
    fun whenGameWasWon(gameWasWon: GameWasWon) {
        log.info("Sending email to player ${gameWasWon.winnerId} since he/she was a winner of game ${gameWasWon.gameId}")
    }
}
