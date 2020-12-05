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

package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.emailwinner

import org.occurrent.example.domain.wordguessinggame.event.GameWasWon
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.support.Policies
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.support.loggerFor
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class SendEmailToWinner {
    private val log = loggerFor<SendEmailToWinner>()

    @Autowired
    lateinit var policies: Policies

    @Bean
    fun whenGameWasWonThenSendEmailToWinner() = policies.newPolicy<GameWasWon>("WhenGameWasWonThenSendEmailToWinnerPolicy") { gameWasWon ->
        log.info("Sending email to player ${gameWasWon.winnerId} since he/she was a winner of game ${gameWasWon.gameId}")
    }
}