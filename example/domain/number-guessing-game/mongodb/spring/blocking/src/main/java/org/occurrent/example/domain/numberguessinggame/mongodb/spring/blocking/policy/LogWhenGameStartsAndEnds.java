/*
 *
 *  Copyright 2024 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.policy;

import org.occurrent.annotation.Subscription;
import org.occurrent.example.domain.numberguessinggame.model.domainevents.GameEvent;
import org.occurrent.example.domain.numberguessinggame.model.domainevents.GuessingAttemptsExhausted;
import org.occurrent.example.domain.numberguessinggame.model.domainevents.NumberGuessingGameWasStarted;
import org.occurrent.example.domain.numberguessinggame.model.domainevents.PlayerGuessedTheRightNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class LogWhenGameStartsAndEnds {
    private static final Logger log = LoggerFactory.getLogger(LogWhenGameStartsAndEnds.class);

    @Subscription(
            id = "LogWhenGameStartsAndEnds",
            eventTypes = {NumberGuessingGameWasStarted.class, PlayerGuessedTheRightNumber.class, GuessingAttemptsExhausted.class}
    )
    void logWhenGameStartsAndEnds(GameEvent game) {
        if (game instanceof NumberGuessingGameWasStarted started) {
            log.info("Game started: {} (startedBy={}, secretNumber={})", started.gameId(), started.startedBy(), started.secretNumberToGuess());
        } else if (game instanceof PlayerGuessedTheRightNumber) {
            log.info("Game won: {}", game.gameId());
        } else if (game instanceof GuessingAttemptsExhausted) {
            log.info("Game lost: {}", game.gameId());
        } else {
            throw new IllegalStateException("Unsupported game event: " + game);
        }
    }

}
