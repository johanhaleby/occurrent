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
import org.occurrent.example.domain.numberguessinggame.model.domainevents.NumberGuessingGameWasStarted;
import org.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.view.numberofstartedgames.NumberOfStartedGames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import static org.occurrent.annotation.Subscription.ResumeBehavior.SAME_AS_START_AT;
import static org.occurrent.annotation.Subscription.StartPosition.BEGINNING_OF_TIME;
import static org.occurrent.annotation.Subscription.WaitUntilStarted.TRUE;

@Component
public class IncreaseNumberOfStartedGames {
    private static final Logger log = LoggerFactory.getLogger(IncreaseNumberOfStartedGames.class);
    private final NumberOfStartedGames numberOfStartedGames;

    public IncreaseNumberOfStartedGames(NumberOfStartedGames numberOfStartedGames) {
        this.numberOfStartedGames = numberOfStartedGames;
    }

    @Subscription(id = "countNumberOfStartedGames", startAt = BEGINNING_OF_TIME, resumeBehavior = SAME_AS_START_AT, waitUntilStarted = TRUE)
    void countGameStarted(NumberGuessingGameWasStarted ignored) {
        numberOfStartedGames.increaseNumberOfStartedGames();
        log.info("Number of started games {}", numberOfStartedGames.get());
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}