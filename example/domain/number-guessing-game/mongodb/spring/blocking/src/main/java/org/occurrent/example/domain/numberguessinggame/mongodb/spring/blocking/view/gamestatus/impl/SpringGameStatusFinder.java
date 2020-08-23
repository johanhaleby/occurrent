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

package org.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.view.gamestatus.impl;

import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.example.domain.numberguessinggame.model.domainevents.NumberGuessingGameWasStarted;
import org.occurrent.example.domain.numberguessinggame.model.domainevents.PlayerGuessedANumberThatWasTooBig;
import org.occurrent.example.domain.numberguessinggame.model.domainevents.PlayerGuessedANumberThatWasTooSmall;
import org.occurrent.example.domain.numberguessinggame.model.domainevents.PlayerGuessedTheRightNumber;
import org.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.infrastructure.Serialization;
import org.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.view.gamestatus.GameStatus;
import org.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.view.gamestatus.WhatIsTheStatusOfGame;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.occurrent.filter.Filter.subject;

@Component
class SpringGameStatusFinder implements WhatIsTheStatusOfGame {

    private final EventStoreQueries queries;
    private final Serialization serialization;

    SpringGameStatusFinder(EventStoreQueries queries, Serialization serialization) {
        this.queries = queries;
        this.serialization = serialization;
    }

    @Override
    public Optional<GameStatus> findFor(UUID gameId) {
        return queries.query(subject(gameId.toString()))
                .map(serialization::deserialize)
                .collect(GameStatusBuilder::new,
                        (gameStatus, event) -> {
                            if (event instanceof NumberGuessingGameWasStarted) {
                                gameStatus.gameId = event.gameId();
                                gameStatus.secretNumber = ((NumberGuessingGameWasStarted) event).secretNumberToGuess();
                                gameStatus.maxNumberOfGuesses = ((NumberGuessingGameWasStarted) event).maxNumberOfGuesses();
                            } else if (event instanceof PlayerGuessedANumberThatWasTooSmall) {
                                PlayerGuessedANumberThatWasTooSmall e = (PlayerGuessedANumberThatWasTooSmall) event;
                                gameStatus.guesses.add(new GameStatus.GuessAndTime(e.guessedNumber(), e.timestamp()));
                            } else if (event instanceof PlayerGuessedANumberThatWasTooBig) {
                                PlayerGuessedANumberThatWasTooBig e = (PlayerGuessedANumberThatWasTooBig) event;
                                gameStatus.guesses.add(new GameStatus.GuessAndTime(e.guessedNumber(), e.timestamp()));
                            } else if (event instanceof PlayerGuessedTheRightNumber) {
                                PlayerGuessedTheRightNumber e = (PlayerGuessedTheRightNumber) event;
                                gameStatus.guesses.add(new GameStatus.GuessAndTime(e.guessedNumber(), e.timestamp()));
                            }
                        }, (gameStatus, gameStatus2) -> {
                        })
                .build();
    }

    private static class GameStatusBuilder {
        public UUID gameId;
        public int maxNumberOfGuesses;
        public int secretNumber;
        public List<GameStatus.GuessAndTime> guesses = new ArrayList<>();

        private Optional<GameStatus> build() {
            return gameId == null ? Optional.empty() : Optional.of(new GameStatus(gameId, secretNumber, maxNumberOfGuesses, guesses));
        }
    }

}
