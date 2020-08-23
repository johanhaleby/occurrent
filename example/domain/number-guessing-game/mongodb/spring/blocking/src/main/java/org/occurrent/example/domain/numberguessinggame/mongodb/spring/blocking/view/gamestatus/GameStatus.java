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

package org.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.view.gamestatus;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

public class GameStatus {
    public final UUID gameId;
    public final int maxNumberOfGuesses;
    public final int secretNumber;
    public final List<GuessAndTime> guesses;

    public GameStatus(UUID gameId, int secretNumber, int maxNumberOfGuesses, List<GuessAndTime> guesses) {
        this.gameId = gameId;
        this.maxNumberOfGuesses = maxNumberOfGuesses;
        this.guesses = guesses;
        this.secretNumber = secretNumber;
    }

    public int numberOfGuesses() {
        return guesses.size();
    }

    public boolean isEnded() {
        return guesses.size() == maxNumberOfGuesses || isSecretNumberGuessedByPlayer();
    }

    public int numberOfGuessesLeft() {
        return maxNumberOfGuesses - guesses.size();
    }

    public int lastGuess() {
        return guesses.isEmpty() ? 1 : guesses.get(guesses.size() - 1).guess;
    }

    public boolean isSecretNumberGuessedByPlayer() {
        return guesses.stream().anyMatch(guessAndTime -> guessAndTime.guess == secretNumber);
    }

    public static class GuessAndTime {
        public final int guess;
        public final LocalDateTime localDateTime;

        public GuessAndTime(int guess, LocalDateTime localDateTime) {
            this.guess = guess;
            this.localDateTime = localDateTime;
        }
    }
}