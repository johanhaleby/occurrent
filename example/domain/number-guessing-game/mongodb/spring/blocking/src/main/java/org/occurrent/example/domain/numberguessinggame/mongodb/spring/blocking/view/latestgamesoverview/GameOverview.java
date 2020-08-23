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

package org.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.view.latestgamesoverview;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;

public class GameOverview {
    public final UUID gameId;
    public final LocalDateTime startedAt;
    public final GameState state;

    public GameOverview(UUID gameId, LocalDateTime startedAt, GameState gameState) {
        this.gameId = gameId;
        this.startedAt = startedAt;
        this.state = gameState;
    }

    public abstract static class GameState {
        private GameState() {
        }

        public static class Ongoing extends GameState {
            public final int numberOfAttemptsLeft;

            public Ongoing(int numberOfAttemptsLeft) {
                this.numberOfAttemptsLeft = numberOfAttemptsLeft;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (!(o instanceof Ongoing)) return false;
                Ongoing ongoing = (Ongoing) o;
                return numberOfAttemptsLeft == ongoing.numberOfAttemptsLeft;
            }

            @Override
            public int hashCode() {
                return Objects.hash(numberOfAttemptsLeft);
            }

            @Override
            public String toString() {
                return "Ongoing{" +
                        "numberOfAttemptsLeft=" + numberOfAttemptsLeft +
                        '}';
            }
        }

        public static class Ended extends GameState {
            public final LocalDateTime endedAt;
            public final boolean playerGuessedTheRightNumber;

            public Ended(LocalDateTime endedAt, boolean playerGuessedTheRightNumber) {
                this.endedAt = endedAt;
                this.playerGuessedTheRightNumber = playerGuessedTheRightNumber;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (!(o instanceof Ended)) return false;
                Ended ended = (Ended) o;
                return playerGuessedTheRightNumber == ended.playerGuessedTheRightNumber &&
                        Objects.equals(endedAt, ended.endedAt);
            }

            @Override
            public int hashCode() {
                return Objects.hash(endedAt, playerGuessedTheRightNumber);
            }

            @Override
            public String toString() {
                return "Ended{" +
                        "endedAt=" + endedAt +
                        ", playerGuessedTheRightNumber=" + playerGuessedTheRightNumber +
                        '}';
            }
        }
    }
}