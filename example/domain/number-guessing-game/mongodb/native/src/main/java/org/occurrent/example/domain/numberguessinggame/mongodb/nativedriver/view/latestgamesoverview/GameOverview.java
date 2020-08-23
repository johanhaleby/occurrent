package org.occurrent.example.domain.numberguessinggame.mongodb.nativedriver.view.latestgamesoverview;

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