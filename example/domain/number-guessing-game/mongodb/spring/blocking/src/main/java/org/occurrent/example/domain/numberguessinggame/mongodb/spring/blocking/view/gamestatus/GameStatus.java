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