package se.haleby.occurrent.example.domain.numberguessinggame.mongodb.nativedriver.integrationevent;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

@SuppressWarnings("unused")
public class NumberGuessingGameCompleted {
    private String gameId;
    private Date startedAt;
    private Date endedAt;
    private int secretNumberToGuess;
    private int maxNumberOfGuesses;
    private List<GuessedNumber> guesses = new ArrayList<>();
    private boolean rightNumberWasGuessed;

    public NumberGuessingGameCompleted() {
    }

    public NumberGuessingGameCompleted(String gameId, Date startedAt, Date endedAt, int secretNumberToGuess, int maxNumberOfGuesses,
                                       List<GuessedNumber> guesses, boolean rightNumberWasGuessed) {
        this.gameId = gameId;
        this.startedAt = startedAt;
        this.endedAt = endedAt;
        this.secretNumberToGuess = secretNumberToGuess;
        this.maxNumberOfGuesses = maxNumberOfGuesses;
        this.guesses = guesses;
        this.rightNumberWasGuessed = rightNumberWasGuessed;
    }

    public String getGameId() {
        return gameId;
    }

    public void setGameId(String gameId) {
        this.gameId = gameId;
    }

    public Date getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(Date startedAt) {
        this.startedAt = startedAt;
    }

    public Date getEndedAt() {
        return endedAt;
    }

    public void setEndedAt(Date endedAt) {
        this.endedAt = endedAt;
    }

    public int getSecretNumberToGuess() {
        return secretNumberToGuess;
    }

    public void setSecretNumberToGuess(int secretNumberToGuess) {
        this.secretNumberToGuess = secretNumberToGuess;
    }

    public int getMaxNumberOfGuesses() {
        return maxNumberOfGuesses;
    }

    public void setMaxNumberOfGuesses(int maxNumberOfGuesses) {
        this.maxNumberOfGuesses = maxNumberOfGuesses;
    }

    public List<GuessedNumber> getGuesses() {
        return guesses;
    }

    public void setGuesses(List<GuessedNumber> guesses) {
        this.guesses = guesses;
    }

    public boolean isRightNumberWasGuessed() {
        return rightNumberWasGuessed;
    }

    public void setRightNumberWasGuessed(boolean rightNumberWasGuessed) {
        this.rightNumberWasGuessed = rightNumberWasGuessed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NumberGuessingGameCompleted)) return false;
        NumberGuessingGameCompleted that = (NumberGuessingGameCompleted) o;
        return secretNumberToGuess == that.secretNumberToGuess &&
                maxNumberOfGuesses == that.maxNumberOfGuesses &&
                rightNumberWasGuessed == that.rightNumberWasGuessed &&
                Objects.equals(gameId, that.gameId) &&
                Objects.equals(startedAt, that.startedAt) &&
                Objects.equals(endedAt, that.endedAt) &&
                Objects.equals(guesses, that.guesses);
    }

    @Override
    public int hashCode() {
        return Objects.hash(gameId, startedAt, endedAt, secretNumberToGuess, maxNumberOfGuesses, guesses, rightNumberWasGuessed);
    }

    @Override
    public String toString() {
        return "NumberGuessingGameCompleted{" +
                "gameId='" + gameId + '\'' +
                ", startedAt=" + startedAt +
                ", endedAt=" + endedAt +
                ", secretNumberToGuess=" + secretNumberToGuess +
                ", maxAttempts=" + maxNumberOfGuesses +
                ", guesses=" + guesses +
                ", rightNumberWasGuessed=" + rightNumberWasGuessed +
                '}';
    }

    public void addGuess(GuessedNumber guessedNumber) {
        this.guesses.add(guessedNumber);
    }

    public static class GuessedNumber {
        private String playerId;
        private int number;
        private Date guessedAt;

        GuessedNumber() {
        }

        public GuessedNumber(String playerId, int number, Date guessedAt) {
            this.playerId = playerId;
            this.number = number;
            this.guessedAt = guessedAt;
        }

        public String getPlayerId() {
            return playerId;
        }

        public void setPlayerId(String playerId) {
            this.playerId = playerId;
        }

        public int getNumber() {
            return number;
        }

        public void setNumber(int number) {
            this.number = number;
        }

        public Date getGuessedAt() {
            return guessedAt;
        }

        public void setGuessedAt(Date guessedAt) {
            this.guessedAt = guessedAt;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof GuessedNumber)) return false;
            GuessedNumber that = (GuessedNumber) o;
            return number == that.number &&
                    Objects.equals(playerId, that.playerId) &&
                    Objects.equals(guessedAt, that.guessedAt);
        }

        @Override
        public int hashCode() {
            return Objects.hash(playerId, number, guessedAt);
        }

        @Override
        public String toString() {
            return "GuessedNumber{" +
                    "playerId='" + playerId + '\'' +
                    ", number=" + number +
                    ", guessedAt=" + guessedAt +
                    '}';
        }
    }
}