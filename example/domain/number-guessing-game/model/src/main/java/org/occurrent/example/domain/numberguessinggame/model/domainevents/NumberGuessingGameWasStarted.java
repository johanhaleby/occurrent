package org.occurrent.example.domain.numberguessinggame.model.domainevents;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;

public class NumberGuessingGameWasStarted implements GameEvent {

    private final UUID eventId;
    private final LocalDateTime timestamp;
    private final UUID gameId;
    private final UUID startedBy;
    private final int secretNumberToGuess;
    private final int maxNumberOfGuesses;

    public NumberGuessingGameWasStarted(UUID eventId, UUID gameId, LocalDateTime timestamp, UUID startedBy,
                                        int secretNumberToGuess, int maxNumberOfGuesses) {
        this.eventId = eventId;
        this.timestamp = timestamp;
        this.gameId = gameId;
        this.startedBy = startedBy;
        this.secretNumberToGuess = secretNumberToGuess;
        this.maxNumberOfGuesses = maxNumberOfGuesses;
    }

    @Override
    public UUID eventId() {
        return eventId;
    }

    @Override
    public LocalDateTime timestamp() {
        return timestamp;
    }

    @Override
    public UUID gameId() {
        return gameId;
    }

    public UUID startedBy() {
        return startedBy;
    }

    public int secretNumberToGuess() {
        return secretNumberToGuess;
    }

    public int maxNumberOfGuesses() {
        return maxNumberOfGuesses;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NumberGuessingGameWasStarted)) return false;
        NumberGuessingGameWasStarted that = (NumberGuessingGameWasStarted) o;
        return secretNumberToGuess == that.secretNumberToGuess &&
                maxNumberOfGuesses == that.maxNumberOfGuesses &&
                Objects.equals(eventId, that.eventId) &&
                Objects.equals(timestamp, that.timestamp) &&
                Objects.equals(gameId, that.gameId) &&
                Objects.equals(startedBy, that.startedBy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, timestamp, gameId, startedBy, secretNumberToGuess, maxNumberOfGuesses);
    }

    @Override
    public String toString() {
        return "NumberGuessingGameWasStarted{" +
                "eventId=" + eventId +
                ", timestamp=" + timestamp +
                ", gameId=" + gameId +
                ", startedBy=" + startedBy +
                ", secretNumberToGuess=" + secretNumberToGuess +
                ", maxNumberOfGuesses=" + maxNumberOfGuesses +
                '}';
    }
}