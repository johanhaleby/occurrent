package se.haleby.occurrent.example.domain.numberguessinggame.model.domainevents;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;

public class PlayerGuessedANumberThatWasTooSmall implements GameEvent {
    private final UUID eventId;
    private final LocalDateTime timestamp;
    private final UUID gameId;
    private final UUID playerId;
    private final int guessedNumber;

    public PlayerGuessedANumberThatWasTooSmall(UUID eventId, UUID gameId, LocalDateTime timestamp, UUID playerId, int guessedNumber) {
        this.eventId = eventId;
        this.timestamp = timestamp;
        this.gameId = gameId;
        this.playerId = playerId;
        this.guessedNumber = guessedNumber;
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

    public UUID playerId() {
        return playerId;
    }

    public int guessedNumber() {
        return guessedNumber;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PlayerGuessedANumberThatWasTooSmall)) return false;
        PlayerGuessedANumberThatWasTooSmall that = (PlayerGuessedANumberThatWasTooSmall) o;
        return guessedNumber == that.guessedNumber &&
                Objects.equals(eventId, that.eventId) &&
                Objects.equals(timestamp, that.timestamp) &&
                Objects.equals(gameId, that.gameId) &&
                Objects.equals(playerId, that.playerId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, timestamp, gameId, playerId, guessedNumber);
    }

    @Override
    public String toString() {
        return "PlayerGuessedANumberThatWasTooSmall{" +
                "eventId=" + eventId +
                ", timestamp=" + timestamp +
                ", gameId=" + gameId +
                ", playerId=" + playerId +
                ", guessedNumber=" + guessedNumber +
                '}';
    }

}