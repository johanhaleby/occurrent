package se.haleby.occurrent.example.domain.numberguessinggame.model.domainevents;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;

public class NumberGuessingGameEnded implements GameEvent {

    private final UUID eventId;
    private final LocalDateTime timestamp;
    private final UUID gameId;

    public NumberGuessingGameEnded(UUID eventId, UUID gameId, LocalDateTime timestamp) {
        this.eventId = eventId;
        this.timestamp = timestamp;
        this.gameId = gameId;
    }

    @Override
    public UUID eventId() {
        return gameId;
    }

    @Override
    public LocalDateTime timestamp() {
        return timestamp;
    }

    @Override
    public UUID gameId() {
        return gameId;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NumberGuessingGameEnded)) return false;
        NumberGuessingGameEnded that = (NumberGuessingGameEnded) o;
        return Objects.equals(eventId, that.eventId) &&
                Objects.equals(timestamp, that.timestamp) &&
                Objects.equals(gameId, that.gameId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, timestamp, gameId);
    }

    @Override
    public String toString() {
        return "NumberGuessingGameEnded{" +
                "eventId=" + eventId +
                ", timestamp=" + timestamp +
                ", gameId=" + gameId +
                '}';
    }

}