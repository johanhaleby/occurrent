package org.occurrent.example.domain.numberguessinggame.model;

import java.util.Objects;
import java.util.UUID;

public class GameNotStarted extends RuntimeException {

    public final UUID gameId;

    public GameNotStarted(UUID gameId) {
        this.gameId = gameId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GameNotStarted)) return false;
        GameNotStarted that = (GameNotStarted) o;
        return Objects.equals(gameId, that.gameId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(gameId);
    }

    @Override
    public String toString() {
        return "GameNotStarted{" +
                "gameId=" + gameId +
                '}';
    }


}
