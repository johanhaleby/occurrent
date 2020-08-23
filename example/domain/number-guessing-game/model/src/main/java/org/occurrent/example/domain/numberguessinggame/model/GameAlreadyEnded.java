package org.occurrent.example.domain.numberguessinggame.model;

import java.util.Objects;
import java.util.UUID;

public class GameAlreadyEnded extends RuntimeException {

    public final UUID gameId;

    public GameAlreadyEnded(UUID gameId) {
        this.gameId = gameId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GameAlreadyEnded)) return false;
        GameAlreadyEnded that = (GameAlreadyEnded) o;
        return Objects.equals(gameId, that.gameId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(gameId);
    }

    @Override
    public String toString() {
        return "GameAlreadyEnded{" +
                "gameId=" + gameId +
                '}';
    }


}
