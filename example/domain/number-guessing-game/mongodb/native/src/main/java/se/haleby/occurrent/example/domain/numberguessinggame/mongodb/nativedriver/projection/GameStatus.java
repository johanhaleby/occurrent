package se.haleby.occurrent.example.domain.numberguessinggame.mongodb.nativedriver.projection;

import java.util.List;
import java.util.UUID;

public class GameStatus {
    public final UUID gameId;
    public final int maxNumberOfGuesses;
    public final int secretNumber;
    public final List<Integer> guesses;

    public GameStatus(UUID gameId, int secretNumber, int maxNumberOfGuesses, List<Integer> guesses) {
        this.gameId = gameId;
        this.maxNumberOfGuesses = maxNumberOfGuesses;
        this.guesses = guesses;
        this.secretNumber = secretNumber;
    }

    public boolean isEnded() {
        return guesses.size() == maxNumberOfGuesses || isSecretNumberGuessedByPlayer();
    }

    public boolean isSecretNumberGuessedByPlayer() {
        return guesses.contains(secretNumber);
    }
}