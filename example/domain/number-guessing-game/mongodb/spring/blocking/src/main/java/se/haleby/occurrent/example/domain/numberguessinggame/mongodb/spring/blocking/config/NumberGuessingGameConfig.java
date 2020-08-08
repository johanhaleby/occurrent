package se.haleby.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "number-guessing-game")
public class NumberGuessingGameConfig {
    private int minNumberToGuess;
    private int maxNumberToGuess;
    private int maxNumberOfGuesses;

    public int getMinNumberToGuess() {
        return minNumberToGuess;
    }

    void setMinNumberToGuess(int minNumberToGuess) {
        this.minNumberToGuess = minNumberToGuess;
    }

    public int getMaxNumberToGuess() {
        return maxNumberToGuess;
    }

    void setMaxNumberToGuess(int maxNumberToGuess) {
        this.maxNumberToGuess = maxNumberToGuess;
    }

    public int getMaxNumberOfGuesses() {
        return maxNumberOfGuesses;
    }

    void setMaxNumberOfGuesses(int maxNumberOfGuesses) {
        this.maxNumberOfGuesses = maxNumberOfGuesses;
    }
}