package se.haleby.occurrent.example.domain.numberguessinggame.model;

import java.util.Random;

public class SecretNumberToGuess {

    public final int value;

    public SecretNumberToGuess(int value) {
        if (value < 1 || value > 1000) {
            throw new IllegalArgumentException("Value to guess must be between 1 and 1000");
        }
        this.value = value;
    }

    public static SecretNumberToGuess randomBetween(int min, int max) {
        if (min >= max) {
            throw new IllegalArgumentException("min must be < max");
        }
        Random rand = new Random();
        int random = rand.nextInt((max - min)) + min;
        return new SecretNumberToGuess(random);
    }
}