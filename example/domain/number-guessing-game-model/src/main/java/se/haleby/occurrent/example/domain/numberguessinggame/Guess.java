package se.haleby.occurrent.example.domain.numberguessinggame;

import java.util.Objects;

public class Guess {
    public final int value;

    public Guess(int value) {
        if (value < 1 || value > 1000) {
            throw new IllegalArgumentException("A guess must be between 1 and 1000");
        }
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Guess)) return false;
        Guess that = (Guess) o;
        return value == that.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return "MaxNumberOfGuesses{" +
                "value=" + value +
                '}';
    }
}
