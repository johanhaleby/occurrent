package se.haleby.occurrent.example.domain.numberguessinggame;

import java.util.Objects;

public class MaxNumberOfGuesses {
    public final int value;

    public MaxNumberOfGuesses(int value) {
        if (value > 100 || value < 1) {
            throw new IllegalArgumentException("Max number of guesses must be between 1 and 100");
        }
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MaxNumberOfGuesses)) return false;
        MaxNumberOfGuesses that = (MaxNumberOfGuesses) o;
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
