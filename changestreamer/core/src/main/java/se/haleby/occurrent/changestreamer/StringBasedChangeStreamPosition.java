package se.haleby.occurrent.changestreamer;

import java.util.Objects;

/**
 * A simple {@link ChangeStreamPosition} that is backed by a fixed String
 */
public class StringBasedChangeStreamPosition implements ChangeStreamPosition {
    private final String value;

    public StringBasedChangeStreamPosition(String value) {
        Objects.requireNonNull(value, "Stream position value cannot be null");
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StringBasedChangeStreamPosition)) return false;
        StringBasedChangeStreamPosition that = (StringBasedChangeStreamPosition) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return "StringBasedStreamPosition{" +
                "value='" + value + '\'' +
                '}';
    }

    @Override
    public String asString() {
        return value;
    }
}
