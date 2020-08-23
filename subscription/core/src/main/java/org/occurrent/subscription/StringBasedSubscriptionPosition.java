package org.occurrent.subscription;

import java.util.Objects;

/**
 * A simple {@link SubscriptionPosition} that is backed by a fixed String
 */
public class StringBasedSubscriptionPosition implements SubscriptionPosition {
    private final String value;

    public StringBasedSubscriptionPosition(String value) {
        Objects.requireNonNull(value, "Stream position value cannot be null");
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StringBasedSubscriptionPosition)) return false;
        StringBasedSubscriptionPosition that = (StringBasedSubscriptionPosition) o;
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
