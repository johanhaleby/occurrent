package se.haleby.occurrent.changestreamer.mongodb.nativedriver.blocking;

import java.time.Duration;
import java.util.Objects;

public abstract class RetryStrategy {

    private RetryStrategy() {
    }

    public static RetryStrategy none() {
        return new None();
    }

    public static RetryStrategy fixed(long millis) {
        return new Fixed(millis);
    }

    public static RetryStrategy fixed(Duration duration) {
        Objects.requireNonNull(duration, "Duration cannot be null");
        return new Fixed(duration.toMillis());
    }

    final static class None extends RetryStrategy {
        private None() {
        }
    }

    public static RetryStrategy backoff(Duration initial, Duration max, double multiplier) {
        return new Backoff(initial, max, multiplier);
    }

    final static class Fixed extends RetryStrategy {
        public final long millis;

        private Fixed(long millis) {
            if (millis <= 0) {
                throw new IllegalArgumentException("Millis cannot be less than zero");
            }
            this.millis = millis;
        }
    }

    final static class Backoff extends RetryStrategy {
        public final Duration initial;
        public final Duration max;
        public final double multiplier;

        private Backoff(Duration initial, Duration max, double multiplier) {
            Objects.requireNonNull(initial, "Initial duration cannot be null");
            Objects.requireNonNull(max, "Max duration cannot be null");
            if (multiplier <= 0) {
                throw new IllegalArgumentException("multiplier cannot be less than zero");
            }
            this.initial = initial;
            this.max = max;
            this.multiplier = multiplier;
        }
    }
}