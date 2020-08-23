package org.occurrent.subscription.mongodb.nativedriver.blocking;

import java.time.Duration;
import java.util.Objects;

/**
 * Retry strategy to use if the action throws an exception.
 */
public abstract class RetryStrategy {

    private RetryStrategy() {
    }

    /**
     * @return Don't retry and re-throw an exception thrown when action is invoked.
     */
    public static RetryStrategy none() {
        return new None();
    }

    /**
     * @return Retry after a fixed number of millis if an action throws an exception.
     */
    public static RetryStrategy fixed(long millis) {
        return new Fixed(millis);
    }

    /**
     * @return Retry after a fixed duration if an action throws an exception.
     */
    public static RetryStrategy fixed(Duration duration) {
        Objects.requireNonNull(duration, "Duration cannot be null");
        return new Fixed(duration.toMillis());
    }

    /**
     * @return Retry after with exponential backoff if an action throws an exception.
     */
    public static RetryStrategy backoff(Duration initial, Duration max, double multiplier) {
        return new Backoff(initial, max, multiplier);
    }


    final static class None extends RetryStrategy {
        private None() {
        }
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