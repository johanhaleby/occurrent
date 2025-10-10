package org.occurrent.retry;

import org.jspecify.annotations.NullMarked;

@NullMarked
public sealed interface MaxAttempts {

    record Limit(int limit) implements MaxAttempts {
        public Limit {
            if (limit < 1) {
                throw new IllegalArgumentException("Max attempts must be greater than 1");
            }
        }
    }

    record Infinite() implements MaxAttempts {
        static Infinite INSTANCE = new Infinite();

        public static MaxAttempts infinite() {
            return INSTANCE;
        }
    }
}
