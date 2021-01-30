package org.occurrent.retry;

public abstract class MaxAttempts {
    private MaxAttempts() {
    }

    public static class Limit extends MaxAttempts {
        public final int limit;

        Limit(int limit) {
            if (limit < 1) {
                throw new IllegalArgumentException("Max attempts must be greater than zero");
            }
            this.limit = limit;
        }
    }

    public static class Infinite extends MaxAttempts {
        Infinite() {
        }

        public static MaxAttempts infinite() {
            return new Infinite();
        }
    }
}
