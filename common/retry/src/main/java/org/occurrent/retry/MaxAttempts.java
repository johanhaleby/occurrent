package org.occurrent.retry;

import java.util.Objects;
import java.util.StringJoiner;

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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Limit)) return false;
            Limit limit1 = (Limit) o;
            return limit == limit1.limit;
        }

        @Override
        public int hashCode() {
            return Objects.hash(limit);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", Limit.class.getSimpleName() + "[", "]")
                    .add("limit=" + limit)
                    .toString();
        }


    }

    public static class Infinite extends MaxAttempts {
        static Infinite INSTANCE = new Infinite();

        private Infinite() {
        }

        public static MaxAttempts infinite() {
            return INSTANCE;
        }
    }
}
