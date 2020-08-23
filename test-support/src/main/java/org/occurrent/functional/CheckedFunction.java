package org.occurrent.functional;

import java.util.function.Function;

@FunctionalInterface
public interface CheckedFunction<T, R, E extends Throwable> {
    R apply(T t) throws E;

    static <T, R, E extends Throwable> Function<T, R> unchecked(CheckedFunction<T, R, E> f) {
        return t -> {
            try {
                return f.apply(t);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        };
    }
}