package org.occurrent.functional;

import java.util.concurrent.Callable;
import java.util.function.Predicate;

public class Not {

    public static <T> Predicate<T> not(Predicate<T> predicate) {
        return predicate.negate();
    }

    public static Callable<Boolean> not(Callable<Boolean> callable) {
        return () -> !callable.call();
    }
}
