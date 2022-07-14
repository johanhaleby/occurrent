package org.occurrent.application.composition.command.partial;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Utility for creating partial function applications for "commands", e.g. ({@code Function<List<T>, List<T>>} , {@code Function<Stream<T>, Stream<T>> etc}.
 * For example let's say you have a domain function defined like this in {@code MyClass}:
 *
 * <pre>
 * public List&lt;DomainEvent&gt; someDomainFunction(List&lt;DomainEvent&gt;, int someValue, String someString) {
 *     ...
 * }
 * </pre>
 * and you want to pass this as a command to an application service. You could do like this:
 *
 * <pre>
 * applicationService.execute("inputid", events -> MyClass.someDomainFunction(events, 2, "my string));
 * </pre>
 * <p>
 * Alternatively, you can use partial function application by using the {@code partial} methods in this class:
 *
 * <pre>
 * applicationService.execute("inputid", partial(Class::someDomainFunction, 2, "my string));
 * </pre>
 */
public class PartialFunctionApplication {

    public static <T, U, R> Function<T, R> partial(BiFunction<T, U, R> fn, U param) {
        return input -> fn.apply(input, param);
    }

    public static <T, U, V, R> Function<T, R> partial(TriFunction<T, U, V, R> fn, U param1, V param2) {
        return input -> fn.apply(input, param1, param2);
    }

    public static <T, U, V, W, R> Function<T, R> partial(QuadrupleFunction<T, U, V, W, R> fn, U param1, V param2, W param3) {
        return input -> fn.apply(input, param1, param2, param3);
    }

    public static <T, U, V, W, X, R> Function<T, R> partial(QuintupleFunction<T, U, V, W, X, R> fn, U param1, V param2, W param3, X param4) {
        return input -> fn.apply(input, param1, param2, param3, param4);
    }

    public static <T, U, V, W, X, Y, R> Function<T, R> partial(SextupleFunction<T, U, V, W, X, Y, R> fn, U param1, V param2, W param3, X param4, Y param5) {
        return input -> fn.apply(input, param1, param2, param3, param4, param5);
    }

    public static <T, U, V, W, X, Y, Z, R> Function<T, R> partial(SeptupleFunction<T, U, V, W, X, Y, Z, R> fn, U param1, V param2, W param3, X param4, Y param5, Z param6) {
        return input -> fn.apply(input, param1, param2, param3, param4, param5, param6);
    }

    public static <T, U, V, W, X, Y, Z, T2, R> Function<T, R> partial(OctubleFunction<T, U, V, W, X, Y, Z, T2, R> fn, U param1, V param2, W param3, X param4, Y param5, Z param6, T2 param7) {
        return input -> fn.apply(input, param1, param2, param3, param4, param5, param6, param7);
    }

    public static <T, U, V, W, X, Y, Z, T2, U2, R> Function<T, R> partial(NonupleFunction<T, U, V, W, X, Y, Z, T2, U2, R> fn, U param1, V param2, W param3, X param4, Y param5, Z param6, T2 param7, U2 param8) {
        return input -> fn.apply(input, param1, param2, param3, param4, param5, param6, param7, param8);
    }

    public static <T, U, V, W, X, Y, Z, T2, U2, V2, R> Function<T, R> partial(DecupleFunction<T, U, V, W, X, Y, Z, T2, U2, V2, R> fn, U param1, V param2, W param3, X param4, Y param5, Z param6, T2 param7, U2 param8, V2 param9) {
        return input -> fn.apply(input, param1, param2, param3, param4, param5, param6, param7, param8, param9);
    }

    public static <T, U, V, W, X, Y, Z, T2, U2, V2, W2, R> Function<T, R> partial(UndecupleFunction<T, U, V, W, X, Y, Z, T2, U2, V2, W2, R> fn, U param1, V param2, W param3, X param4, Y param5, Z param6, T2 param7, U2 param8, V2 param9, W2 param10) {
        return input -> fn.apply(input, param1, param2, param3, param4, param5, param6, param7, param8, param9, param10);
    }

    public static <T, U, V, W, X, Y, Z, T2, U2, V2, W2, X2, R> Function<T, R> partial(DuodecupleFunction<T, U, V, W, X, Y, Z, T2, U2, V2, W2, X2, R> fn, U param1, V param2, W param3, X param4, Y param5, Z param6, T2 param7, U2 param8, V2 param9, W2 param10, X2 param11) {
        return input -> fn.apply(input, param1, param2, param3, param4, param5, param6, param7, param8, param9, param10, param11);
    }

    public static <T, U, V, W, X, Y, Z, T2, U2, V2, W2, X2, Y2, R> Function<T, R> partial(TredecupleFunction<T, U, V, W, X, Y, Z, T2, U2, V2, W2, X2, Y2, R> fn, U param1, V param2, W param3, X param4, Y param5, Z param6, T2 param7, U2 param8, V2 param9, W2 param10, X2 param11, Y2 param12) {
        return input -> fn.apply(input, param1, param2, param3, param4, param5, param6, param7, param8, param9, param10, param11, param12);
    }


    // Just some definitions of functions that are used to create "partial functions".
    @FunctionalInterface
    public interface TriFunction<T, U, V, W> {
        W apply(T t, U u, V v);
    }

    @FunctionalInterface
    public interface QuadrupleFunction<T, U, V, W, X> {
        X apply(T t, U u, V v, W w);
    }

    @FunctionalInterface
    public interface QuintupleFunction<T, U, V, W, X, Y> {
        Y apply(T t, U u, V v, W w, X x);
    }

    @FunctionalInterface
    public interface SextupleFunction<T, U, V, W, X, Y, Z> {
        Z apply(T t, U u, V v, W w, X x, Y y);
    }

    @FunctionalInterface
    public interface SeptupleFunction<T, U, V, W, X, Y, Z, T2> {
        T2 apply(T t, U u, V v, W w, X x, Y y, Z z);
    }

    @FunctionalInterface
    public interface OctubleFunction<T, U, V, W, X, Y, Z, T2, U2> {
        U2 apply(T t, U u, V v, W w, X x, Y y, Z z, T2 t2);
    }

    @FunctionalInterface
    public interface NonupleFunction<T, U, V, W, X, Y, Z, T2, U2, V2> {
        V2 apply(T t, U u, V v, W w, X x, Y y, Z z, T2 t2, U2 u2);
    }

    @FunctionalInterface
    public interface DecupleFunction<T, U, V, W, X, Y, Z, T2, U2, V2, W2> {
        W2 apply(T t, U u, V v, W w, X x, Y y, Z z, T2 t2, U2 u2, V2 v2);
    }

    @FunctionalInterface
    public interface UndecupleFunction<T, U, V, W, X, Y, Z, T2, U2, V2, W2, X2> {
        X2 apply(T t, U u, V v, W w, X x, Y y, Z z, T2 t2, U2 u2, V2 v2, W2 w2);
    }

    @FunctionalInterface
    public interface DuodecupleFunction<T, U, V, W, X, Y, Z, T2, U2, V2, W2, X2, Y2> {
        Y2 apply(T t, U u, V v, W w, X x, Y y, Z z, T2 t2, U2 u2, V2 v2, W2 w2, X2 x2);
    }

    @FunctionalInterface
    public interface TredecupleFunction<T, U, V, W, X, Y, Z, T2, U2, V2, W2, X2, Y2, Z2> {
        Z2 apply(T t, U u, V v, W w, X x, Y y, Z z, T2 t2, U2 u2, V2 v2, W2 w2, X2 x2, Y2 y2);
    }
}
