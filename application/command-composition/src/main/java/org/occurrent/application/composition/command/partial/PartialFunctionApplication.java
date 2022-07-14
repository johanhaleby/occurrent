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
 * applicationService.execute("streamid", events -> MyClass.someDomainFunction(events, 2, "my string));
 * </pre>
 * <p>
 * Alternatively, you can use partial function application by using the {@code partial} methods in this class:
 *
 * <pre>
 * applicationService.execute("streamid", partial(Class::someDomainFunction, 2, "my string));
 * </pre>
 */
public class PartialFunctionApplication {

    public static <T, U> Function<T, T> partial(BiFunction<T, U, T> fn, U param) {
        return stream -> fn.apply(stream, param);
    }

    public static <T, U, V> Function<T, T> partial(TriFunction<T, U, V> fn, U param1, V param2) {
        return stream -> fn.apply(stream, param1, param2);
    }

    public static <T, U, V, W> Function<T, T> partial(QuadrupleFunction<T, U, V, W> fn, U param1, V param2, W param3) {
        return stream -> fn.apply(stream, param1, param2, param3);
    }

    public static <T, U, V, W, X> Function<T, T> partial(QuintupleFunction<T, U, V, W, X> fn, U param1, V param2, W param3, X param4) {
        return stream -> fn.apply(stream, param1, param2, param3, param4);
    }

    public static <T, U, V, W, X, Y> Function<T, T> partial(SextupleFunction<T, U, V, W, X, Y> fn, U param1, V param2, W param3, X param4, Y param5) {
        return stream -> fn.apply(stream, param1, param2, param3, param4, param5);
    }

    public static <T, U, V, W, X, Y, Z> Function<T, T> partial(SeptupleFunction<T, U, V, W, X, Y, Z> fn, U param1, V param2, W param3, X param4, Y param5, Z param6) {
        return stream -> fn.apply(stream, param1, param2, param3, param4, param5, param6);
    }

    public static <T, U, V, W, X, Y, Z, T2> Function<T, T> partial(OctubleFunction<T, U, V, W, X, Y, Z, T2> fn, U param1, V param2, W param3, X param4, Y param5, Z param6, T2 param7) {
        return stream -> fn.apply(stream, param1, param2, param3, param4, param5, param6, param7);
    }

    public static <T, U, V, W, X, Y, Z, T2, U2> Function<T, T> partial(NonupleFunction<T, U, V, W, X, Y, Z, T2, U2> fn, U param1, V param2, W param3, X param4, Y param5, Z param6, T2 param7, U2 param8) {
        return stream -> fn.apply(stream, param1, param2, param3, param4, param5, param6, param7, param8);
    }

    public static <T, U, V, W, X, Y, Z, T2, U2, V2> Function<T, T> partial(DecupleFunction<T, U, V, W, X, Y, Z, T2, U2, V2> fn, U param1, V param2, W param3, X param4, Y param5, Z param6, T2 param7, U2 param8, V2 param9) {
        return stream -> fn.apply(stream, param1, param2, param3, param4, param5, param6, param7, param8, param9);
    }

    public static <T, U, V, W, X, Y, Z, T2, U2, V2, W2> Function<T, T> partial(UndecupleFunction<T, U, V, W, X, Y, Z, T2, U2, V2, W2> fn, U param1, V param2, W param3, X param4, Y param5, Z param6, T2 param7, U2 param8, V2 param9, W2 param10) {
        return stream -> fn.apply(stream, param1, param2, param3, param4, param5, param6, param7, param8, param9, param10);
    }

    public static <T, U, V, W, X, Y, Z, T2, U2, V2, W2, X2> Function<T, T> partial(DuodecupleFunction<T, U, V, W, X, Y, Z, T2, U2, V2, W2, X2> fn, U param1, V param2, W param3, X param4, Y param5, Z param6, T2 param7, U2 param8, V2 param9, W2 param10, X2 param11) {
        return stream -> fn.apply(stream, param1, param2, param3, param4, param5, param6, param7, param8, param9, param10, param11);
    }

    public static <T, U, V, W, X, Y, Z, T2, U2, V2, W2, X2, Y2> Function<T, T> partial(TredecupleFunction<T, U, V, W, X, Y, Z, T2, U2, V2, W2, X2, Y2> fn, U param1, V param2, W param3, X param4, Y param5, Z param6, T2 param7, U2 param8, V2 param9, W2 param10, X2 param11, Y2 param12) {
        return stream -> fn.apply(stream, param1, param2, param3, param4, param5, param6, param7, param8, param9, param10, param11, param12);
    }


    // Just some definitions of functions that are used to create "partial functions".
    @FunctionalInterface
    public interface TriFunction<T, U, V> {
        T apply(T t, U u, V v);
    }

    @FunctionalInterface
    public interface QuadrupleFunction<T, U, V, W> {
        T apply(T t, U u, V v, W w);
    }

    @FunctionalInterface
    public interface QuintupleFunction<T, U, V, W, X> {
        T apply(T t, U u, V v, W w, X x);
    }

    @FunctionalInterface
    public interface SextupleFunction<T, U, V, W, X, Y> {
        T apply(T t, U u, V v, W w, X x, Y y);
    }

    @FunctionalInterface
    public interface SeptupleFunction<T, U, V, W, X, Y, Z> {
        T apply(T t, U u, V v, W w, X x, Y y, Z z);
    }

    @FunctionalInterface
    public interface OctubleFunction<T, U, V, W, X, Y, Z, T2> {
        T apply(T t, U u, V v, W w, X x, Y y, Z z, T2 t2);
    }

    @FunctionalInterface
    public interface NonupleFunction<T, U, V, W, X, Y, Z, T2, U2> {
        T apply(T t, U u, V v, W w, X x, Y y, Z z, T2 t2, U2 u2);
    }

    @FunctionalInterface
    public interface DecupleFunction<T, U, V, W, X, Y, Z, T2, U2, V2> {
        T apply(T t, U u, V v, W w, X x, Y y, Z z, T2 t2, U2 u2, V2 v2);
    }

    @FunctionalInterface
    public interface UndecupleFunction<T, U, V, W, X, Y, Z, T2, U2, V2, W2> {
        T apply(T t, U u, V v, W w, X x, Y y, Z z, T2 t2, U2 u2, V2 v2, W2 w2);
    }

    @FunctionalInterface
    public interface DuodecupleFunction<T, U, V, W, X, Y, Z, T2, U2, V2, W2, X2> {
        T apply(T t, U u, V v, W w, X x, Y y, Z z, T2 t2, U2 u2, V2 v2, W2 w2, X2 x2);
    }

    @FunctionalInterface
    public interface TredecupleFunction<T, U, V, W, X, Y, Z, T2, U2, V2, W2, X2, Y2> {
        T apply(T t, U u, V v, W w, X x, Y y, Z z, T2 t2, U2 u2, V2 v2, W2 w2, X2 x2, Y2 y2);
    }
}
