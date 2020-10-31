package org.occurrent.application.composition.command.partial;

import org.occurrent.application.composition.command.partial.PartialApplicationFunctions.*;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Utility for creating partial function applications for "list commands", ({@code Function<Stream<T>, Stream<T>>}).
 * For example let's say you have a domain function defined like this in {@code MyClass}:
 *
 * <pre>
 * public Stream&lt;DomainEvent&gt; someDomainFunction(Stream&lt;DomainEvent&gt;, int someValue, String someString) {
 *     ...
 * }
 * </pre>
 *
 * and you want to pass this as a command to an application service. You could do like this:
 *
 * <pre>
 * applicationService.execute("streamId", events -> MyClass.someDomainFunction(events, 2, "my string));
 * </pre>
 *
 * Alternatively, you can use partial function application by using the {@code partial} methods in this class:
 *
 * <pre>
 * applicationService.execute("streamId", partial(Class::someDomainFunction, 2, "my string));
 * </pre>
 */
public class PartialStreamCommandApplication {

    public static <T, U> Function<Stream<T>, Stream<T>> partial(BiFunction<Stream<T>, U, Stream<T>> fn, U param) {
        return stream -> fn.apply(stream, param);
    }

    public static <T, U, V> Function<Stream<T>, Stream<T>> partial(TriFunction<Stream<T>, U, V> fn, U param1, V param2) {
        return stream -> fn.apply(stream, param1, param2);
    }

    public static <T, U, V, W> Function<Stream<T>, Stream<T>> partial(QuadrupleFunction<Stream<T>, U, V, W> fn, U param1, V param2, W param3) {
        return stream -> fn.apply(stream, param1, param2, param3);
    }

    public static <T, U, V, W, X> Function<Stream<T>, Stream<T>> partial(QuintupleFunction<Stream<T>, U, V, W, X> fn, U param1, V param2, W param3, X param4) {
        return stream -> fn.apply(stream, param1, param2, param3, param4);
    }

    public static <T, U, V, W, X, Y> Function<Stream<T>, Stream<T>> partial(SextupleFunction<Stream<T>, U, V, W, X, Y> fn, U param1, V param2, W param3, X param4, Y param5) {
        return stream -> fn.apply(stream, param1, param2, param3, param4, param5);
    }

    public static <T, U, V, W, X, Y, Z> Function<Stream<T>, Stream<T>> partial(SeptupleFunction<Stream<T>, U, V, W, X, Y, Z> fn, U param1, V param2, W param3, X param4, Y param5, Z param6) {
        return stream -> fn.apply(stream, param1, param2, param3, param4, param5, param6);
    }

    public static <T, U, V, W, X, Y, Z, T2> Function<Stream<T>, Stream<T>> partial(OctubleFunction<Stream<T>, U, V, W, X, Y, Z, T2> fn, U param1, V param2, W param3, X param4, Y param5, Z param6, T2 param7) {
        return stream -> fn.apply(stream, param1, param2, param3, param4, param5, param6, param7);
    }

    public static <T, U, V, W, X, Y, Z, T2, U2> Function<Stream<T>, Stream<T>> partial(NonupleFunction<Stream<T>, U, V, W, X, Y, Z, T2, U2> fn, U param1, V param2, W param3, X param4, Y param5, Z param6, T2 param7, U2 param8) {
        return stream -> fn.apply(stream, param1, param2, param3, param4, param5, param6, param7, param8);
    }

    public static <T, U, V, W, X, Y, Z, T2, U2, V2> Function<Stream<T>, Stream<T>> partial(DecupleFunction<Stream<T>, U, V, W, X, Y, Z, T2, U2, V2> fn, U param1, V param2, W param3, X param4, Y param5, Z param6, T2 param7, U2 param8, V2 param9) {
        return stream -> fn.apply(stream, param1, param2, param3, param4, param5, param6, param7, param8, param9);
    }

    public static <T, U, V, W, X, Y, Z, T2, U2, V2, W2> Function<Stream<T>, Stream<T>> partial(UndecupleFunction<Stream<T>, U, V, W, X, Y, Z, T2, U2, V2, W2> fn, U param1, V param2, W param3, X param4, Y param5, Z param6, T2 param7, U2 param8, V2 param9, W2 param10) {
        return stream -> fn.apply(stream, param1, param2, param3, param4, param5, param6, param7, param8, param9, param10);
    }

    public static <T, U, V, W, X, Y, Z, T2, U2, V2, W2, X2> Function<Stream<T>, Stream<T>> partial(DuodecupleFunction<Stream<T>, U, V, W, X, Y, Z, T2, U2, V2, W2, X2> fn, U param1, V param2, W param3, X param4, Y param5, Z param6, T2 param7, U2 param8, V2 param9, W2 param10, X2 param11) {
        return stream -> fn.apply(stream, param1, param2, param3, param4, param5, param6, param7, param8, param9, param10, param11);
    }

    public static <T, U, V, W, X, Y, Z, T2, U2, V2, W2, X2, Y2> Function<Stream<T>, Stream<T>> partial(TredecupleFunction<Stream<T>, U, V, W, X, Y, Z, T2, U2, V2, W2, X2, Y2> fn, U param1, V param2, W param3, X param4, Y param5, Z param6, T2 param7, U2 param8, V2 param9, W2 param10, X2 param11, Y2 param12) {
        return stream -> fn.apply(stream, param1, param2, param3, param4, param5, param6, param7, param8, param9, param10, param11, param12);
    }
}
