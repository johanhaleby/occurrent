package org.occurrent.application.composition.command.partial;

import org.occurrent.application.composition.command.partial.PartialApplicationFunctions.*;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public class PartialListCommandApplication {

    public static <T, U> Function<List<T>, List<T>> partial(BiFunction<List<T>, U, List<T>> fn, U param) {
        return stream -> fn.apply(stream, param);
    }

    public static <T, U, V> Function<List<T>, List<T>> partial(TriFunction<List<T>, U, V> fn, U param1, V param2) {
        return stream -> fn.apply(stream, param1, param2);
    }

    public static <T, U, V, W> Function<List<T>, List<T>> partial(QuadrupleFunction<List<T>, U, V, W> fn, U param1, V param2, W param3) {
        return stream -> fn.apply(stream, param1, param2, param3);
    }

    public static <T, U, V, W, X> Function<List<T>, List<T>> partial(QuintupleFunction<List<T>, U, V, W, X> fn, U param1, V param2, W param3, X param4) {
        return stream -> fn.apply(stream, param1, param2, param3, param4);
    }

    public static <T, U, V, W, X, Y> Function<List<T>, List<T>> partial(SextupleFunction<List<T>, U, V, W, X, Y> fn, U param1, V param2, W param3, X param4, Y param5) {
        return stream -> fn.apply(stream, param1, param2, param3, param4, param5);
    }

    public static <T, U, V, W, X, Y, Z> Function<List<T>, List<T>> partial(SeptupleFunction<List<T>, U, V, W, X, Y, Z> fn, U param1, V param2, W param3, X param4, Y param5, Z param6) {
        return stream -> fn.apply(stream, param1, param2, param3, param4, param5, param6);
    }

    public static <T, U, V, W, X, Y, Z, T2> Function<List<T>, List<T>> partial(OctubleFunction<List<T>, U, V, W, X, Y, Z, T2> fn, U param1, V param2, W param3, X param4, Y param5, Z param6, T2 param7) {
        return stream -> fn.apply(stream, param1, param2, param3, param4, param5, param6, param7);
    }

    public static <T, U, V, W, X, Y, Z, T2, U2> Function<List<T>, List<T>> partial(NonupleFunction<List<T>, U, V, W, X, Y, Z, T2, U2> fn, U param1, V param2, W param3, X param4, Y param5, Z param6, T2 param7, U2 param8) {
        return stream -> fn.apply(stream, param1, param2, param3, param4, param5, param6, param7, param8);
    }

    public static <T, U, V, W, X, Y, Z, T2, U2, V2> Function<List<T>, List<T>> partial(DecupleFunction<List<T>, U, V, W, X, Y, Z, T2, U2, V2> fn, U param1, V param2, W param3, X param4, Y param5, Z param6, T2 param7, U2 param8, V2 param9) {
        return stream -> fn.apply(stream, param1, param2, param3, param4, param5, param6, param7, param8, param9);
    }

    public static <T, U, V, W, X, Y, Z, T2, U2, V2, W2> Function<List<T>, List<T>> partial(UndecupleFunction<List<T>, U, V, W, X, Y, Z, T2, U2, V2, W2> fn, U param1, V param2, W param3, X param4, Y param5, Z param6, T2 param7, U2 param8, V2 param9, W2 param10) {
        return stream -> fn.apply(stream, param1, param2, param3, param4, param5, param6, param7, param8, param9, param10);
    }

    public static <T, U, V, W, X, Y, Z, T2, U2, V2, W2, X2> Function<List<T>, List<T>> partial(DuodecupleFunction<List<T>, U, V, W, X, Y, Z, T2, U2, V2, W2, X2> fn, U param1, V param2, W param3, X param4, Y param5, Z param6, T2 param7, U2 param8, V2 param9, W2 param10, X2 param11) {
        return stream -> fn.apply(stream, param1, param2, param3, param4, param5, param6, param7, param8, param9, param10, param11);
    }

    public static <T, U, V, W, X, Y, Z, T2, U2, V2, W2, X2, Y2> Function<List<T>, List<T>> partial(TredecupleFunction<List<T>, U, V, W, X, Y, Z, T2, U2, V2, W2, X2, Y2> fn, U param1, V param2, W param3, X param4, Y param5, Z param6, T2 param7, U2 param8, V2 param9, W2 param10, X2 param11, Y2 param12) {
        return stream -> fn.apply(stream, param1, param2, param3, param4, param5, param6, param7, param8, param9, param10, param11, param12);
    }
}
