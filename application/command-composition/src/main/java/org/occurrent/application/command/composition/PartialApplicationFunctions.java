package org.occurrent.application.command.composition;

public class PartialApplicationFunctions {
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
