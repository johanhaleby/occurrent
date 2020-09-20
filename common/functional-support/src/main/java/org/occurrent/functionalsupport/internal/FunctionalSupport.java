/*
 * Copyright 2020 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.functionalsupport.internal;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Some functional operations that are missing in java.util.Stream
 */
public class FunctionalSupport {

    public static <A, T> Stream<T> mapWithIndex(Stream<A> stream, long startIndex, Function<Pair<Long, A>, T> fn) {
        return zip(LongStream.iterate(startIndex + 1, i -> i + 1).boxed(), stream, Pair::new).map(fn);
    }

    public static <A, B, C> Stream<C> zip(Stream<A> streamA, Stream<B> streamB, BiFunction<A, B, C> zipper) {
        final Iterator<A> iteratorA = streamA.iterator();
        final Iterator<B> iteratorB = streamB.iterator();
        final Iterator<C> iteratorC = new Iterator<C>() {
            @Override
            public boolean hasNext() {
                return iteratorA.hasNext() && iteratorB.hasNext();
            }

            @Override
            public C next() {
                return zipper.apply(iteratorA.next(), iteratorB.next());
            }
        };
        final boolean parallel = streamA.isParallel() || streamB.isParallel();
        return iteratorToFiniteStream(iteratorC, parallel);
    }

    private static <T> Stream<T> iteratorToFiniteStream(Iterator<T> iterator, boolean parallel) {
        final Iterable<T> iterable = () -> iterator;
        return StreamSupport.stream(iterable.spliterator(), parallel);
    }

    public static class Pair<T1, T2> {
        public final T1 t1;
        public final T2 t2;

        public Pair(T1 t1, T2 t2) {
            this.t1 = t1;
            this.t2 = t2;
        }

        public T1 getT1() {
            return t1;
        }

        public T2 getT2() {
            return t2;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Pair)) return false;
            Pair<?, ?> pair = (Pair<?, ?>) o;
            return Objects.equals(t1, pair.t1) &&
                    Objects.equals(t2, pair.t2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(t1, t2);
        }

        @Override
        public String toString() {
            return "Pair{" +
                    "t1=" + t1 +
                    ", t2=" + t2 +
                    '}';
        }
    }

    public static <T> Predicate<T> not(Predicate<T> predicate) {
        return predicate.negate();
    }

    public static <T> Stream<T> takeWhile(Stream<T> stream, Predicate<? super T> p) {
        class Taking extends Spliterators.AbstractSpliterator<T> implements Consumer<T> {
            private static final int CANCEL_CHECK_COUNT = 63;
            private final Spliterator<T> s;
            private int count;
            private T t;
            private final AtomicBoolean cancel = new AtomicBoolean();
            private boolean takeOrDrop = true;

            Taking(Spliterator<T> s) {
                super(s.estimateSize(), s.characteristics() & ~(Spliterator.SIZED | Spliterator.SUBSIZED));
                this.s = s;
            }

            @Override
            public boolean tryAdvance(Consumer<? super T> action) {
                boolean test = true;
                if (takeOrDrop &&               // If can take
                        (count != 0 || !cancel.get()) && // and if not cancelled
                        s.tryAdvance(this) &&   // and if advanced one element
                        (test = p.test(t))) {   // and test on element passes
                    action.accept(t);           // then accept element
                    return true;
                } else {
                    // Taking is finished
                    takeOrDrop = false;
                    // Cancel all further traversal and splitting operations
                    // only if test of element failed (short-circuited)
                    if (!test)
                        cancel.set(true);
                    return false;
                }
            }

            @Override
            public Comparator<? super T> getComparator() {
                return s.getComparator();
            }

            @Override
            public void accept(T t) {
                count = (count + 1) & CANCEL_CHECK_COUNT;
                this.t = t;
            }

            @Override
            public Spliterator<T> trySplit() {
                return null;
            }
        }
        return StreamSupport.stream(new Taking(stream.spliterator()), stream.isParallel()).onClose(stream::close);
    }
}