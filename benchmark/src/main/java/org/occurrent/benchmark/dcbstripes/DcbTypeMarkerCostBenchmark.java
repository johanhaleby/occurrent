/*
 * Copyright 2026 Johan Haleby
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

package org.occurrent.benchmark.dcbstripes;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import static org.occurrent.benchmark.dcbstripes.MongoDcbStore.TYPE;

/**
 * What the type marker stripes of ADR 162 cost with the setting at its default, on the native store, on a single-node
 * replica set in a container of its own per fork. Compare it with the same class built before the stripes, where a
 * type has one marker. It uses no API the stripes added, so it compiles there unchanged.
 * <ul>
 *     <li>{@code typeOnlyRead}: {@code read(types(X))} on one thread, over {@code eventsOfType} events of the type.</li>
 *     <li>{@code typeOnlyAppendOneType}: type-only conditional appends of one type on 16 threads, which refuse each
 *     other.</li>
 *     <li>{@code typeOnlyAppendTypePerThread}: the same with a type per thread, so the cost of the marker writes
 *     without contention.</li>
 *     <li>{@code taggedAppendOneType}: tagged conditional appends of one type on 16 threads.</li>
 * </ul>
 * Needs Docker.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class DcbTypeMarkerCostBenchmark {

    @State(Scope.Benchmark)
    public static class Store {

        MongoDcbStore store;

        @Setup(Level.Trial)
        public void start() {
            store = MongoDcbStore.start(UnaryOperator.identity());
        }

        @TearDown(Level.Trial)
        public void stop() {
            store.close();
        }
    }

    @State(Scope.Benchmark)
    public static class StoreWithEvents {

        @Param({"1", "1000"})
        public int eventsOfType;

        MongoDcbStore store;

        @Setup(Level.Trial)
        public void start() {
            store = MongoDcbStore.start(UnaryOperator.identity());
            for (int i = 1; i < eventsOfType; i++) {
                store.unconditionalAppend(TYPE);
            }
            // A type-only conditional append writes every marker of the type, so the read finds all of them
            if (!store.typeOnlyConditionalAppend(TYPE)) {
                throw new IllegalStateException("Nothing else appends, so the condition can't fail");
            }
        }

        @TearDown(Level.Trial)
        public void stop() {
            store.close();
        }
    }

    @Benchmark
    @Threads(1)
    public void typeOnlyRead(StoreWithEvents store, Blackhole blackhole) {
        blackhole.consume(store.store.readType(TYPE));
    }

    @Benchmark
    @Threads(16)
    public void typeOnlyAppendOneType(Store store, TypeOnlyWriter writer) {
        writer.append(store.store, TYPE);
    }

    @Benchmark
    @Threads(16)
    public void typeOnlyAppendTypePerThread(Store store, OwnType ownType, TypeOnlyWriter writer) {
        writer.append(store.store, ownType.type);
    }

    @Benchmark
    @Threads(16)
    public void taggedAppendOneType(Store store, TaggedWriter writer) {
        writer.append(store.store, TYPE);
    }
}
