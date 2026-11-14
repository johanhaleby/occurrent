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
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;

import java.util.concurrent.TimeUnit;

import static org.occurrent.benchmark.dcbstripes.MongoDcbStore.TYPE;

/**
 * The native store's DCB appends with {@code spreadTypeMarkerWrites} off and on (ADR 162), 16 threads, on a single-node
 * replica set in a container of its own per fork.
 * <ul>
 *     <li>{@code oneType}: tagged conditional appends of one type, each with a tag of its own and a condition on that
 *     tag, the ADR's simulated K = 8 case when the setting is on.</li>
 *     <li>{@code typePerThread}: the same with a type per thread, the ADR's distinct-types reference. Each thread has a
 *     type marker of its own, so spreading can't take contention away here, and its two results show the noise.</li>
 *     <li>{@code mixed}: 15 threads of {@code oneType} plus one thread making type-only conditional appends
 *     ({@code types(X)}) of the same type.</li>
 * </ul>
 * Needs Docker. Depends on {@code spreadTypeMarkerWrites}, unlike {@link DcbTypeMarkerCostBenchmark}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class DcbTypeStripeSettingBenchmark {

    @State(Scope.Benchmark)
    public static class Store {

        @Param({"false", "true"})
        public boolean spreadTypeMarkerWrites;

        MongoDcbStore store;

        @Setup(Level.Trial)
        public void start() {
            store = MongoDcbStore.start(builder -> builder.spreadTypeMarkerWrites(spreadTypeMarkerWrites));
        }

        @TearDown(Level.Trial)
        public void stop() {
            store.close();
        }
    }

    @Benchmark
    @Threads(16)
    public void oneType(Store store, TaggedWriter writer) {
        writer.append(store.store, TYPE);
    }

    @Benchmark
    @Threads(16)
    public void typePerThread(Store store, OwnType ownType, TaggedWriter writer) {
        writer.append(store.store, ownType.type);
    }

    @Benchmark
    @Group("mixed")
    @GroupThreads(15)
    public void mixedTagged(Store store, TaggedWriter writer) {
        writer.append(store.store, TYPE);
    }

    @Benchmark
    @Group("mixed")
    @GroupThreads(1)
    public void mixedTypeOnly(Store store, TypeOnlyWriter writer) {
        writer.append(store.store, TYPE);
    }
}
