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

package org.occurrent.benchmark.handover;

import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.api.blocking.internal.BlockingHandover;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

/**
 * Reproduces the measurement setup behind ADR 108 ("A live push handler runs outside the handover lock"): the real
 * {@code BlockingHandover.accept(T)} live-delivery path, driven from 1, 2, 4 and 8 concurrent threads, at handler
 * costs of 50 and 200 microseconds, compared against {@link FullyLockedHandover}, which reconstructs the pre-ADR-108
 * shape (fold inside the lock) since that shape no longer exists in production code.
 * <p>
 * The ADR's own 1-microsecond row is intentionally not reproduced here: the ADR itself notes that at that size the
 * benchmark measures lock/dedup overhead rather than the handler cost in question, with error bars the ADR describes
 * as "±100-400%".
 * <p>
 * Run with, for example:
 * <pre>{@code
 * java -jar benchmark/target/benchmarks.jar BlockingHandoverThroughputBenchmark -wi 3 -i 5 -f 1
 * }</pre>
 * which is the exact {@code -wi 3 -i 5 -f 1} protocol ADR 108 states.
 * <p>
 * No external service or container is required; this benchmark only exercises in-process code.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class BlockingHandoverThroughputBenchmark {

    private static final BlockingHandover.Source<Long> ALREADY_CAUGHT_UP = new BlockingHandover.Source<>() {
        @Override
        public boolean isAlreadyCaughtUp() {
            return true;
        }

        @Override
        public Stream<Long> replay() {
            return Stream.empty();
        }

        @Override
        public void markCaughtUp() {
        }
    };

    /**
     * The "proposed" column from ADR 108's table: the real, shipped {@code BlockingHandover}, where only the de-dup
     * reservation runs under the lock and the fold runs outside it.
     */
    @State(Scope.Benchmark)
    public static class ProposedState {

        @Param({"50", "200"})
        public long workMicros;

        private BlockingHandover<Long> handover;
        private final LongAdder sink = new LongAdder();
        private final java.util.concurrent.atomic.AtomicLong idSequence = new java.util.concurrent.atomic.AtomicLong();

        @Setup(Level.Trial)
        public void setUp() {
            handover = BlockingHandover.create(
                    payload -> BusySpin.spinMicros(workMicros, sink),
                    Object::toString,
                    CatchupThenLiveOptions.defaults(),
                    "benchmark");
            handover.catchUp(ALREADY_CAUGHT_UP);
        }

        void acceptOnePayload() {
            handover.accept(idSequence.getAndIncrement());
        }
    }

    /**
     * The "current (locked)" column from ADR 108's table: {@link FullyLockedHandover}, folding inside the lock.
     */
    @State(Scope.Benchmark)
    public static class LockedState {

        @Param({"50", "200"})
        public long workMicros;

        private FullyLockedHandover<Long> handover;
        private final LongAdder sink = new LongAdder();
        private final java.util.concurrent.atomic.AtomicLong idSequence = new java.util.concurrent.atomic.AtomicLong();

        @Setup(Level.Trial)
        public void setUp() {
            handover = new FullyLockedHandover<>(payload -> BusySpin.spinMicros(workMicros, sink), Object::toString);
        }

        void acceptOnePayload() {
            handover.accept(idSequence.getAndIncrement());
        }
    }

    @Benchmark
    @Threads(1)
    public void proposed_threads1(ProposedState state) {
        state.acceptOnePayload();
    }

    @Benchmark
    @Threads(2)
    public void proposed_threads2(ProposedState state) {
        state.acceptOnePayload();
    }

    @Benchmark
    @Threads(4)
    public void proposed_threads4(ProposedState state) {
        state.acceptOnePayload();
    }

    @Benchmark
    @Threads(8)
    public void proposed_threads8(ProposedState state) {
        state.acceptOnePayload();
    }

    @Benchmark
    @Threads(1)
    public void locked_threads1(LockedState state) {
        state.acceptOnePayload();
    }

    @Benchmark
    @Threads(2)
    public void locked_threads2(LockedState state) {
        state.acceptOnePayload();
    }

    @Benchmark
    @Threads(4)
    public void locked_threads4(LockedState state) {
        state.acceptOnePayload();
    }

    @Benchmark
    @Threads(8)
    public void locked_threads8(LockedState state) {
        state.acceptOnePayload();
    }
}
