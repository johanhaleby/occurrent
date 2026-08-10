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

package org.occurrent.benchmark.coalescing;

import org.occurrent.dsl.projection.MaterializedViewOptions;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.projection.blocking.Projections;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ReplayAware;
import org.occurrent.retry.RetryStrategy;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Measures a catch-up replay's flush cost against {@link MaterializedViewOptions#batchSize()}
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0110-a-replay-tells-the-view-where-it-begins-and-ends.md">ADR 110</a>),
 * the number {@code MaterializedViewOptions.DEFAULT_BATCH_SIZE} defaults to without a benchmark behind it
 * (<a href="https://github.com/johanhaleby/occurrent/issues/692">#692</a>).
 * <p>
 * Each invocation drives {@code batchSize} events through the real, shipped
 * {@link Projections#materializedView(Projection, org.occurrent.dsl.view.ViewStateRepository, RetryStrategy, MaterializedViewOptions)}
 * during a replay, which buffers them and flushes exactly once, on the last event, since the buffer reaches
 * {@code batchSize} at that point. {@link SimulatedLatencyRepository} stands in for the store. Every
 * {@code findAllById}/{@code saveAll} call (a flush uses one of each) pays a fixed round-trip cost plus a small cost
 * per key it touches, the same shape a real network round trip to a database has. A flush's total cost is therefore
 * two round trips plus one per-key cost for every key the batch touched, whatever the batch size.
 * <p>
 * {@link CoalescingFixtures.KeyDensity#SPARSE} gives every event in the batch its own key, so a flush touches as
 * many keys as it has events, the same read/write volume coalescing was never going to reduce. {@code DENSE} cycles
 * every event through a fixed pool of {@link CoalescingFixtures#DENSE_KEY_COUNT} keys, so a flush touches at most
 * that many keys no matter how large the batch is, the case where combining several events for the same key before
 * touching the repository actually pays off. Comparing both across {@code batchSize} 1, 100 and 1000 is what shows
 * whether a bigger batch keeps paying for itself or only helps up to a point.
 * <p>
 * {@code @Setup(Level.Invocation)} builds a fresh view, repository and projection for every invocation, so one
 * invocation's buffered state can never leak into the next. That is more setup work per invocation than a typical
 * JMH benchmark does, but the timed region is only {@link BatchState#driveOneFlush()}, so the setup itself is not
 * measured.
 * <p>
 * Run with, for example:
 * <pre>{@code
 * java -jar benchmark/target/benchmarks.jar CoalescingFlushBenchmark -wi 3 -i 5 -f 1
 * }</pre>
 * No external service or container is required.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class CoalescingFlushBenchmark {

    private static final long ROUND_TRIP_MICROS = 500;
    private static final long PER_KEY_MICROS = 5;

    @State(Scope.Thread)
    public static class BatchState {

        @Param({"1", "100", "1000"})
        public int batchSize;

        @Param({"SPARSE", "DENSE"})
        public CoalescingFixtures.KeyDensity keyDensity;

        private final LongAdder sink = new LongAdder();
        private MaterializedView<Long> view;

        @Setup(Level.Invocation)
        public void setUp() {
            SimulatedLatencyRepository repository = new SimulatedLatencyRepository(ROUND_TRIP_MICROS, PER_KEY_MICROS, sink);
            Projection<Long, Long, String> projection = Projection.<Long, Long, String>builder(0L)
                    .id(event -> CoalescingFixtures.keyFor(keyDensity, batchSize, event))
                    .on(Long.class, (state, event) -> state + 1)
                    .build();
            MaterializedView<Long> materializedView =
                    Projections.materializedView(projection, repository, RetryStrategy.none(), new MaterializedViewOptions(batchSize));
            if (!(materializedView instanceof ReplayAware replayAware)) {
                throw new IllegalStateException(Projections.class.getSimpleName() + " no longer returns a "
                        + ReplayAware.class.getSimpleName() + ", so this benchmark can no longer start a replay to buffer through.");
            }
            replayAware.replayStarted();
            view = materializedView;
        }

        void driveOneFlush() {
            for (long i = 0; i < batchSize; i++) {
                view.update(i);
            }
        }
    }

    @Benchmark
    public void flushOneBatch(BatchState state) {
        state.driveOneFlush();
    }
}
