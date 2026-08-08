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

package org.occurrent.benchmark.reactorhandoff;

import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.projection.reactor.Projections;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.View;
import org.occurrent.dsl.view.ViewStateRepository;
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
import reactor.core.publisher.Mono;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiFunction;

/**
 * Measures the per-event thread hand-off <a href="https://github.com/johanhaleby/occurrent/issues/639">#639</a> asks
 * about: the reactor projection DSL folds every event through {@code Mono.fromRunnable(..).subscribeOn(Schedulers.boundedElastic())},
 * both in {@code Projections.reactiveUpdateWithMetadata(MaterializedView)} ({@code Projections.java}) and in
 * {@code CoalescingMaterializedUpdate.apply}, the write-through fold {@code Projections.reactiveUpdateWithMetadata(Projection, ViewStateRepository, ...)}
 * builds on.
 * <p>
 * Each site is measured two ways: {@code wrapped} is the real, shipped fold, unmodified; {@code unwrapped} is the
 * same fold run synchronously on the calling thread, the shape removing the {@code subscribeOn} hop would leave.
 * The difference between the two columns is the hand-off's own cost, isolated from the fold's own work (a busy spin
 * standing in for a repository round trip, the same role and the same 50/200 microsecond magnitudes ADR 108's own
 * benchmark uses, see {@code BlockingHandoverThroughputBenchmark}), driven from 1, 2, 4 and 8 concurrent threads the
 * same way.
 * <p>
 * This benchmark answers the performance half of #639 only. Whether the hand-off can be removed at all is a
 * separate, correctness-first question answered on the issue: every path that reaches these folds is not guaranteed
 * to already be running on {@code boundedElastic} (a live push-subscription delivery thread is not), so a wrapped
 * fold reachable from such a path cannot drop the hop without moving blocking repository work onto whatever thread
 * the source used to deliver the event.
 * <p>
 * Run with, for example:
 * <pre>{@code
 * java -jar benchmark/target/benchmarks.jar ReactorProjectionHandoffBenchmark -wi 3 -i 5 -f 1
 * }</pre>
 * the same {@code -wi 3 -i 5 -f 1} protocol ADR 108's table uses.
 * <p>
 * No external service or container is required; this benchmark only exercises in-process code.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class ReactorProjectionHandoffBenchmark {

    /**
     * Stands in for a real fold's per-event work (a repository round trip), the same role and the same
     * nanosecond-deadline shape ADR 108's own benchmark uses: a fixed wall-clock duration is closer to the ADR's
     * stated 50/200 microsecond workloads than a JMH token count, and folding a counter into {@code sink} on every
     * iteration keeps the loop from being eligible for dead-code elimination.
     */
    private static void spinMicros(long micros, LongAdder sink) {
        long deadlineNanos = System.nanoTime() + (micros * 1_000L);
        long iterations = 0;
        while (System.nanoTime() < deadlineNanos) {
            iterations++;
        }
        sink.add(iterations);
    }

    private static View<Long, Long> spinningView(long workMicros, LongAdder sink) {
        return View.create(0L, (state, event) -> {
            spinMicros(workMicros, sink);
            return state + 1;
        });
    }

    private static MaterializedView<Long> materializedView(long workMicros, LongAdder sink) {
        ConcurrentHashMap<String, Long> store = new ConcurrentHashMap<>();
        ViewStateRepository<Long, String> repository = ViewStateRepository.create(store::get, store::put);
        return MaterializedView.create(event -> "singleton", spinningView(workMicros, sink), repository);
    }

    private static Projection<Long, Long, String> singletonProjection(long workMicros, LongAdder sink) {
        return Projection.<Long, Long>singletonBuilder(0L)
                .on(Long.class, (state, event) -> {
                    spinMicros(workMicros, sink);
                    return state + 1;
                })
                .build();
    }

    private static ViewStateRepository<Long, String> newRepository() {
        ConcurrentHashMap<String, Long> store = new ConcurrentHashMap<>();
        return ViewStateRepository.create(store::get, store::put);
    }

    // --- MaterializedView site: Projections.reactiveUpdateWithMetadata(MaterializedView) ---

    /**
     * The real, shipped {@link Projections#reactiveUpdateWithMetadata(MaterializedView)}: every fold wrapped in
     * {@code Mono.fromRunnable(..).subscribeOn(Schedulers.boundedElastic())}.
     */
    @State(Scope.Benchmark)
    public static class MaterializedViewWrappedState {

        @Param({"50", "200"})
        public long workMicros;

        private BiFunction<EventMetadata, Long, Mono<Void>> fold;
        private final LongAdder sink = new LongAdder();
        private final AtomicLong idSequence = new AtomicLong();

        @Setup(Level.Trial)
        public void setUp() {
            fold = Projections.reactiveUpdateWithMetadata(materializedView(workMicros, sink));
        }

        void applyOnePayload() {
            fold.apply(EventMetadata.empty(), idSequence.getAndIncrement()).block();
        }
    }

    /**
     * The same fold as {@link MaterializedViewWrappedState}, called directly on the calling thread with no
     * {@code Mono}/{@code subscribeOn} hop at all, the shape removing the wrapping would leave.
     */
    @State(Scope.Benchmark)
    public static class MaterializedViewUnwrappedState {

        @Param({"50", "200"})
        public long workMicros;

        private MaterializedView<Long> materializedView;
        private final LongAdder sink = new LongAdder();
        private final AtomicLong idSequence = new AtomicLong();

        @Setup(Level.Trial)
        public void setUp() {
            materializedView = materializedView(workMicros, sink);
        }

        void applyOnePayload() {
            materializedView.update(EventMetadata.empty(), idSequence.getAndIncrement());
        }
    }

    // --- CoalescingMaterializedUpdate site: Projections.reactiveUpdateWithMetadata(Projection, ViewStateRepository, String) ---

    /**
     * The real, shipped write-through fold behind {@code Projections.reactiveUpdateWithMetadata(Projection,
     * ViewStateRepository, String)}: {@code CoalescingMaterializedUpdate.apply}, wrapped in the same
     * {@code Mono.fromRunnable(..).subscribeOn(Schedulers.boundedElastic())} shape. No replay is started, so every
     * call takes the write-through branch (read, fold, save) rather than the batching one.
     */
    @State(Scope.Benchmark)
    public static class CoalescingWrappedState {

        @Param({"50", "200"})
        public long workMicros;

        private BiFunction<EventMetadata, Long, Mono<Void>> fold;
        private final LongAdder sink = new LongAdder();
        private final AtomicLong idSequence = new AtomicLong();

        @Setup(Level.Trial)
        public void setUp() {
            fold = Projections.reactiveUpdateWithMetadata(singletonProjection(workMicros, sink), newRepository(), "singleton");
        }

        void applyOnePayload() {
            fold.apply(EventMetadata.empty(), idSequence.getAndIncrement()).block();
        }
    }

    /**
     * The same read-fold-save sequence {@code CoalescingMaterializedUpdate.apply}'s write-through branch runs, called
     * directly on the calling thread with no {@code Mono}/{@code subscribeOn} hop, the shape removing the wrapping
     * would leave.
     */
    @State(Scope.Benchmark)
    public static class CoalescingUnwrappedState {

        @Param({"50", "200"})
        public long workMicros;

        private View<Long, Long> view;
        private ViewStateRepository<Long, String> repository;
        private final LongAdder sink = new LongAdder();
        private final AtomicLong idSequence = new AtomicLong();

        @Setup(Level.Trial)
        public void setUp() {
            view = singletonProjection(workMicros, sink).view();
            repository = newRepository();
        }

        void applyOnePayload() {
            Long event = idSequence.getAndIncrement();
            Long currentState = repository.findById("singleton").orElse(view.initialState());
            repository.save("singleton", view.evolve(currentState, EventMetadata.empty(), event));
        }
    }

    @Benchmark
    @Threads(1)
    public void materializedView_wrapped_threads1(MaterializedViewWrappedState state) {
        state.applyOnePayload();
    }

    @Benchmark
    @Threads(2)
    public void materializedView_wrapped_threads2(MaterializedViewWrappedState state) {
        state.applyOnePayload();
    }

    @Benchmark
    @Threads(4)
    public void materializedView_wrapped_threads4(MaterializedViewWrappedState state) {
        state.applyOnePayload();
    }

    @Benchmark
    @Threads(8)
    public void materializedView_wrapped_threads8(MaterializedViewWrappedState state) {
        state.applyOnePayload();
    }

    @Benchmark
    @Threads(1)
    public void materializedView_unwrapped_threads1(MaterializedViewUnwrappedState state) {
        state.applyOnePayload();
    }

    @Benchmark
    @Threads(2)
    public void materializedView_unwrapped_threads2(MaterializedViewUnwrappedState state) {
        state.applyOnePayload();
    }

    @Benchmark
    @Threads(4)
    public void materializedView_unwrapped_threads4(MaterializedViewUnwrappedState state) {
        state.applyOnePayload();
    }

    @Benchmark
    @Threads(8)
    public void materializedView_unwrapped_threads8(MaterializedViewUnwrappedState state) {
        state.applyOnePayload();
    }

    @Benchmark
    @Threads(1)
    public void coalescing_wrapped_threads1(CoalescingWrappedState state) {
        state.applyOnePayload();
    }

    @Benchmark
    @Threads(2)
    public void coalescing_wrapped_threads2(CoalescingWrappedState state) {
        state.applyOnePayload();
    }

    @Benchmark
    @Threads(4)
    public void coalescing_wrapped_threads4(CoalescingWrappedState state) {
        state.applyOnePayload();
    }

    @Benchmark
    @Threads(8)
    public void coalescing_wrapped_threads8(CoalescingWrappedState state) {
        state.applyOnePayload();
    }

    @Benchmark
    @Threads(1)
    public void coalescing_unwrapped_threads1(CoalescingUnwrappedState state) {
        state.applyOnePayload();
    }

    @Benchmark
    @Threads(2)
    public void coalescing_unwrapped_threads2(CoalescingUnwrappedState state) {
        state.applyOnePayload();
    }

    @Benchmark
    @Threads(4)
    public void coalescing_unwrapped_threads4(CoalescingUnwrappedState state) {
        state.applyOnePayload();
    }

    @Benchmark
    @Threads(8)
    public void coalescing_unwrapped_threads8(CoalescingUnwrappedState state) {
        state.applyOnePayload();
    }
}
