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

package org.occurrent.subscription.api.reactor.internal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.internal.HandoverMessages;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

// A stop that finds a catch-up running is owed until a catch-up answers the waiting payloads. These cover each way
// that debt is paid or settled, and that a payload never waits once no catch-up runs after a stop.
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactiveHandoverOwedStopTest {

    private static final String STOPPED = "STOPPED";
    private static final String APPLIED = "APPLIED";
    private static final String WAITING = "WAITING";

    private final List<String> delivered = new CopyOnWriteArrayList<>();
    private final ReactiveHandover<String, String> handover = ReactiveHandover.create(
            payload -> Mono.fromRunnable(() -> delivered.add(payload)), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @AfterEach
    void shutDownExecutor() {
        executor.shutdownNow();
    }

    @Test
    void a_catch_up_going_live_delivers_what_an_owed_stop_would_have_answered_and_a_later_throw_stops_nothing() throws Exception {
        CompletableFuture<Void> l1 = accept("L1");
        HeldCatchUp first = new HeldCatchUp();
        handover.stopIfNotCatchingUp();

        assertThat(catchUp(true)).isTrue();
        first.throwNow();

        assertThat(List.of(outcome(l1), outcome(accept("L2")))).containsExactly(APPLIED, APPLIED);
        assertThat(delivered).contains("L1", "L2");
    }

    @Test
    void a_replay_stopped_through_keepReplaying_answers_what_an_owed_stop_would_have_and_the_handover_stays_stopped() throws Exception {
        CompletableFuture<Void> l1 = accept("L1");
        HeldCatchUp first = new HeldCatchUp();
        handover.stopIfNotCatchingUp();

        assertThat(catchUp(false)).isFalse();
        first.throwNow();

        assertThat(List.of(outcome(l1), outcome(accept("L2")))).containsExactly(STOPPED, STOPPED);
        assertThat(catchUp(true)).isTrue();
        assertThat(outcome(accept("L3"))).isEqualTo(APPLIED);
        assertThat(delivered).doesNotContain("L1", "L2");
    }

    @Test
    void an_owed_stop_kept_past_a_throw_is_settled_by_a_later_catch_up_going_live() throws Exception {
        CompletableFuture<Void> l1 = accept("L1");
        HeldCatchUp first = new HeldCatchUp();
        handover.stopIfNotCatchingUp();
        HeldCatchUp second = new HeldCatchUp();
        first.throwNow();

        assertThat(catchUp(true)).isTrue();
        second.throwNow();

        assertThat(List.of(outcome(l1), outcome(accept("L2")))).containsExactly(APPLIED, APPLIED);
    }

    @Test
    void a_failing_catch_up_errors_what_an_owed_stop_would_have_answered_with_its_failure() throws Exception {
        CompletableFuture<Void> l1 = accept("L1");
        HeldCatchUp first = new HeldCatchUp();
        handover.stopIfNotCatchingUp();

        try {
            handover.catchUp(source(Flux.error(new IllegalStateException("replay failed")), true)).block(Duration.ofSeconds(10));
        } catch (RuntimeException expected) {
            // The catch-up's own failure.
        }
        first.throwNow();

        assertThat(outcome(l1)).startsWith("REFUSED:").isNotEqualTo("REFUSED:" + HandoverMessages.stoppedBeforeApplied("test payload"));
    }

    @Test
    void a_catch_up_going_live_after_an_owed_stop_was_paid_is_not_stopped_by_a_throw() throws Exception {
        CompletableFuture<Void> l1 = accept("L1");
        HeldCatchUp first = new HeldCatchUp();
        handover.stopIfNotCatchingUp();
        HeldCatchUp second = new HeldCatchUp();
        first.throwNow();
        handover.stopIfNotCatchingUp();
        second.throwNow();
        assertThat(outcome(l1)).isEqualTo(STOPPED);

        HeldCatchUp third = new HeldCatchUp();
        CompletableFuture<Void> l2 = accept("L2");
        assertThat(catchUp(true)).isTrue();
        third.throwNow();

        assertThat(outcome(l2)).as("L2, fed while the third catch-up ran").isEqualTo(APPLIED);
        assertThat(outcome(accept("L3"))).isEqualTo(APPLIED);
    }

    @Test
    void catch_ups_that_throw_while_another_one_replays_do_not_stop_the_handover_that_one_takes_live() throws Exception {
        CompletableFuture<Void> l1 = accept("L1");
        HeldCatchUp first = new HeldCatchUp();
        handover.stopIfNotCatchingUp();
        HeldCatchUp second = new HeldCatchUp();
        CountDownLatch replaying = new CountDownLatch(1);
        CountDownLatch finishReplay = new CountDownLatch(1);
        CompletableFuture<Boolean> slow = handover.catchUp(source(Flux.just("r1").doOnNext(replayed -> {
            replaying.countDown();
            await(finishReplay);
        }), true)).toFuture();
        await(replaying);

        first.throwNow();
        second.throwNow();
        CompletableFuture<Void> l2 = accept("L2");
        finishReplay.countDown();

        assertThat(slow.get(10, TimeUnit.SECONDS)).isTrue();
        assertThat(List.of(outcome(l1), outcome(l2))).containsExactly(APPLIED, APPLIED);
    }

    // The replay's stop clears the owed stop and then tells the source the replay was abandoned, before it gives its
    // count back. A catch-up called in that window clears the stop, throws, and finds the stopping one still counted.
    @Test
    void a_catch_up_that_throws_while_a_replay_stopped_through_keepReplaying_is_ending_leaves_the_handover_stopped() throws Exception {
        CountDownLatch abandoning = new CountDownLatch(1);
        CountDownLatch otherThrew = new CountDownLatch(1);
        ReactiveHandover.Source<String> stopping = new ReactiveHandover.Source<>() {
            @Override
            public Mono<Boolean> isAlreadyCaughtUp() {
                return Mono.just(false);
            }

            @Override
            public Flux<String> replay() {
                return Flux.just("r1");
            }

            @Override
            public Mono<Void> markCaughtUp() {
                return Mono.empty();
            }

            @Override
            public boolean keepReplaying() {
                return false;
            }

            @Override
            public void replayAbandoned() {
                abandoning.countDown();
                await(otherThrew);
            }
        };
        CompletableFuture<Boolean> stopped = handover.catchUp(stopping).toFuture();
        await(abandoning);
        Future<?> throwing = executor.submit(() -> handover.catchUp(sourceThatThrowsWhenAsked(new CountDownLatch(1), new CountDownLatch(0))));
        assertThat(failureOf(throwing)).hasMessage("marker unreadable");
        otherThrew.countDown();
        assertThat(stopped.get(10, TimeUnit.SECONDS)).isFalse();

        assertThat(outcome(accept("L1"))).as("what accept(L1) ended with once the stopped replay ended and the other catch-up threw")
                .isEqualTo(STOPPED);
    }

    // Stops, catch-ups that throw, replays stopped through keepReplaying(), replays that go live or fail, and payloads,
    // all racing on one handover. Once every catch-up has ended without one going live or failing, every payload has
    // an answer and a payload fed then is answered at once. A closing stop then answers whatever is left, and no
    // payload may be delivered twice, or be delivered and answered as not applied. Runs for a few seconds by default,
    // set occurrent.test.raceSeconds to run longer.
    @Test
    @Timeout(1800)
    void no_payload_waits_once_no_catch_up_runs_after_a_stop_whatever_the_interleaving() throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(Long.getLong("occurrent.test.raceSeconds", 3));
        int rounds = 0;
        int roundsWithNoCatchUpLeft = 0;
        Set<String> violations = ConcurrentHashMap.newKeySet();
        ExecutorService pool = Executors.newFixedThreadPool(10);
        try {
            while (System.nanoTime() < deadline && violations.isEmpty()) {
                rounds++;
                int round = rounds;
                ThreadLocalRandom random = ThreadLocalRandom.current();
                Map<String, Integer> deliveredCount = new ConcurrentHashMap<>();
                ReactiveHandover<String, String> raced = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> {
                            if (!payload.startsWith("r")) {
                                deliveredCount.merge(payload, 1, Integer::sum);
                            }
                        }), payload -> payload,
                        new CatchupThenLiveOptions(CatchupThenLiveOptions.DEFAULT_DEDUP_CACHE_SIZE, 4 + random.nextInt(30)), "test payload");
                int feeders = 2;
                int stoppers = 1 + random.nextInt(2);
                int catchUps = 1 + random.nextInt(4);
                // Most rounds use only catch-ups that throw or stop their replay, so the handover never goes live.
                boolean allowLive = random.nextInt(4) == 0;
                boolean allowFailure = random.nextInt(6) == 0;
                AtomicBoolean wentLiveOrFailed = new AtomicBoolean();
                Map<String, CompletableFuture<Boolean>> accepts = new ConcurrentHashMap<>();
                List<CompletableFuture<?>> catchUpResults = new CopyOnWriteArrayList<>();
                CyclicBarrier start = new CyclicBarrier(feeders + stoppers + catchUps);
                List<Future<?>> tasks = new ArrayList<>();
                for (int f = 0; f < feeders; f++) {
                    int feeder = f;
                    tasks.add(pool.submit(() -> {
                        awaitBarrier(start);
                        for (int i = 0; i < 15; i++) {
                            String id = "p" + round + "-" + feeder + "-" + i;
                            CompletableFuture<Boolean> result = new CompletableFuture<>();
                            accepts.put(id, result);
                            raced.acceptReportingDelivery(id).subscribe(result::complete, result::completeExceptionally, () -> result.complete(null));
                            spin(ThreadLocalRandom.current().nextInt(100));
                        }
                    }));
                }
                for (int s = 0; s < stoppers; s++) {
                    tasks.add(pool.submit(() -> {
                        awaitBarrier(start);
                        for (int i = 0; i < 3; i++) {
                            spin(ThreadLocalRandom.current().nextInt(400));
                            raced.stopIfNotCatchingUp();
                        }
                    }));
                }
                for (int c = 0; c < catchUps; c++) {
                    CatchUpKind kind = CatchUpKind.pick(random.nextInt(10), allowLive, allowFailure);
                    tasks.add(pool.submit(() -> {
                        awaitBarrier(start);
                        spin(ThreadLocalRandom.current().nextInt(400));
                        try {
                            catchUpResults.add(raced.catchUp(racingSource(kind)).toFuture().whenComplete((finished, error) -> {
                                if (Boolean.TRUE.equals(finished) || kind == CatchUpKind.REPLAY_FAILS) {
                                    wentLiveOrFailed.set(true);
                                }
                            }));
                        } catch (IllegalStateException expected) {
                            // A catch-up that throws when asked whether it is caught up.
                        }
                    }));
                }
                for (Future<?> task : tasks) {
                    task.get(30, TimeUnit.SECONDS);
                }
                for (CompletableFuture<?> result : catchUpResults) {
                    try {
                        result.get(30, TimeUnit.SECONDS);
                    } catch (ExecutionException ignored) {
                        // A failed replay, or one refused because another failed.
                    } catch (TimeoutException e) {
                        violations.add("a catch-up did not end in round " + round);
                    }
                }
                if (!wentLiveOrFailed.get()) {
                    roundsWithNoCatchUpLeft++;
                    long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                    for (Map.Entry<String, CompletableFuture<Boolean>> accepted : accepts.entrySet()) {
                        try {
                            accepted.getValue().get(Math.max(1, until - System.nanoTime()), TimeUnit.NANOSECONDS);
                        } catch (ExecutionException ignored) {
                            // Answered.
                        } catch (TimeoutException e) {
                            violations.add(accepted.getKey() + " unanswered once no catch-up ran, round " + round);
                            break;
                        }
                    }
                    CompletableFuture<Boolean> fedAfter = raced.acceptReportingDelivery("after-" + round).toFuture();
                    try {
                        if (!Boolean.FALSE.equals(fedAfter.get(3, TimeUnit.SECONDS))) {
                            violations.add("a payload fed once no catch-up ran was not answered as not applied, round " + round);
                        }
                    } catch (ExecutionException | TimeoutException e) {
                        violations.add("a payload fed once no catch-up ran ended with " + e + ", round " + round);
                    }
                }
                raced.stopIfNotCatchingUp();
                for (Map.Entry<String, CompletableFuture<Boolean>> accepted : accepts.entrySet()) {
                    String id = accepted.getKey();
                    try {
                        Boolean applied = accepted.getValue().get(10, TimeUnit.SECONDS);
                        if (Boolean.TRUE.equals(applied) && !deliveredCount.containsKey(id)) {
                            violations.add(id + " answered as applied but never delivered");
                        }
                        if (Boolean.FALSE.equals(applied) && deliveredCount.containsKey(id)) {
                            violations.add(id + " answered as not applied but delivered");
                        }
                    } catch (ExecutionException e) {
                        if (!(e.getCause() instanceof ReactiveHandover.PreDispatchRefusalException)) {
                            violations.add(id + " ended with " + e.getCause());
                        } else if (deliveredCount.containsKey(id)) {
                            violations.add(id + " refused but delivered");
                        }
                    } catch (TimeoutException e) {
                        violations.add(id + " unanswered after the closing stop, round " + round);
                        break;
                    }
                }
                deliveredCount.forEach((id, times) -> {
                    if (times > 1) {
                        violations.add(id + " delivered " + times + " times");
                    }
                });
            }
        } finally {
            pool.shutdownNow();
        }
        System.out.println("ReactiveHandoverOwedStopTest race: " + rounds + " rounds, " + roundsWithNoCatchUpLeft + " checked with no catch-up left");
        assertThat(violations).as("violations after " + rounds + " rounds").isEmpty();
    }

    private enum CatchUpKind {
        THROWS, GOES_LIVE, STOPS_ITS_REPLAY, REPLAY_FAILS;

        static CatchUpKind pick(int dice, boolean allowLive, boolean allowFailure) {
            if (dice < 6) {
                return THROWS;
            }
            if (dice < 8 || (!allowLive && !allowFailure)) {
                return STOPS_ITS_REPLAY;
            }
            if (allowFailure && dice == 8) {
                return REPLAY_FAILS;
            }
            return allowLive ? GOES_LIVE : STOPS_ITS_REPLAY;
        }
    }

    private static ReactiveHandover.Source<String> racingSource(CatchUpKind kind) {
        AtomicInteger seen = new AtomicInteger();
        int stopAfter = ThreadLocalRandom.current().nextInt(3);
        return new ReactiveHandover.Source<>() {
            @Override
            public Mono<Boolean> isAlreadyCaughtUp() {
                if (kind == CatchUpKind.THROWS) {
                    spin(ThreadLocalRandom.current().nextInt(300));
                    throw new IllegalStateException("marker unreadable");
                }
                return Mono.just(false);
            }

            @Override
            public Flux<String> replay() {
                return kind == CatchUpKind.REPLAY_FAILS
                        ? Flux.just("r1").concatWith(Flux.error(new IllegalStateException("replay failed")))
                        : Flux.just("r1", "r2", "r3");
            }

            @Override
            public boolean keepReplaying() {
                return kind != CatchUpKind.STOPS_ITS_REPLAY || seen.incrementAndGet() <= stopAfter;
            }

            @Override
            public Mono<Void> markCaughtUp() {
                return Mono.empty();
            }
        };
    }

    // A catch-up held on its own thread inside isAlreadyCaughtUp(), which throws once released.
    private final class HeldCatchUp {
        private final CountDownLatch release = new CountDownLatch(1);
        private final Future<?> call;

        HeldCatchUp() {
            CountDownLatch asking = new CountDownLatch(1);
            call = executor.submit(() -> handover.catchUp(sourceThatThrowsWhenAsked(asking, release)));
            await(asking);
        }

        void throwNow() throws Exception {
            release.countDown();
            assertThat(failureOf(call)).hasMessage("marker unreadable");
        }
    }

    private static ReactiveHandover.Source<String> sourceThatThrowsWhenAsked(CountDownLatch asking, CountDownLatch release) {
        return new ReactiveHandover.Source<>() {
            @Override
            public Mono<Boolean> isAlreadyCaughtUp() {
                asking.countDown();
                await(release);
                throw new IllegalStateException("marker unreadable");
            }

            @Override
            public Flux<String> replay() {
                return Flux.empty();
            }

            @Override
            public Mono<Void> markCaughtUp() {
                return Mono.empty();
            }
        };
    }

    private static ReactiveHandover.Source<String> source(Flux<String> replay, boolean keepReplaying) {
        return new ReactiveHandover.Source<>() {
            @Override
            public Mono<Boolean> isAlreadyCaughtUp() {
                return Mono.just(false);
            }

            @Override
            public Flux<String> replay() {
                return replay;
            }

            @Override
            public Mono<Void> markCaughtUp() {
                return Mono.empty();
            }

            @Override
            public boolean keepReplaying() {
                return keepReplaying;
            }
        };
    }

    private Boolean catchUp(boolean keepReplaying) {
        return handover.catchUp(source(Flux.just("r1"), keepReplaying)).block(Duration.ofSeconds(10));
    }

    private CompletableFuture<Void> accept(String payload) {
        return handover.accept(payload).toFuture();
    }

    private static String outcome(CompletableFuture<Void> accepted) {
        try {
            accepted.get(5, TimeUnit.SECONDS);
            return APPLIED;
        } catch (ExecutionException e) {
            String message = e.getCause().getMessage();
            return message.equals(HandoverMessages.stoppedBeforeApplied("test payload")) ? STOPPED : "REFUSED:" + message;
        } catch (TimeoutException e) {
            return WAITING;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static Throwable failureOf(Future<?> call) throws Exception {
        try {
            call.get(10, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            return e.getCause();
        }
        throw new AssertionError("the catch-up did not throw");
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("timed out waiting for the latch");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static void awaitBarrier(CyclicBarrier barrier) {
        try {
            barrier.await(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static void spin(int micros) {
        long end = System.nanoTime() + micros * 1000L;
        while (System.nanoTime() < end) {
            Thread.onSpinWait();
        }
    }
}
