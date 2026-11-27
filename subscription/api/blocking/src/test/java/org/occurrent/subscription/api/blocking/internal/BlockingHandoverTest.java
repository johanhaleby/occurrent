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

package org.occurrent.subscription.api.blocking.internal;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.internal.HandoverMessages;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

@DisplayNameGeneration(ReplaceUnderscores.class)
class BlockingHandoverTest {

    private static final String NOUN = "thing";
    private static final String INTERRUPTED_READ = "store read on an interrupted thread";

    @Test
    void live_payloads_accepted_before_catch_up_are_buffered_and_delivered_after_the_replay_in_order() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String, String> handover = handover(delivered);

        handover.acceptReportingDelivery("L1");
        handover.acceptReportingDelivery("L2");

        handover.catchUp(source(List.of("R1", "R2"), false));

        assertThat(delivered).containsExactly("R1", "R2", "L1", "L2");

        handover.accept("L3");
        assertThat(delivered).containsExactly("R1", "R2", "L1", "L2", "L3");
    }

    @Test
    void the_buffer_is_drained_and_the_marker_is_recorded_only_after_every_buffered_live_payload_was_delivered() {
        List<String> log = Collections.synchronizedList(new ArrayList<>());
        BlockingHandover<String, String> handover = BlockingHandover.create(log::add, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);

        handover.acceptReportingDelivery("L1");
        FakeSource source = source(List.of("R1"), false);
        source.onMarkCaughtUp = () -> log.add("marker");

        handover.catchUp(source);

        // Load-bearing order for the blocking engine: replay, then the buffered live payload, then the marker.
        assertThat(log).containsExactly("R1", "L1", "marker");
    }

    @Test
    void when_already_caught_up_the_replay_is_skipped_the_marker_is_not_recorded_again_but_buffered_live_payloads_are_still_delivered() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String, String> handover = handover(delivered);

        handover.acceptReportingDelivery("L1");
        FakeSource source = source(List.of("R1"), true);

        handover.catchUp(source);

        assertThat(source.replayCallCount).isZero();
        assertThat(source.markCaughtUpCallCount).isZero();
        assertThat(delivered).containsExactly("L1");
    }

    @Test
    void a_payload_already_delivered_by_the_replay_is_not_delivered_again_whether_buffered_or_live() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String, String> handover = handover(delivered);

        // Buffered before the replay runs, but shares the replay's dedup id.
        handover.acceptReportingDelivery("1");
        handover.catchUp(source(List.of("1"), false));

        assertThat(delivered).containsExactly("1");

        // A second live copy of the same id, arriving after the engine has gone live, is skipped too.
        handover.accept("1");
        assertThat(delivered).containsExactly("1");
    }

    // The replay applies an event and the live copy of it is suppressed, so on the recording paths neither delivery
    // would write down the append it came from. The suppression tells the source instead, once per suppressed copy,
    // and for both timings, the copy that buffered during the replay and the one that arrived after the drain.
    @Test
    void a_payload_the_replay_delivered_reaches_the_source_when_its_live_copy_is_suppressed() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String, String> handover = handover(delivered);
        FakeSource source = source(List.of("1"), false);

        handover.acceptReportingDelivery("1");
        handover.catchUp(source);

        assertThat(delivered).containsExactly("1");
        assertThat(source.alreadyDeliveredByReplay).containsExactly("1");

        handover.accept("1");

        assertThat(delivered).containsExactly("1");
        assertThat(source.alreadyDeliveredByReplay).containsExactly("1", "1");
    }

    // The negative half, and the one that would still pass with a single cache. A repeat the replay never delivered
    // was already delivered live, and that delivery wrote down whatever it owed, so telling the source again would
    // have it write the same thing twice for one event.
    @Test
    void a_payload_an_earlier_live_delivery_handled_reaches_the_source_again_for_nothing() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String, String> handover = handover(delivered);
        FakeSource source = source(List.of(), false);
        handover.catchUp(source);

        handover.accept("A");
        handover.accept("A");

        assertThat(delivered).containsExactly("A");
        assertThat(source.alreadyDeliveredByReplay).isEmpty();
    }

    // The test above only repeats an id the replay already delivered, so nothing covered a repeat that was only ever
    // live. That case is the common one in production, because a push sink acknowledges after the fold, so the broker
    // sends the event again whenever a fold throws. Below, A sent twice in a row is folded once. A, B, C, A folds A
    // twice, because the cache only holds two ids here and B and C pushed A out of it.
    @Test
    void a_live_payload_sent_twice_is_folded_once_until_the_cache_forgets_it() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(
                delivered::add, payload -> payload, new CatchupThenLiveOptions(2, CatchupThenLiveOptions.DEFAULT_MAX_BUFFERED_EVENTS), NOUN);
        handover.catchUp(source(List.of(), false));

        handover.accept("A");
        handover.accept("A");
        assertThat(delivered).containsExactly("A");

        handover.accept("B");
        handover.accept("C");
        handover.accept("A");
        assertThat(delivered).containsExactly("A", "B", "C", "A");
    }

    // A push sink acknowledges after the fold, so a fold that throws must not be recorded as delivered, or the
    // broker's redelivery of the same payload would be skipped as a duplicate and the event lost for good.
    @Test
    void a_live_payload_whose_delivery_throws_is_not_recorded_as_delivered_so_a_redelivery_is_retried() {
        List<String> delivered = new ArrayList<>();
        AtomicBoolean failNext = new AtomicBoolean(true);
        BlockingHandover<String, String> handover = BlockingHandover.create(
                payload -> {
                    if (failNext.getAndSet(false)) {
                        throw new RuntimeException("delivery boom");
                    }
                    delivered.add(payload);
                },
                payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handover.catchUp(source(List.of(), true));

        assertThatThrownBy(() -> handover.accept("A")).hasMessage("delivery boom");
        assertThat(delivered).isEmpty();

        handover.accept("A");
        assertThat(delivered).containsExactly("A");
    }

    @Test
    void exceeding_the_max_buffered_events_cap_while_replaying_fails_loud_with_the_documented_message() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch replaying = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            if (payload.equals("R1")) {
                replaying.countDown();
                awaitLatch(releaseReplay);
            }
            delivered.add(payload);
        }, payload -> payload, new CatchupThenLiveOptions(CatchupThenLiveOptions.DEFAULT_DEDUP_CACHE_SIZE, 2), NOUN);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> catchUp = executor.submit(() -> handover.catchUp(source(List.of("R1"), false)));
            awaitLatch(replaying);
            handover.acceptReportingDelivery("L1");
            handover.acceptReportingDelivery("L2");

            Throwable thrown = catchThrowable(() -> handover.acceptReportingDelivery("L3"));
            releaseReplay.countDown();

            assertThat(thrown).isInstanceOf(IllegalStateException.class)
                    .hasMessage(HandoverMessages.bufferOverflow(2))
                    .hasMessageContaining("(cap 2)");
            assertThatThrownBy(() -> catchUp.get(5, TimeUnit.SECONDS)).as("the catch-up the overflow happened in")
                    .isInstanceOf(ExecutionException.class).hasRootCauseMessage(HandoverMessages.bufferOverflow(2));
        } finally {
            releaseReplay.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void a_failed_catch_up_makes_a_subsequent_accept_fail_fast_with_the_original_failure_as_its_cause() {
        BlockingHandover<String, String> handover = handover(new ArrayList<>());

        RuntimeException replayFailure = new RuntimeException("replay boom");
        FakeSource source = source(List.of(), false);
        source.replayFailure = replayFailure;

        Throwable thrownByCatchUp = catchThrowable(() -> handover.catchUp(source));
        assertThat(thrownByCatchUp).isSameAs(replayFailure);

        Throwable thrownByAccept = catchThrowable(() -> handover.accept("L1"));
        assertThat(thrownByAccept).isInstanceOf(IllegalStateException.class)
                .hasMessage(HandoverMessages.catchUpFailed(NOUN));
        assertThat(thrownByAccept.getCause()).isSameAs(replayFailure);
    }

    /**
     * The direct case for the accessor {@code CatchupProjectionFeed} and {@code DomainEventFeed} delegate to. Covers
     * all three states it distinguishes, not yet live, live, and permanently failed. The failure half matters most.
     * A later catch-up that itself reaches live must not revive a handover an earlier failure already poisoned, and
     * that is exactly what the round-11 delegation exists to get right.
     */
    @Test
    void is_ready_for_live_delivery_is_false_before_catch_up_true_once_live_and_false_forever_after_a_failure() {
        BlockingHandover<String, String> handover = handover(new ArrayList<>());

        assertThat(handover.isReadyForLiveDelivery()).as("nothing has run yet").isFalse();

        handover.catchUp(source(List.of("R1"), false));
        assertThat(handover.isReadyForLiveDelivery()).as("the catch-up reached live").isTrue();

        RuntimeException replayFailure = new RuntimeException("replay boom");
        FakeSource failingSource = source(List.of(), false);
        failingSource.replayFailure = replayFailure;
        Throwable thrown = catchThrowable(() -> handover.catchUp(failingSource));
        assertThat(thrown).isSameAs(replayFailure);

        assertThat(handover.isReadyForLiveDelivery()).as("a failed catch-up leaves this permanently false, even "
                        + "though an earlier attempt had reached live")
                .isFalse();

        handover.catchUp(source(List.of("R2"), false));
        assertThat(handover.isReadyForLiveDelivery()).as("a later catch-up reaching live does not clear an earlier "
                        + "failure, since catchUpFailure is recorded once and never cleared")
                .isFalse();
    }

    @Test
    void accept_and_catch_up_reject_null_arguments_eagerly() {
        BlockingHandover<String, String> handover = handover(new ArrayList<>());

        assertThatThrownBy(() -> handover.accept(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("payload cannot be null");
        assertThatThrownBy(() -> handover.catchUp(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("source cannot be null");
    }

    @Test
    void a_null_de_dup_key_fails_loud_on_both_the_replay_and_the_live_path() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(
                delivered::add, payload -> null, CatchupThenLiveOptions.defaults(), NOUN);

        // Without the guard this reaches BoundedIdCache, whose eviction queue rejects a null element, so it surfaces as
        // a bare NullPointerException from inside the cache after the payload was already folded.
        assertThatThrownBy(() -> handover.catchUp(source(List.of("R1"), false)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(HandoverMessages.dedupKeyRequired());

        BlockingHandover<String, String> live = BlockingHandover.create(
                delivered::add, payload -> null, CatchupThenLiveOptions.defaults(), NOUN);
        live.catchUp(source(List.of(), true));
        assertThatThrownBy(() -> live.accept("L1"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(HandoverMessages.dedupKeyRequired());
    }

    @Test
    void deliver_runs_concurrently_for_live_payloads_instead_of_serialized_behind_the_handover_lock() throws Exception {
        int threadCount = 4;
        // Every thread's deliver call rendezvous here before any of them returns. If deliver were still called while
        // holding the handover's lock (the behaviour #588 measured and removed), only one thread could ever be inside
        // deliver at a time, so this barrier could never fill and the test would time out instead of completing.
        CyclicBarrier allInsideDeliverAtOnce = new CyclicBarrier(threadCount);
        BlockingHandover<String, String> handover = BlockingHandover.create(
                payload -> await(allInsideDeliverAtOnce), payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handover.catchUp(source(List.of(), true));

        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        try {
            List<Future<?>> deliveries = new ArrayList<>();
            for (int i = 0; i < threadCount; i++) {
                String payload = "L" + i;
                deliveries.add(pool.submit(() -> handover.accept(payload)));
            }
            for (Future<?> delivery : deliveries) {
                delivery.get(5, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdown();
        }
    }

    // tryReserve(..) answers null both for an already-delivered key and for a key another delivery is currently
    // running under. Conflating those two would report the second, concurrent caller as delivered before the
    // in-flight attempt had actually succeeded or failed.
    @Test
    void a_concurrent_delivery_of_the_same_key_already_in_flight_is_not_reported_as_delivered() throws Exception {
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        BlockingHandover<String, String> handover = BlockingHandover.create(
                payload -> {
                    firstStarted.countDown();
                    awaitLatch(releaseFirst);
                    delivered.add(payload);
                },
                payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handover.catchUp(source(List.of(), true));

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> first = pool.submit(() -> handover.acceptReportingDelivery("A"));
            awaitLatch(firstStarted);

            boolean secondResult = handover.acceptReportingDelivery("A");
            releaseFirst.countDown();

            assertThat(first.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(secondResult).as("the concurrent duplicate must not be reported delivered while the "
                            + "in-flight attempt for the same key has not itself succeeded yet")
                    .isFalse();
            assertThat(delivered).containsExactly("A");
        } finally {
            pool.shutdown();
        }
    }

    @Test
    void accept_throws_rather_than_returning_while_another_delivery_of_the_same_payload_is_still_running() throws Exception {
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        BlockingHandover<String, String> handover = BlockingHandover.create(
                payload -> {
                    firstStarted.countDown();
                    awaitLatch(releaseFirst);
                    delivered.add(payload);
                },
                payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handover.catchUp(source(List.of(), true));

        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<?> first = pool.submit(() -> handover.accept("A"));
            awaitLatch(firstStarted);

            Throwable thrownBySecond = catchThrowable(() -> handover.accept("A"));
            releaseFirst.countDown();

            first.get(5, TimeUnit.SECONDS);
            assertThat(thrownBySecond).as("what the second accept(..) threw while the first was still delivering")
                    .isInstanceOf(BlockingHandover.PreDispatchRefusalException.class)
                    .hasMessage(HandoverMessages.sameEventStillDelivering(NOUN));
            assertThat(delivered).containsExactly("A");
        } finally {
            releaseFirst.countDown();
            pool.shutdown();
        }
    }

    // The drain meets the same race. A copy buffered while a catch-up waits for the first live delivery of that
    // payload is a repeat of a key still in flight, and a catch-up with nothing to replay drains it before the first
    // delivery ends.
    @Test
    void a_waiting_accept_throws_when_the_drain_finds_another_delivery_of_the_same_payload_still_running() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch deliveringK = new CountDownLatch(1);
        CountDownLatch releaseK = new CountDownLatch(1);
        BlockingHandover<String, String> handover = handoverHoldingItsReplayAt("K", delivered, deliveringK, releaseK, null);
        handover.catchUp(source(List.of(), true));
        AtomicReference<Throwable> thrownByFirst = new AtomicReference<>();
        AtomicReference<Throwable> thrownByRepeat = new AtomicReference<>();
        AtomicBoolean replayCaughtUp = new AtomicBoolean();
        Thread first = new Thread(() -> thrownByFirst.set(catchThrowable(() -> handover.accept("K"))), "first-delivery");
        Thread replay = new Thread(() -> replayCaughtUp.set(handover.catchUp(source(List.of("R1"), false))), "replay");
        Thread repeat = new Thread(() -> thrownByRepeat.set(catchThrowable(() -> handover.accept("K"))), "repeat-delivery");
        try {
            first.start();
            awaitLatch(deliveringK);
            replay.start();
            awaitWaiting(replay);
            repeat.start();
            awaitWaiting(repeat);

            assertThat(handover.catchUp(source(List.of(), true))).isTrue();
            repeat.join(5_000);

            assertThat(repeat.isAlive()).as("accept(..) of the repeat answered by the drain").isFalse();
            assertThat(thrownByRepeat.get()).as("what accept(..) of the repeat threw")
                    .isInstanceOf(BlockingHandover.PreDispatchRefusalException.class)
                    .hasMessage(HandoverMessages.sameEventStillDelivering(NOUN));

            releaseK.countDown();
            first.join(5_000);
            replay.join(5_000);
            assertThat(thrownByFirst.get()).as("what the first accept(..) threw").isNull();
            assertThat(replayCaughtUp).isTrue();
            assertThat(delivered).containsExactly("K", "R1");
        } finally {
            releaseK.countDown();
            repeat.interrupt();
            replay.interrupt();
        }
    }

    // acceptIfLive(..): a caller that can redeliver, unlike accept(..) and acceptReportingDelivery(..), which buffer
    // when not live. live_payloads_accepted_before_catch_up_are_buffered_and_delivered_after_the_replay_in_order above
    // covers that through acceptReportingDelivery(..), the write path's entry point.

    @Test
    void acceptIfLive_refuses_without_buffering_when_not_live() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String, String> handover = handover(delivered);

        boolean landed = handover.acceptIfLive("L1");

        assertThat(landed).isFalse();
        assertThat(delivered).as("refused outright, never buffered").isEmpty();

        // Proof it was truly refused rather than silently buffered: a catch-up that reaches live delivers only the
        // replay's own history, never the refused payload.
        handover.catchUp(source(List.of("R1"), false));
        assertThat(delivered).containsExactly("R1");
    }

    @Test
    void acceptIfLive_delivers_when_live() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String, String> handover = handover(delivered);
        handover.catchUp(source(List.of(), true));

        boolean landed = handover.acceptIfLive("L1");

        assertThat(landed).isTrue();
        assertThat(delivered).containsExactly("L1");
    }

    @Test
    void acceptIfLive_reports_true_for_a_key_an_earlier_attempt_already_delivered() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String, String> handover = handover(delivered);
        handover.catchUp(source(List.of(), true));

        assertThat(handover.acceptIfLive("L1")).isTrue();
        assertThat(handover.acceptIfLive("L1")).as("already delivered, so a redelivery still lands true").isTrue();

        assertThat(delivered).as("folded once, not twice").containsExactly("L1");
    }

    @Test
    void acceptIfLive_reports_false_for_a_concurrent_delivery_of_the_same_key_already_in_flight() throws Exception {
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        BlockingHandover<String, String> handover = BlockingHandover.create(
                payload -> {
                    firstStarted.countDown();
                    awaitLatch(releaseFirst);
                    delivered.add(payload);
                },
                payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handover.catchUp(source(List.of(), true));

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> first = pool.submit(() -> handover.acceptIfLive("A"));
            awaitLatch(firstStarted);

            boolean secondResult = handover.acceptIfLive("A");
            releaseFirst.countDown();

            assertThat(first.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(secondResult).as("the concurrent duplicate must not be reported landed while the in-flight "
                            + "attempt for the same key has not itself succeeded yet, safe to redeliver again since "
                            + "it was never buffered either")
                    .isFalse();
            assertThat(delivered).containsExactly("A");
        } finally {
            pool.shutdown();
        }
    }

    /**
     * The regression guard for the exact bug a fresh-context review caught in an earlier draft: reading
     * {@code catchUpFailure} after the live check would let a permanently failed catch-up report {@code false}
     * (redeliver forever) instead of throwing, turning a real failure into an unbounded bypass-of-every-delivery-
     * failure-policy loop. {@code catchUpFailure} must be checked, and thrown, before the live check, exactly as
     * {@link BlockingHandover#acceptReportingDelivery(Object)} already orders it.
     */
    @Test
    void acceptIfLive_throws_rather_than_reports_false_after_a_catch_up_failure() {
        BlockingHandover<String, String> handover = handover(new ArrayList<>());
        RuntimeException replayFailure = new RuntimeException("replay boom");
        FakeSource failingSource = source(List.of(), false);
        failingSource.replayFailure = replayFailure;
        catchThrowable(() -> handover.catchUp(failingSource));

        Throwable thrown = catchThrowable(() -> handover.acceptIfLive("L1"));

        assertThat(thrown).isInstanceOf(IllegalStateException.class)
                .hasMessage(HandoverMessages.catchUpFailed(NOUN));
        assertThat(thrown.getCause()).isSameAs(replayFailure);
    }

    /**
     * dec-0011's amendment 1: a {@code stop()} on the wrapping subscription model interrupts a replay in flight
     * (this handover's own {@code stopped} state) at essentially the same moment {@code RegisteringSubscribable}
     * adds the subscription id to {@code pausedSubscriptions}, which {@code routeReportingMatch} checks before ever
     * reaching this method. The only window where {@code acceptIfLive} can observe {@code stopped} at all is the
     * narrow interleaving between those two, and this proves the bound: at most one {@code false} for a payload fed
     * into that window, not an unbounded loop, since a stopped-but-not-yet-poisoned handover keeps reporting
     * {@code false} exactly like "still replaying, never poisoned" does, not like a permanent failure.
     */
    @Test
    void acceptIfLive_reports_false_rather_than_throwing_when_stopped_but_not_poisoned() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String, String> handover = handover(delivered);

        assertThat(handover.acceptIfLive("before-any-catch-up")).as("never started").isFalse();

        FakeSource source = source(List.of("R1", "R2", "R3"), false);
        source.stopAfter(2);
        boolean caughtUp = handover.catchUp(source);
        assertThat(caughtUp).as("Source.keepReplaying() stopped it, not a failure").isFalse();

        boolean landedAfterStop = handover.acceptIfLive("after-stop");

        assertThat(landedAfterStop).as("stopped, not poisoned: refused rather than buffered, never thrown, the "
                        + "same as any other not-live case, per dec-0011's amendment 1")
                .isFalse();
        assertThat(delivered).as("nothing buffered or delivered through the refused payload").containsExactly("R1", "R2");
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            assertThat(latch.await(5, TimeUnit.SECONDS)).as("latch reached within the timeout").isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static void await(CyclicBarrier barrier) {
        try {
            barrier.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // --- helpers ---

    @Test
    void a_stopped_replay_reports_stopped_and_records_no_marker() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String, String> handover = handover(delivered);
        FakeSource source = source(List.of("R1", "R2", "R3"), false);
        source.stopAfter(2);

        boolean caughtUp = handover.catchUp(source);

        assertThat(caughtUp).isFalse();
        assertThat(delivered).containsExactly("R1", "R2");
        // The whole reason a stop is not just an early return: recording completion here would make the next catch-up
        // skip a history it never finished folding.
        assertThat(source.markCaughtUpCallCount()).isZero();
    }

    @Test
    void a_stopped_replay_leaves_the_handover_usable_rather_than_rejecting_every_later_payload() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String, String> handover = handover(delivered);
        FakeSource stopped = source(List.of("R1", "R2"), false);
        stopped.stopAfter(1);
        handover.catchUp(stopped);

        // Not applied, so accept(..) must not return as if it were. Refused as not applied rather than as a failed
        // catch-up, and the handover does not refuse for good, which is what lets a shared feed keep serving its other
        // projections.
        assertThat(handover.acceptReportingDelivery("L1")).isFalse();
        Throwable thrownByAccept = catchThrowable(() -> handover.accept("L1"));
        assertThat(thrownByAccept)
                .isInstanceOf(BlockingHandover.PreDispatchRefusalException.class)
                .hasMessage(HandoverMessages.stoppedBeforeApplied(NOUN));
        assertThat(handover.refusesPermanently()).isFalse();
        // Dropped rather than buffered, since nothing is coming to drain it.
        assertThat(delivered).containsExactly("R1");
    }

    @Test
    void accept_during_a_replay_returns_only_once_the_drain_has_applied_the_payload() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch replaying = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        BlockingHandover<String, String> handover = handoverHoldingItsReplayAt("R1", delivered, replaying, releaseReplay, null);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> catchUp = executor.submit(() -> handover.catchUp(source(List.of("R1"), false)));
            awaitLatch(replaying);

            AtomicReference<List<String>> appliedWhenAcceptReturned = new AtomicReference<>();
            Thread accepting = new Thread(() -> {
                handover.accept("L1");
                appliedWhenAcceptReturned.set(List.copyOf(delivered));
            }, "live-delivery");
            accepting.start();
            awaitWaiting(accepting);

            assertThat(accepting.isAlive()).as("accept(..) still waiting while the replay holds its payload").isTrue();
            releaseReplay.countDown();
            assertThat(catchUp.get(5, TimeUnit.SECONDS)).isTrue();
            accepting.join(5_000);
            assertThat(appliedWhenAcceptReturned.get()).as("what was applied when accept(..) returned")
                    .containsExactly("R1", "L1");
        } finally {
            releaseReplay.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void a_replay_stopped_while_accept_waits_makes_accept_throw_and_applies_nothing() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch replaying = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        BlockingHandover<String, String> handover = handoverHoldingItsReplayAt("R1", delivered, replaying, releaseReplay, null);
        FakeSource stopping = source(List.of("R1", "R2"), false);
        stopping.stopAfter(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> catchUp = executor.submit(() -> handover.catchUp(stopping));
            awaitLatch(replaying);

            AtomicReference<Throwable> thrownByAccept = new AtomicReference<>();
            Thread accepting = new Thread(() -> thrownByAccept.set(catchThrowable(() -> handover.accept("L1"))), "live-delivery");
            accepting.start();
            awaitWaiting(accepting);
            releaseReplay.countDown();

            assertThat(catchUp.get(5, TimeUnit.SECONDS)).isFalse();
            accepting.join(5_000);
            assertThat(thrownByAccept.get()).as("what accept(..) threw once the replay stopped")
                    .isInstanceOf(BlockingHandover.PreDispatchRefusalException.class)
                    .hasMessage(HandoverMessages.stoppedBeforeApplied(NOUN));
            assertThat(delivered).containsExactly("R1");
            assertThat(handover.refusesPermanently()).isFalse();
        } finally {
            releaseReplay.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void a_catch_up_failing_while_accept_waits_makes_accept_throw_the_failure() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch replaying = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        IllegalStateException foldFailure = new IllegalStateException("fold failed");
        BlockingHandover<String, String> handover = handoverHoldingItsReplayAt("R1", delivered, replaying, releaseReplay, foldFailure);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> catchUp = executor.submit(() -> handover.catchUp(source(List.of("R1"), false)));
            awaitLatch(replaying);

            AtomicReference<Throwable> thrownByAccept = new AtomicReference<>();
            Thread accepting = new Thread(() -> thrownByAccept.set(catchThrowable(() -> handover.accept("L1"))), "live-delivery");
            accepting.start();
            awaitWaiting(accepting);
            releaseReplay.countDown();

            assertThatThrownBy(() -> catchUp.get(5, TimeUnit.SECONDS)).hasCause(foldFailure);
            accepting.join(5_000);
            assertThat(thrownByAccept.get()).as("what accept(..) threw once the catch-up failed")
                    .isInstanceOf(BlockingHandover.PreDispatchRefusalException.class)
                    .hasMessage(HandoverMessages.catchUpFailed(NOUN))
                    .hasCause(foldFailure);
            assertThat(delivered).isEmpty();
        } finally {
            releaseReplay.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void stopping_a_handover_no_catch_up_has_started_makes_a_waiting_accept_throw() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        BlockingHandover<String, String> handover = handover(delivered);
        AtomicReference<Throwable> thrownByAccept = new AtomicReference<>();
        Thread accepting = new Thread(() -> thrownByAccept.set(catchThrowable(() -> handover.accept("L1"))), "live-delivery");
        accepting.start();
        awaitWaiting(accepting);

        handover.stopIfNotCatchingUp();

        accepting.join(5_000);
        assertThat(accepting.isAlive()).as("accept(..) released by the stop").isFalse();
        assertThat(thrownByAccept.get()).as("what accept(..) threw once the handover stopped")
                .isInstanceOf(BlockingHandover.PreDispatchRefusalException.class)
                .hasMessage(HandoverMessages.stoppedBeforeApplied(NOUN));
        assertThat(delivered).isEmpty();
    }

    @Test
    void an_interrupted_accept_throws_and_leaves_its_payload_out_of_the_drain() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        BlockingHandover<String, String> handover = handover(delivered);
        AtomicReference<Throwable> thrownByAccept = new AtomicReference<>();
        Thread accepting = new Thread(() -> thrownByAccept.set(catchThrowable(() -> handover.accept("L1"))), "live-delivery");
        accepting.start();
        awaitWaiting(accepting);

        accepting.interrupt();
        accepting.join(5_000);
        handover.catchUp(source(List.of("R1"), false));

        assertThat(thrownByAccept.get()).as("what accept(..) threw once interrupted")
                .isInstanceOf(BlockingHandover.PreDispatchRefusalException.class)
                .hasMessage(HandoverMessages.interruptedBeforeApplied(NOUN));
        assertThat(delivered).as("the payload its caller gave up on is not applied behind its back").containsExactly("R1");
    }

    @Test
    void accept_from_inside_the_replay_it_would_wait_for_is_refused_rather_than_deadlocking() {
        List<String> delivered = new ArrayList<>();
        AtomicReference<BlockingHandover<String, String>> self = new AtomicReference<>();
        AtomicReference<Throwable> thrownByAccept = new AtomicReference<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            if (payload.equals("R1")) {
                thrownByAccept.set(catchThrowable(() -> self.get().accept("L1")));
            }
            delivered.add(payload);
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        self.set(handover);

        assertThat(handover.catchUp(source(List.of("R1"), false))).isTrue();

        assertThat(thrownByAccept.get()).as("what accept(..) threw from the replay's own fold")
                .isInstanceOf(BlockingHandover.PreDispatchRefusalException.class)
                .hasMessage(HandoverMessages.acceptedFromOwnDelivery(NOUN));
        assertThat(delivered).containsExactly("R1");
    }

    // A live fold that feeds this handover again while a catch-up from another thread waits for that fold to end. The
    // nested payload would buffer and wait for the drain after a replay that cannot start until the fold returns.
    @Test
    void accept_from_inside_a_live_delivery_a_catch_up_waits_for_is_refused_rather_than_deadlocking() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch deliveringL1 = new CountDownLatch(1);
        CountDownLatch releaseL1 = new CountDownLatch(1);
        AtomicReference<BlockingHandover<String, String>> self = new AtomicReference<>();
        AtomicReference<Throwable> thrownByNestedAccept = new AtomicReference<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            if (payload.equals("L1")) {
                deliveringL1.countDown();
                awaitLatch(releaseL1);
                thrownByNestedAccept.set(catchThrowable(() -> self.get().accept("L2")));
            }
            delivered.add(payload);
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        self.set(handover);
        handover.catchUp(source(List.of(), true));
        Thread delivering = new Thread(() -> handover.accept("L1"), "live-delivery");
        Thread replay = new Thread(() -> handover.catchUp(source(List.of("R1"), false)), "replay");
        try {
            delivering.start();
            awaitLatch(deliveringL1);
            replay.start();
            awaitWaiting(replay);

            releaseL1.countDown();
            delivering.join(5_000);
            replay.join(5_000);

            assertThat(delivering.isAlive()).as("the live delivery ended rather than waiting on the catch-up").isFalse();
            assertThat(replay.isAlive()).as("the catch-up ended").isFalse();
            assertThat(thrownByNestedAccept.get()).as("what the nested accept(..) threw")
                    .isInstanceOf(BlockingHandover.PreDispatchRefusalException.class)
                    .hasMessage(HandoverMessages.acceptedFromOwnDelivery(NOUN));
            assertThat(delivered).containsExactly("L1", "R1");
        } finally {
            releaseL1.countDown();
            delivering.interrupt();
            replay.interrupt();
        }
    }

    // The same wait from inside Source.alreadyDeliveredByReplay(..), which a live copy of a replayed payload runs on
    // the caller's thread and which a catch-up waits for the same way it waits for a live fold.
    @Test
    void accept_from_inside_the_already_delivered_by_replay_callback_a_catch_up_waits_for_is_refused_rather_than_deadlocking() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(delivered::add, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        CountDownLatch reportingR1 = new CountDownLatch(1);
        CountDownLatch releaseReport = new CountDownLatch(1);
        AtomicReference<Throwable> thrownByNestedAccept = new AtomicReference<>();
        FakeSource replayed = source(List.of("R1"), false);
        replayed.onAlreadyDeliveredByReplay = () -> {
            reportingR1.countDown();
            awaitLatch(releaseReport);
            thrownByNestedAccept.set(catchThrowable(() -> handover.accept("L2")));
        };
        handover.catchUp(replayed);
        Thread reporting = new Thread(() -> handover.accept("R1"), "live-delivery");
        Thread replay = new Thread(() -> handover.catchUp(source(List.of("R2"), false)), "replay");
        try {
            reporting.start();
            awaitLatch(reportingR1);
            replay.start();
            awaitWaiting(replay);

            releaseReport.countDown();
            reporting.join(5_000);
            replay.join(5_000);

            assertThat(reporting.isAlive()).as("the live delivery ended rather than waiting on the catch-up").isFalse();
            assertThat(replay.isAlive()).as("the catch-up ended").isFalse();
            assertThat(thrownByNestedAccept.get()).as("what the nested accept(..) threw")
                    .isInstanceOf(BlockingHandover.PreDispatchRefusalException.class)
                    .hasMessage(HandoverMessages.acceptedFromOwnDelivery(NOUN));
            assertThat(delivered).containsExactly("R1", "R2");
        } finally {
            releaseReport.countDown();
            reporting.interrupt();
            replay.interrupt();
        }
    }

    // A catch-up with nothing to replay tells the source its history is done before it drains, while the handover is
    // not live yet, so a payload fed from there would wait for a drain that runs on the same thread after it returns.
    @Test
    void accept_from_the_drain_of_a_catch_up_with_nothing_to_replay_is_refused_rather_than_waiting_for_itself() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(delivered::add, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        AtomicReference<Throwable> thrownByNestedAccept = new AtomicReference<>();
        FakeSource goingLive = source(List.of(), true);
        goingLive.onHistoryDone = () -> thrownByNestedAccept.set(catchThrowable(() -> handover.accept("L1")));
        AtomicBoolean caughtUp = new AtomicBoolean();
        Thread goLive = new Thread(() -> caughtUp.set(handover.catchUp(goingLive)), "go-live");
        try {
            goLive.start();
            goLive.join(5_000);

            assertThat(goLive.isAlive()).as("the catch-up ended rather than waiting on itself").isFalse();
            assertThat(caughtUp).isTrue();
            assertThat(thrownByNestedAccept.get()).as("what the nested accept(..) threw")
                    .isInstanceOf(BlockingHandover.PreDispatchRefusalException.class)
                    .hasMessage(HandoverMessages.acceptedFromOwnDelivery(NOUN));
            assertThat(delivered).isEmpty();
        } finally {
            goLive.interrupt();
        }
    }

    // The refusal covers only an accept(..) that would wait. On a live handover the nested payload is delivered on the
    // same thread and nothing waits.
    @Test
    void accept_from_inside_a_live_delivery_with_no_catch_up_running_delivers_the_nested_payload() {
        List<String> delivered = new ArrayList<>();
        AtomicReference<BlockingHandover<String, String>> self = new AtomicReference<>();
        AtomicReference<Throwable> thrownByNestedAccept = new AtomicReference<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            if (payload.equals("L1")) {
                thrownByNestedAccept.set(catchThrowable(() -> self.get().accept("L2")));
            }
            delivered.add(payload);
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        self.set(handover);
        handover.catchUp(source(List.of(), true));

        handover.accept("L1");

        assertThat(thrownByNestedAccept.get()).as("what the nested accept(..) threw").isNull();
        assertThat(delivered).containsExactly("L2", "L1");
    }

    // A thread whose live delivery threw is no longer inside the handover's own work, so a later accept(..) from it
    // during a catch-up waits for the drain rather than being refused.
    @Test
    void a_live_delivery_that_throws_does_not_leave_its_thread_refused_by_a_later_waiting_accept() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch replaying = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            if (payload.equals("BOOM")) {
                throw new IllegalStateException("BOOM");
            }
            if (payload.equals("R1")) {
                replaying.countDown();
                awaitLatch(releaseReplay);
            }
            delivered.add(payload);
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handover.catchUp(source(List.of(), true));
        CountDownLatch boomThrown = new CountDownLatch(1);
        CountDownLatch acceptAgain = new CountDownLatch(1);
        AtomicReference<Throwable> thrownByBoom = new AtomicReference<>();
        AtomicReference<Throwable> thrownByLaterAccept = new AtomicReference<>();
        Thread listener = new Thread(() -> {
            thrownByBoom.set(catchThrowable(() -> handover.accept("BOOM")));
            boomThrown.countDown();
            awaitLatch(acceptAgain);
            thrownByLaterAccept.set(catchThrowable(() -> handover.accept("L1")));
        }, "listener");
        Thread replay = new Thread(() -> handover.catchUp(source(List.of("R1"), false)), "replay");
        try {
            listener.start();
            awaitLatch(boomThrown);
            replay.start();
            awaitLatch(replaying);
            acceptAgain.countDown();
            awaitWaiting(listener);

            releaseReplay.countDown();
            listener.join(5_000);
            replay.join(5_000);

            assertThat(listener.isAlive()).as("the listener ended").isFalse();
            assertThat(replay.isAlive()).as("the catch-up ended").isFalse();
            assertThat(thrownByBoom.get()).as("what the throwing live delivery threw").hasMessage("BOOM");
            assertThat(thrownByLaterAccept.get()).as("what the later accept(..) threw").isNull();
            assertThat(delivered).containsExactly("R1", "L1");
        } finally {
            releaseReplay.countDown();
            acceptAgain.countDown();
            listener.interrupt();
            replay.interrupt();
        }
    }

    @Test
    void two_waiting_accepts_of_the_same_payload_both_return_once_the_drain_has_applied_it_once() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch replaying = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        BlockingHandover<String, String> handover = handoverHoldingItsReplayAt("R1", delivered, replaying, releaseReplay, null);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> catchUp = executor.submit(() -> handover.catchUp(source(List.of("R1"), false)));
            awaitLatch(replaying);

            AtomicReference<Throwable> thrownByFirst = new AtomicReference<>();
            AtomicReference<Throwable> thrownBySecond = new AtomicReference<>();
            Thread first = new Thread(() -> thrownByFirst.set(catchThrowable(() -> handover.accept("L1"))), "first-delivery");
            Thread second = new Thread(() -> thrownBySecond.set(catchThrowable(() -> handover.accept("L1"))), "second-delivery");
            first.start();
            awaitWaiting(first);
            second.start();
            awaitWaiting(second);
            releaseReplay.countDown();

            assertThat(catchUp.get(5, TimeUnit.SECONDS)).isTrue();
            first.join(5_000);
            second.join(5_000);
            // The drain delivers the first copy and holds the second back as a repeat, which it answers applied once
            // the first copy is recorded delivered.
            assertThat(thrownByFirst.get()).as("what the first accept(..) threw").isNull();
            assertThat(thrownBySecond.get()).as("what the second accept(..) threw").isNull();
            assertThat(delivered).containsExactly("R1", "L1");
        } finally {
            releaseReplay.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void an_accept_interrupted_after_the_drain_took_its_payload_throws_and_the_payload_is_still_applied() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch deliveringL1 = new CountDownLatch(1);
        CountDownLatch releaseL1 = new CountDownLatch(1);
        BlockingHandover<String, String> handover = handoverHoldingItsReplayAt("L1", delivered, deliveringL1, releaseL1, null);
        AtomicReference<Throwable> thrownByAccept = new AtomicReference<>();
        Thread accepting = new Thread(() -> thrownByAccept.set(catchThrowable(() -> handover.accept("L1"))), "live-delivery");
        accepting.start();
        awaitWaiting(accepting);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> catchUp = executor.submit(() -> handover.catchUp(source(List.of("R1"), false)));
            awaitLatch(deliveringL1);

            accepting.interrupt();
            accepting.join(5_000);
            // The caller gave up without knowing whether the payload is applied, so it must not acknowledge it. The
            // redelivery that follows is de-duplicated.
            assertThat(accepting.isAlive()).as("accept(..) released by the interrupt while the drain applies its payload").isFalse();
            releaseL1.countDown();

            assertThat(catchUp.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(thrownByAccept.get()).as("what accept(..) threw once interrupted")
                    .isInstanceOf(BlockingHandover.PreDispatchRefusalException.class)
                    .hasMessage(HandoverMessages.interruptedBeforeApplied(NOUN));
            assertThat(delivered).containsExactly("R1", "L1");

            handover.accept("L1");
            assertThat(delivered).as("the redelivery of the payload the drain applied").containsExactly("R1", "L1");
        } finally {
            releaseL1.countDown();
            accepting.interrupt();
            executor.shutdownNow();
        }
    }

    @Test
    void a_waiting_accept_throws_the_failure_of_telling_the_source_its_payload_was_already_replayed_and_the_catch_up_goes_on() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch replaying = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        BlockingHandover<String, String> handover = handoverHoldingItsReplayAt("R2", delivered, replaying, releaseReplay, null);
        IllegalStateException listenerFailure = new IllegalStateException("listener failed");
        FakeSource failing = source(List.of("R1", "R2"), false);
        failing.onAlreadyDeliveredByReplay = () -> {
            throw listenerFailure;
        };
        AtomicReference<Throwable> thrownByAccept = new AtomicReference<>();
        Thread accepting = new Thread(() -> thrownByAccept.set(catchThrowable(() -> handover.accept("R1"))), "live-delivery");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> catchUp = executor.submit(() -> handover.catchUp(failing));
            awaitLatch(replaying);
            accepting.start();
            awaitWaiting(accepting);
            releaseReplay.countDown();

            assertThat(catchUp).as("the catch-up that only failed to tell the source").succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            accepting.join(5_000);
            assertThat(accepting.isAlive()).as("accept(..) released by the failure").isFalse();
            assertThat(thrownByAccept.get()).as("what accept(..) threw once the drain recorded nothing for its payload")
                    .isInstanceOf(BlockingHandover.PreDispatchRefusalException.class)
                    .hasMessage(HandoverMessages.alreadyAppliedNotRecorded(NOUN))
                    .hasCause(listenerFailure);
            assertThat(handover.isReadyForLiveDelivery()).as("live").isTrue();
            assertThat(handover.refusesPermanently()).as("refuses permanently").isFalse();
            assertThat(failing.markCaughtUpCallCount()).as("marker writes").isEqualTo(1);
            assertThat(delivered).as("delivered once each").containsExactly("R1", "R2");
        } finally {
            releaseReplay.countDown();
            accepting.interrupt();
            executor.shutdownNow();
        }
    }

    @Test
    void stopping_a_handover_while_its_catch_up_runs_leaves_a_waiting_accept_to_the_drain() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch replaying = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        BlockingHandover<String, String> handover = handoverHoldingItsReplayAt("R1", delivered, replaying, releaseReplay, null);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> catchUp = executor.submit(() -> handover.catchUp(source(List.of("R1"), false)));
            awaitLatch(replaying);
            AtomicReference<Throwable> thrownByAccept = new AtomicReference<>();
            Thread accepting = new Thread(() -> thrownByAccept.set(catchThrowable(() -> handover.accept("L1"))), "live-delivery");
            accepting.start();
            awaitWaiting(accepting);

            handover.stopIfNotCatchingUp();
            releaseReplay.countDown();

            assertThat(catchUp.get(5, TimeUnit.SECONDS)).isTrue();
            accepting.join(5_000);
            assertThat(thrownByAccept.get()).as("what accept(..) threw once the catch-up went live").isNull();
            assertThat(delivered).containsExactly("R1", "L1");
        } finally {
            releaseReplay.countDown();
            executor.shutdownNow();
        }
    }

    private static BlockingHandover<String, String> handoverHoldingItsReplayAt(String held, List<String> delivered, CountDownLatch reached,
                                                                               CountDownLatch release, RuntimeException failure) {
        return BlockingHandover.create(payload -> {
            if (payload.equals(held)) {
                reached.countDown();
                awaitLatch(release);
                if (failure != null) {
                    throw failure;
                }
            }
            delivered.add(payload);
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
    }

    // A waiting accept(..) parks in Object.wait()
    private static void awaitWaiting(Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (thread.getState() != Thread.State.WAITING) {
            if (!thread.isAlive()) {
                fail(thread.getName() + " ended without waiting");
            }
            if (System.nanoTime() > deadline) {
                fail(thread.getName() + " did not start waiting within 5 seconds, it is " + thread.getState());
            }
            Thread.onSpinWait();
        }
    }

    @Test
    void a_later_catch_up_revives_a_handover_a_previous_one_stopped() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String, String> handover = handover(delivered);
        FakeSource stopped = source(List.of("R1", "R2"), false);
        stopped.stopAfter(1);
        handover.catchUp(stopped);

        FakeSource retried = source(List.of("R1", "R2"), false);
        boolean caughtUp = handover.catchUp(retried);

        assertThat(caughtUp).isTrue();
        assertThat(retried.markCaughtUpCallCount()).isEqualTo(1);
        handover.accept("L1");
        assertThat(delivered).containsExactly("R1", "R1", "R2", "L1");
    }

    // A view that buffers during a replay discards that buffer when the replay stops, so a key the stopped replay left
    // behind would suppress the only copy of an event the read model never got. The key is forgotten instead, and a
    // view that wrote the event through receives it twice, which at-least-once delivery allows.
    @Test
    void a_payload_a_stopped_replay_delivered_is_delivered_again_once_the_handover_goes_live() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String, String> handover = handover(delivered);
        FakeSource stopped = source(List.of("R1", "R2"), false);
        stopped.stopAfter(1);
        handover.catchUp(stopped);

        FakeSource goLive = source(List.of(), true);
        handover.catchUp(goLive);
        handover.accept("R1");

        assertThat(delivered).containsExactly("R1", "R1");
        assertThat(stopped.alreadyDeliveredByReplay).isEmpty();
        assertThat(goLive.alreadyDeliveredByReplay).isEmpty();
    }

    // A feed's goLive() is a catch-up that replays nothing. A live copy of a payload the earlier, finished replay
    // delivered still reaches the source that replayed it, since that source is the one that can record it.
    @Test
    void a_catch_up_that_replays_nothing_leaves_the_earlier_replays_payloads_reaching_the_source_that_replayed_them() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String, String> handover = handover(delivered);
        FakeSource replayed = source(List.of("1"), false);
        handover.catchUp(replayed);

        FakeSource goLive = source(List.of(), true);
        handover.catchUp(goLive);
        handover.accept("1");

        assertThat(delivered).containsExactly("1");
        assertThat(replayed.alreadyDeliveredByReplay).containsExactly("1");
        assertThat(goLive.alreadyDeliveredByReplay).isEmpty();
    }

    // catchUp() on a handover that is already live, a feed's catchUp() after its goLive(), runs a replay while live
    // payloads keep arriving. A view that buffers during a replay throws that buffer away when the replay is stopped,
    // so a live payload handed to it mid-replay would be lost. The live payloads wait instead and are delivered once
    // the replay has ended, stopped or not, including one that shares its key with a payload the replay delivered.
    @Test
    void a_live_payload_accepted_while_a_replay_runs_on_a_live_handover_is_delivered_after_that_replay_is_stopped() {
        List<String> log = new ArrayList<>();
        AtomicReference<BlockingHandover<String, String>> self = new AtomicReference<>();
        AtomicBoolean offered = new AtomicBoolean();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            log.add(payload);
            if (payload.equals("R1") && offered.compareAndSet(false, true)) {
                self.get().acceptReportingDelivery("R1");
                self.get().acceptReportingDelivery("L1");
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        self.set(handover);
        handover.catchUp(source(List.of(), true));
        FakeSource replaying = source(List.of("R1", "R2"), false);
        replaying.stopAfter(1);
        replaying.onReplayAbandoned = () -> log.add("abandoned");

        boolean caughtUp = handover.catchUp(replaying);

        assertThat(caughtUp).isFalse();
        assertThat(log).containsExactly("R1", "abandoned", "R1", "L1");
    }

    // A stop ends the replay, not the live delivery the handover already had, so a payload fed after it is delivered.
    @Test
    void a_live_handover_keeps_delivering_after_a_replay_on_it_is_stopped() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String, String> handover = handover(delivered);
        handover.catchUp(source(List.of(), true));
        FakeSource replaying = source(List.of("R1", "R2"), false);
        replaying.stopAfter(1);
        handover.catchUp(replaying);

        assertThat(handover.acceptReportingDelivery("L1")).isTrue();
        assertThat(delivered).containsExactly("R1", "L1");
    }

    // A replay owns the keys it delivered. A second replay starts from none, so a live copy of an event only the first
    // replay delivered is delivered, rather than suppressed and reported to the second replay's source, which never
    // saw it.
    @Test
    void a_second_replay_starts_without_the_keys_the_first_replay_delivered() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String, String> handover = handover(delivered);
        FakeSource first = source(List.of("1"), false);
        handover.catchUp(first);
        FakeSource second = source(List.of(), false);
        handover.catchUp(second);

        handover.accept("1");

        assertThat(delivered).containsExactly("1", "1");
        assertThat(first.alreadyDeliveredByReplay).isEmpty();
        assertThat(second.alreadyDeliveredByReplay).isEmpty();
    }

    // A replay on a live handover buffers live payloads, and a payload taken into that buffer has already been reported
    // handled, so its caller has acknowledged it. When the replay fails, those payloads are delivered before the failure
    // is recorded, rather than left in a buffer nothing drains any more.
    @Test
    void a_live_payload_buffered_while_a_replay_on_a_live_handover_fails_is_still_delivered() {
        List<String> log = new ArrayList<>();
        AtomicReference<BlockingHandover<String, String>> self = new AtomicReference<>();
        AtomicBoolean offered = new AtomicBoolean();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            if (payload.equals("R2")) {
                throw new IllegalStateException("replay boom");
            }
            log.add(payload);
            if (payload.equals("R1") && offered.compareAndSet(false, true)) {
                assertThat(self.get().acceptReportingDelivery("L1")).isTrue();
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        self.set(handover);
        handover.catchUp(source(List.of(), true));

        Throwable failure = catchThrowable(() -> handover.catchUp(source(List.of("R1", "R2"), false)));

        assertThat(failure).hasMessage("replay boom");
        assertThat(log).containsExactly("R1", "L1");
        assertThat(handover.refusesPermanently()).isTrue();
    }

    // acceptIfLive refuses while a replay runs, even on a handover that is already live, so a caller that can
    // redeliver is told to try again rather than having its payload held until the replay ends.
    @Test
    void acceptIfLive_refuses_while_a_replay_runs_on_a_live_handover() {
        List<String> log = new ArrayList<>();
        List<Boolean> answers = new ArrayList<>();
        AtomicReference<BlockingHandover<String, String>> self = new AtomicReference<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            log.add(payload);
            if (payload.equals("R1")) {
                answers.add(self.get().acceptIfLive("L1"));
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        self.set(handover);
        handover.catchUp(source(List.of(), true));

        handover.catchUp(source(List.of("R1"), false));

        assertThat(answers).containsExactly(false);
        assertThat(log).containsExactly("R1");
    }

    // The drain tells the source that replayed a payload about the live copy it suppressed, and it does that after this
    // handover is live. A catch-up starting right then waits for those calls, so a replay never runs while the source
    // of the replay before it is still being told about its payloads.
    @Test
    void a_replay_waits_for_a_drain_callback_of_the_replay_before_it() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch callbackRunning = new CountDownLatch(1);
        CountDownLatch releaseCallback = new CountDownLatch(1);
        CountDownLatch secondReplayStarted = new CountDownLatch(1);
        BlockingHandover<String, String> handover = BlockingHandover.create(
                delivered::add, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        // Buffered before the replay runs and sharing its key, so the drain suppresses it and reports it to the source.
        handover.acceptReportingDelivery("1");
        FakeSource first = source(List.of("1"), false);
        first.onAlreadyDeliveredByReplay = () -> {
            callbackRunning.countDown();
            awaitLatch(releaseCallback);
        };
        FakeSource second = source(List.of("2"), false);
        second.onReplayStarted = secondReplayStarted::countDown;
        ExecutorService threads = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> firstCatchUp = threads.submit(() -> handover.catchUp(first));
            assertThat(callbackRunning.await(5, TimeUnit.SECONDS)).isTrue();
            Future<Boolean> secondCatchUp = threads.submit(() -> handover.catchUp(second));

            assertThat(secondReplayStarted.await(300, TimeUnit.MILLISECONDS)).isFalse();

            releaseCallback.countDown();
            assertThat(firstCatchUp.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(secondCatchUp.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(secondReplayStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(delivered).containsExactly("1", "2");
        } finally {
            releaseCallback.countDown();
            threads.shutdownNow();
        }
    }

    // A catch-up with nothing to replay claims this handover's live transition before it signals or delivers anything.
    // A replay starting while that runs waits for it, so the two never write to the view at the same time, and the
    // payloads this one delivers cannot end up in a batch that replay later throws away.
    @Test
    void a_catch_up_with_nothing_to_replay_keeps_a_replay_from_starting_while_it_goes_live() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch replayStarted = new CountDownLatch(1);
        AtomicBoolean replayStartedDuringTheTransition = new AtomicBoolean();
        BlockingHandover<String, String> handover = BlockingHandover.create(
                log::add, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handover.acceptReportingDelivery("L1");
        CountDownLatch replayCompleted = new CountDownLatch(1);
        FakeSource replaying = source(List.of("R1"), false);
        replaying.onReplayStarted = replayStarted::countDown;
        replaying.onReplayCompleted = replayCompleted::countDown;
        FakeSource goingLive = source(List.of(), true);
        ExecutorService threads = Executors.newSingleThreadExecutor();
        try {
            // Between the check that no replay is running and the drain itself, which is where a replay used to slip in.
            goingLive.onHistoryDone = () -> {
                threads.submit(() -> handover.catchUp(replaying));
                replayStartedDuringTheTransition.set(reachedWithin(replayStarted, 300));
            };

            handover.catchUp(goingLive);

            assertThat(replayStartedDuringTheTransition).isFalse();
            assertThat(replayStarted.await(5, TimeUnit.SECONDS)).isTrue();
            // The fold runs after the replay starts, so the log is read once the replay is through it.
            assertThat(replayCompleted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(log).containsExactly("L1", "R1");
        } finally {
            threads.shutdownNow();
        }
    }

    private static boolean reachedWithin(CountDownLatch latch, long millis) {
        try {
            return latch.await(millis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Test
    void replay_lifecycle_is_started_then_completed_before_the_buffer_drain_and_the_marker() {
        List<String> log = Collections.synchronizedList(new ArrayList<>());
        BlockingHandover<String, String> handover = BlockingHandover.create(log::add, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handover.acceptReportingDelivery("L1");
        FakeSource source = source(List.of("R1"), false);
        source.onReplayStarted = () -> log.add("started");
        source.onReplayCompleted = () -> log.add("completed");
        source.onMarkCaughtUp = () -> log.add("marker");

        handover.catchUp(source);

        assertThat(log).containsExactly("started", "R1", "completed", "L1", "marker");
        assertThat(source.replayAbandonedCallCount).isZero();
    }

    @Test
    void replay_lifecycle_methods_are_never_called_when_already_caught_up() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String, String> handover = handover(delivered);
        FakeSource source = source(List.of("R1"), true);

        handover.catchUp(source);

        assertThat(source.replayStartedCallCount).isZero();
        assertThat(source.replayCompletedCallCount).isZero();
        assertThat(source.replayAbandonedCallCount).isZero();
    }

    @Test
    void a_stopped_replay_calls_replay_abandoned_instead_of_replay_completed() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String, String> handover = handover(delivered);
        FakeSource source = source(List.of("R1", "R2", "R3"), false);
        source.stopAfter(2);

        handover.catchUp(source);

        assertThat(source.replayStartedCallCount).isEqualTo(1);
        assertThat(source.replayCompletedCallCount).isZero();
        assertThat(source.replayAbandonedCallCount).isEqualTo(1);
    }

    @Test
    void a_failed_replay_calls_replay_abandoned_before_the_failure_propagates() {
        BlockingHandover<String, String> handover = handover(new ArrayList<>());
        RuntimeException replayFailure = new RuntimeException("replay boom");
        FakeSource source = source(List.of(), false);
        source.replayFailure = replayFailure;

        Throwable thrown = catchThrowable(() -> handover.catchUp(source));

        assertThat(thrown).isSameAs(replayFailure);
        assertThat(source.replayAbandonedCallCount).isEqualTo(1);
        assertThat(source.replayCompletedCallCount).isZero();
    }

    // A source's replayAbandoned() throwing must not replace the failure that made the engine call it: the caller
    // still sees the original replay failure, not whatever replayAbandoned() itself threw.
    @Test
    void a_replay_abandoned_that_itself_throws_does_not_mask_the_failure_that_triggered_it() {
        BlockingHandover<String, String> handover = handover(new ArrayList<>());
        RuntimeException replayFailure = new RuntimeException("replay boom");
        FakeSource source = source(List.of(), false);
        source.replayFailure = replayFailure;
        source.onReplayAbandoned = () -> {
            throw new IllegalStateException("replayAbandoned boom");
        };

        Throwable thrown = catchThrowable(() -> handover.catchUp(source));

        assertThat(thrown).isSameAs(replayFailure);
    }

    // Once replayCompleted() has run successfully, a later failure (e.g. from the live buffer drain) must not call
    // replayAbandoned() again: that lifecycle already closed cleanly.
    @Test
    void a_failure_after_a_successful_replay_completed_does_not_call_replay_abandoned() {
        AtomicBoolean replayFinished = new AtomicBoolean(false);
        BlockingHandover<String, String> handover = BlockingHandover.create(
                payload -> {
                    if (replayFinished.get()) {
                        throw new RuntimeException("live drain boom");
                    }
                },
                payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handover.acceptReportingDelivery("L1");
        FakeSource source = source(List.of("R1"), false);
        source.onReplayCompleted = () -> replayFinished.set(true);

        Throwable thrown = catchThrowable(() -> handover.catchUp(source));

        assertThat(thrown).hasMessage("live drain boom");
        assertThat(source.replayCompletedCallCount).isEqualTo(1);
        assertThat(source.replayAbandonedCallCount).isZero();
    }

    /**
     * A delivery that throws part way through the drain stops the drain and fails the catch-up, so the payloads
     * behind it are never delivered by this handover. A caller recovers by fixing the cause and building a new
     * handover, which then delivers them, since nothing about them was recorded as delivered.
     * <p>
     * The engine also releases the de-dup reservations it took for those payloads. That is not observable from
     * here, and no test can make it observable, because a failed catch-up refuses every later payload for the life
     * of the handover, so nothing ever reaches the de-dup check again. It is done to keep the engine's own state
     * consistent rather than to change any answer it gives.
     */
    @Test
    void a_delivery_that_throws_mid_drain_stops_the_drain_and_leaves_the_rest_for_a_replacement() {
        List<String> delivered = new ArrayList<>();
        AtomicBoolean failNext = new AtomicBoolean(true);
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            if (payload.equals("L2") && failNext.getAndSet(false)) {
                throw new IllegalStateException("drain boom");
            }
            delivered.add(payload);
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);

        handover.acceptReportingDelivery("L1");
        handover.acceptReportingDelivery("L2");
        handover.acceptReportingDelivery("L3");

        FakeSource source = source(List.of(), false);
        Throwable thrown = catchThrowable(() -> handover.catchUp(source));
        assertThat(thrown).hasMessage("drain boom");
        assertThat(delivered).as("the drain stopped at the payload that threw").containsExactly("L1");

        // The handover refuses every later payload because the catch-up failed, which is the documented answer, so
        // a second handover stands in for the replacement a caller builds after fixing the cause. What matters is
        // that L3 is not silently skipped, which is what a leaked reservation would cause.
        List<String> redelivered = new ArrayList<>();
        BlockingHandover<String, String> replacement = handover(redelivered);
        replacement.catchUp(source(List.of(), true));
        replacement.accept("L3");

        assertThat(redelivered).as("L3 was never delivered, so offering it again delivers it").containsExactly("L3");
    }

    /**
     * The same drain, without a failure. Nothing is left reserved, so a repeat of a payload the drain did deliver
     * is still recognised as already delivered rather than delivered twice.
     */
    @Test
    void a_drain_that_completes_leaves_no_payload_reserved() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String, String> handover = handover(delivered);

        handover.acceptReportingDelivery("L1");
        handover.acceptReportingDelivery("L2");
        handover.catchUp(source(List.of(), false));

        handover.accept("L1");
        handover.accept("L2");

        assertThat(delivered).as("each payload was delivered once and the repeats were recognised")
                .containsExactly("L1", "L2");
    }

    /**
     * A replay that waited for a catch-up with nothing to replay must not inherit the live handover that catch-up
     * left behind. If it does, a live payload arriving during the replay is folded next to it and thrown away with
     * the replay's buffer when the replay stops, while its caller was told it was handled.
     */
    @Test
    void a_replay_that_waited_out_a_live_transition_buffers_the_payloads_that_arrive_while_it_runs() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        AtomicReference<BlockingHandover<String, String>> handoverRef = new AtomicReference<>();
        AtomicBoolean replayRunning = new AtomicBoolean();
        AtomicBoolean liveFoldedDuringTheReplay = new AtomicBoolean();
        CountDownLatch transitionReached = new CountDownLatch(1);
        CountDownLatch releaseTransition = new CountDownLatch(1);
        CountDownLatch replayStarted = new CountDownLatch(1);
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            if (payload.equals("L1") && replayRunning.get()) {
                liveFoldedDuringTheReplay.set(true);
            }
            log.add(payload);
            if (payload.equals("R1")) {
                // A live payload arriving mid-replay, which is the copy the abandoned replay would take with it.
                handoverRef.get().acceptReportingDelivery("L1");
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handoverRef.set(handover);
        FakeSource replaying = source(List.of("R1"), false);
        replaying.stopAfter(1);
        replaying.onReplayStarted = () -> {
            replayRunning.set(true);
            replayStarted.countDown();
        };
        replaying.onReplayCompleted = () -> replayRunning.set(false);
        replaying.onReplayAbandoned = () -> replayRunning.set(false);
        FakeSource goingLive = source(List.of(), true);
        goingLive.onHistoryDone = () -> {
            transitionReached.countDown();
            awaitLatch(releaseTransition);
        };
        ExecutorService threads = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> goLive = threads.submit(() -> handover.catchUp(goingLive));
            awaitLatch(transitionReached);
            Future<Boolean> replay = threads.submit(() -> handover.catchUp(replaying));
            // Parked in the wait, with the transition about to make the handover live behind its back.
            assertThat(reachedWithin(replayStarted, 300)).isFalse();

            releaseTransition.countDown();

            assertThat(goLive.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(replay.get(5, TimeUnit.SECONDS)).as("the replay was stopped").isFalse();
            assertThat(liveFoldedDuringTheReplay).isFalse();
            assertThat(log).as("the live payload was delivered when the replay ended, not during it")
                    .containsExactly("R1", "L1");
        } finally {
            threads.shutdownNow();
        }
    }

    /**
     * Two catch-ups with nothing to replay never drain at the same time. The second waits for the drain running, then
     * drains itself, and a replay waits for both, so the first to finish does not release it into a view the second is
     * still draining into.
     */
    @Test
    void a_replay_waits_for_every_live_transition_running_rather_than_the_first_to_finish() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch firstReached = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch secondReached = new CountDownLatch(1);
        CountDownLatch releaseSecond = new CountDownLatch(1);
        CountDownLatch replayStarted = new CountDownLatch(1);
        BlockingHandover<String, String> handover = BlockingHandover.create(
                log::add, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        AtomicInteger draining = new AtomicInteger();
        AtomicInteger mostDrainingAtOnce = new AtomicInteger();
        FakeSource first = source(List.of(), true);
        first.onHistoryDone = () -> {
            mostDrainingAtOnce.accumulateAndGet(draining.incrementAndGet(), Math::max);
            firstReached.countDown();
            awaitLatch(releaseFirst);
            draining.decrementAndGet();
        };
        FakeSource second = source(List.of(), true);
        second.onHistoryDone = () -> {
            mostDrainingAtOnce.accumulateAndGet(draining.incrementAndGet(), Math::max);
            secondReached.countDown();
            awaitLatch(releaseSecond);
            draining.decrementAndGet();
        };
        CountDownLatch replayCompleted = new CountDownLatch(1);
        FakeSource replaying = source(List.of("R1"), false);
        replaying.onReplayStarted = replayStarted::countDown;
        replaying.onReplayCompleted = replayCompleted::countDown;
        ExecutorService threads = Executors.newFixedThreadPool(3);
        try {
            threads.submit(() -> handover.catchUp(first));
            awaitLatch(firstReached);
            Future<Boolean> secondCatchUp = threads.submit(() -> handover.catchUp(second));
            assertThat(reachedWithin(secondReached, 300)).as("the second transition drains next to the first").isFalse();
            threads.submit(() -> handover.catchUp(replaying));
            assertThat(reachedWithin(replayStarted, 300)).isFalse();

            releaseFirst.countDown();

            assertThat(secondReached.await(5, TimeUnit.SECONDS)).as("the second transition drains once the first has").isTrue();
            assertThat(reachedWithin(replayStarted, 300))
                    .as("the second transition is still delivering").isFalse();

            releaseSecond.countDown();

            assertThat(secondCatchUp.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(replayStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(replayCompleted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(mostDrainingAtOnce).as("drains running at once").hasValue(1);
            assertThat(log).containsExactly("R1");
        } finally {
            threads.shutdownNow();
        }
    }

    /**
     * A catch-up that gives up on an interrupt puts back what it changed before it waited. What it must not put back
     * is the live flag another catch-up set while it waited, since that catch-up went live for its own caller and a
     * handover marked stopped here drops every payload that arrives after.
     */
    @Test
    void an_interrupted_wait_leaves_a_handover_that_went_live_meanwhile_still_live() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch historyDoneReached = new CountDownLatch(1);
        CountDownLatch releaseHistoryDone = new CountDownLatch(1);
        CountDownLatch bufferDelivering = new CountDownLatch(1);
        CountDownLatch releaseBufferDelivery = new CountDownLatch(1);
        CountDownLatch replayStarted = new CountDownLatch(1);
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            log.add(payload);
            if (payload.equals("L1")) {
                // Held inside the drain, so the handover is live while the replay is still waiting for the drain to end.
                bufferDelivering.countDown();
                awaitLatch(releaseBufferDelivery);
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handover.acceptReportingDelivery("L1");
        FakeSource goingLive = source(List.of(), true);
        goingLive.onHistoryDone = () -> {
            historyDoneReached.countDown();
            awaitLatch(releaseHistoryDone);
        };
        FakeSource replaying = source(List.of("R1"), false);
        replaying.onReplayStarted = replayStarted::countDown;
        AtomicBoolean caughtUp = new AtomicBoolean(true);
        AtomicBoolean interruptKept = new AtomicBoolean();
        Thread goLive = new Thread(() -> handover.catchUp(goingLive), "go-live");
        Thread replay = new Thread(() -> {
            caughtUp.set(handover.catchUp(replaying));
            interruptKept.set(Thread.currentThread().isInterrupted());
        }, "replay");

        goLive.start();
        awaitLatch(historyDoneReached);
        replay.start();
        assertThat(reachedWithin(replayStarted, 300)).isFalse();
        releaseHistoryDone.countDown();
        awaitLatch(bufferDelivering);
        replay.interrupt();
        replay.join(5_000);
        releaseBufferDelivery.countDown();
        goLive.join(5_000);

        assertThat(caughtUp).as("the interrupted catch-up gave up").isFalse();
        assertThat(interruptKept).as("the interrupt stayed on the thread").isTrue();
        handover.accept("L2");
        assertThat(log).as("the handover the other catch-up made live still delivers").containsExactly("L1", "L2");
    }

    /**
     * A catch-up holds live payloads back while it waits, and each of those was reported handled when it was taken
     * in. One that gives up on an interrupt has to deliver them, since a handover that is live again takes later
     * payloads straight past the buffer and nothing would come back for the ones sitting in it.
     */
    @Test
    void an_interrupted_wait_delivers_the_payloads_it_took_in_while_it_waited() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch deliveringL1 = new CountDownLatch(1);
        CountDownLatch releaseL1 = new CountDownLatch(1);
        CountDownLatch replayStarted = new CountDownLatch(1);
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            log.add(payload);
            if (payload.equals("L1")) {
                deliveringL1.countDown();
                awaitLatch(releaseL1);
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handover.catchUp(source(List.of(), true));
        FakeSource replaying = source(List.of("R1"), false);
        replaying.onReplayStarted = replayStarted::countDown;
        Thread delivering = new Thread(() -> handover.accept("L1"), "live-delivery");
        Thread replay = new Thread(() -> handover.catchUp(replaying), "replay");

        delivering.start();
        awaitLatch(deliveringL1);
        replay.start();
        // Waiting for the live delivery above, which is the window a payload lands in the buffer in.
        assertThat(reachedWithin(replayStarted, 300)).isFalse();
        handover.acceptReportingDelivery("L2");
        replay.interrupt();
        replay.join(5_000);
        releaseL1.countDown();
        delivering.join(5_000);
        handover.accept("L3");

        assertThat(log).as("the payload taken in while the catch-up waited was delivered")
                .containsExactly("L1", "L2", "L3");
    }

    @Test
    void an_interrupted_wait_answers_a_waiting_accept_once_it_has_delivered_the_payload_it_took_in() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch deliveringL1 = new CountDownLatch(1);
        CountDownLatch releaseL1 = new CountDownLatch(1);
        CountDownLatch replayStarted = new CountDownLatch(1);
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            log.add(payload);
            if (payload.equals("L1")) {
                deliveringL1.countDown();
                awaitLatch(releaseL1);
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handover.catchUp(source(List.of(), true));
        FakeSource replaying = source(List.of("R1"), false);
        replaying.onReplayStarted = replayStarted::countDown;
        Thread delivering = new Thread(() -> handover.accept("L1"), "live-delivery");
        Thread replay = new Thread(() -> handover.catchUp(replaying), "replay");
        AtomicReference<Throwable> thrownByAccept = new AtomicReference<>();
        Thread waiting = new Thread(() -> thrownByAccept.set(catchThrowable(() -> handover.accept("L2"))), "waiting-delivery");
        try {
            delivering.start();
            awaitLatch(deliveringL1);
            replay.start();
            // Waiting for the live delivery above, which is when a payload goes into the buffer.
            awaitWaiting(replay);
            assertThat(replayStarted.getCount()).as("replays not started").isOne();
            waiting.start();
            awaitWaiting(waiting);

            replay.interrupt();
            replay.join(5_000);
            waiting.join(5_000);

            assertThat(waiting.isAlive()).as("accept(..) answered by the drain after the interrupted wait").isFalse();
            assertThat(thrownByAccept.get()).as("what accept(..) threw").isNull();
            assertThat(log).containsExactly("L1", "L2");
        } finally {
            releaseL1.countDown();
            waiting.interrupt();
            delivering.join(5_000);
        }
    }

    /**
     * Two replays folding into the same view at once is the loss the wait before a replay exists to prevent. The
     * first to finish takes the handover live, and from then on live payloads reach the view next to the second
     * replay, which throws them away with its batch if it stops.
     */
    @Test
    void a_replay_waits_for_a_replay_already_running() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch foldingR1 = new CountDownLatch(1);
        CountDownLatch releaseR1 = new CountDownLatch(1);
        CountDownLatch secondReplayStarted = new CountDownLatch(1);
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            log.add(payload);
            if (payload.equals("R1")) {
                foldingR1.countDown();
                awaitLatch(releaseR1);
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        FakeSource first = source(List.of("R1"), false);
        FakeSource second = source(List.of("R2"), false);
        second.onReplayStarted = secondReplayStarted::countDown;
        Thread firstReplay = new Thread(() -> handover.catchUp(first), "first-replay");
        Thread secondReplay = new Thread(() -> handover.catchUp(second), "second-replay");

        firstReplay.start();
        awaitLatch(foldingR1);
        secondReplay.start();
        assertThat(reachedWithin(secondReplayStarted, 300)).isFalse();

        releaseR1.countDown();

        assertThat(secondReplayStarted.await(5, TimeUnit.SECONDS)).isTrue();
        firstReplay.join(5_000);
        secondReplay.join(5_000);
        assertThat(log).containsExactly("R1", "R2");
    }

    /**
     * The drain and the marker belong to the replay before them. A replay starting while the catch-up ahead of it is
     * still writing its marker would buffer live payloads next to that marker, and a marker that then fails would have
     * the catch block read and drain the second replay's state as its own.
     */
    @Test
    void a_replay_waits_for_the_catch_up_ahead_of_it_to_write_its_marker() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch writingMarker = new CountDownLatch(1);
        CountDownLatch releaseMarker = new CountDownLatch(1);
        CountDownLatch secondReplayStarted = new CountDownLatch(1);
        BlockingHandover<String, String> handover = BlockingHandover.create(
                log::add, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        FakeSource first = source(List.of("R1"), false);
        first.onMarkCaughtUp = () -> {
            writingMarker.countDown();
            awaitLatch(releaseMarker);
        };
        FakeSource second = source(List.of("R2"), false);
        second.onReplayStarted = secondReplayStarted::countDown;
        Thread firstCatchUp = new Thread(() -> handover.catchUp(first), "first-catch-up");
        Thread secondCatchUp = new Thread(() -> handover.catchUp(second), "second-catch-up");

        firstCatchUp.start();
        awaitLatch(writingMarker);
        secondCatchUp.start();
        assertThat(reachedWithin(secondReplayStarted, 300))
                .as("the second replay waits for the first catch-up's marker").isFalse();

        releaseMarker.countDown();

        assertThat(secondReplayStarted.await(5, TimeUnit.SECONDS)).isTrue();
        firstCatchUp.join(5_000);
        secondCatchUp.join(5_000);
        assertThat(log).containsExactly("R1", "R2");
    }

    /**
     * A catch-up that fails before it takes the replay turn owns none of the replay state. If its failure handling
     * cleared the running replay's flag anyway, a catch-up with nothing to replay would drain the buffer into the view
     * while that replay is still folding.
     */
    @Test
    void a_catch_up_failing_before_its_replay_leaves_the_running_replays_state_alone() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch foldingR1 = new CountDownLatch(1);
        CountDownLatch releaseR1 = new CountDownLatch(1);
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            log.add(payload);
            if (payload.equals("R1")) {
                foldingR1.countDown();
                awaitLatch(releaseR1);
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        BlockingHandover.Source<String> failingLookup = new BlockingHandover.Source<>() {
            @Override
            public boolean isAlreadyCaughtUp() {
                throw new IllegalStateException("marker lookup failed");
            }

            @Override
            public Stream<String> replay() {
                return Stream.empty();
            }

            @Override
            public void markCaughtUp() {
            }
        };
        Thread replay = new Thread(() -> handover.catchUp(source(List.of("R1", "R2"), false)), "replay");

        FutureTask<Boolean> nothingToReplay = new FutureTask<>(() -> handover.catchUp(source(List.of(), true)));
        Thread waiting = new Thread(nothingToReplay, "nothing to replay");

        try {
            replay.start();
            awaitLatch(foldingR1);
            handover.acceptReportingDelivery("L1");
            assertThatThrownBy(() -> handover.catchUp(failingLookup)).isInstanceOf(IllegalStateException.class);
            waiting.start();
            awaitWaiting(waiting);
            assertThat(nothingToReplay).as("the catch-up with nothing to replay while R1 is held").isNotDone();
            releaseR1.countDown();
            replay.join(5_000);

            assertThat(nothingToReplay.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(log).as("the buffered payload waited for the running replay").containsExactly("R1", "R2", "L1");
        } finally {
            releaseR1.countDown();
            waiting.join(5_000);
        }
    }

    // A feed's goLive() called while its first catchUp() replays. Returning true before that replay ended left
    // acceptIfLive(..) refusing after the caller was told the handover is live.
    @Test
    void a_catch_up_with_nothing_to_replay_returns_only_once_the_running_replay_has_ended() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch foldingR1 = new CountDownLatch(1);
        CountDownLatch releaseR1 = new CountDownLatch(1);
        BlockingHandover<String, String> handover = holdingAt("R1", log, foldingR1, releaseR1, null);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> replaying = executor.submit(() -> handover.catchUp(source(List.of("R1"), false)));
            awaitLatch(foldingR1);

            FutureTask<Boolean> nothingToReplay = new FutureTask<>(() -> handover.catchUp(source(List.of(), true)));
            Thread waiting = new Thread(nothingToReplay, "nothing to replay");
            waiting.start();
            awaitWaiting(waiting);

            assertThat(nothingToReplay).as("the catch-up with nothing to replay while R1 is held").isNotDone();
            releaseR1.countDown();
            assertThat(replaying.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(nothingToReplay.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(handover.acceptIfLive("L1")).isTrue();
            assertThat(log).containsExactly("R1", "L1");
        } finally {
            releaseR1.countDown();
            executor.shutdownNow();
        }
    }

    // When the replay it waited for fails, the handover refuses everything, so the catch-up waiting for it throws that
    // failure rather than returning true for a handover that is not live.
    @Test
    void a_catch_up_with_nothing_to_replay_throws_when_the_running_replay_it_waited_for_fails() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch foldingR1 = new CountDownLatch(1);
        CountDownLatch releaseR1 = new CountDownLatch(1);
        IllegalStateException foldFailure = new IllegalStateException("fold boom");
        BlockingHandover<String, String> handover = holdingAt("R1", log, foldingR1, releaseR1, foldFailure);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> replaying = executor.submit(() -> handover.catchUp(source(List.of("R1"), false)));
            awaitLatch(foldingR1);

            FutureTask<Boolean> nothingToReplay = new FutureTask<>(() -> handover.catchUp(source(List.of(), true)));
            Thread waiting = new Thread(nothingToReplay, "nothing to replay");
            waiting.start();
            awaitWaiting(waiting);

            assertThat(nothingToReplay).as("the catch-up with nothing to replay while R1 is held").isNotDone();
            releaseR1.countDown();
            assertThatThrownBy(() -> replaying.get(5, TimeUnit.SECONDS)).hasCauseReference(foldFailure);
            assertThatThrownBy(() -> nothingToReplay.get(5, TimeUnit.SECONDS)).cause()
                    .isInstanceOf(BlockingHandover.PreDispatchRefusalException.class)
                    .hasCauseReference(foldFailure);
            assertThat(handover.refusesPermanently()).isTrue();
        } finally {
            releaseR1.countDown();
            executor.shutdownNow();
        }
    }

    // The first catch-up already failed, so the failure recorded before the call is not the one the call waited for.
    // The catch-up with nothing to replay still throws the failure of the replay it waited for.
    @Test
    void a_catch_up_with_nothing_to_replay_throws_when_the_replay_it_waited_for_fails_on_a_handover_that_had_already_failed() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch foldingR1 = new CountDownLatch(1);
        CountDownLatch releaseR1 = new CountDownLatch(1);
        IllegalStateException firstFailure = new IllegalStateException("first boom");
        IllegalStateException secondFailure = new IllegalStateException("second boom");
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            if (payload.equals("R0")) {
                throw firstFailure;
            }
            if (payload.equals("R1")) {
                foldingR1.countDown();
                awaitLatch(releaseR1);
                throw secondFailure;
            }
            log.add(payload);
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            assertThatThrownBy(() -> handover.catchUp(source(List.of("R0"), false))).isSameAs(firstFailure);
            Future<Boolean> replaying = executor.submit(() -> handover.catchUp(source(List.of("R1"), false)));
            awaitLatch(foldingR1);

            FutureTask<Boolean> nothingToReplay = new FutureTask<>(() -> handover.catchUp(source(List.of(), true)));
            Thread waiting = new Thread(nothingToReplay, "nothing to replay");
            waiting.start();
            awaitWaiting(waiting);

            assertThat(nothingToReplay).as("the catch-up with nothing to replay while R1 is held").isNotDone();
            releaseR1.countDown();
            assertThatThrownBy(() -> replaying.get(5, TimeUnit.SECONDS)).hasCauseReference(secondFailure);
            assertThatThrownBy(() -> nothingToReplay.get(5, TimeUnit.SECONDS)).cause()
                    .isInstanceOf(BlockingHandover.PreDispatchRefusalException.class)
                    .hasCauseReference(secondFailure);
        } finally {
            releaseR1.countDown();
            executor.shutdownNow();
        }
    }

    // Each waiter refuses because the replay failed, and neither refusal is a failure of that replay, so the waiter that
    // reads the failure after the other one has refused still gets the replay's own failure, wrapped once. Which waiter
    // takes the lock first is up to the JVM, and only some orders read the failure after the other waiter refused, so
    // this repeats.
    @RepeatedTest(100)
    void two_catch_ups_with_nothing_to_replay_waiting_for_a_replay_that_fails_each_throw_that_replays_failure() throws Exception {
        CountDownLatch foldingR1 = new CountDownLatch(1);
        CountDownLatch releaseR1 = new CountDownLatch(1);
        IllegalStateException failure = new IllegalStateException("boom");
        BlockingHandover<String, String> handover = holdingAt("R1", new CopyOnWriteArrayList<>(), foldingR1, releaseR1, failure);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        List<Thread> waiters = new ArrayList<>();
        try {
            Future<Boolean> replaying = executor.submit(() -> handover.catchUp(source(List.of("R1"), false)));
            awaitLatch(foldingR1);
            List<FutureTask<Boolean>> nothingToReplay = List.of(new FutureTask<>(() -> handover.catchUp(source(List.of(), true))),
                    new FutureTask<>(() -> handover.catchUp(source(List.of(), true))));
            for (FutureTask<Boolean> waiter : nothingToReplay) {
                Thread waiting = new Thread(waiter, "nothing to replay " + waiters.size());
                waiters.add(waiting);
                waiting.start();
                awaitWaiting(waiting);
            }

            releaseR1.countDown();

            assertThatThrownBy(() -> replaying.get(5, TimeUnit.SECONDS)).hasCauseReference(failure);
            for (FutureTask<Boolean> waiter : nothingToReplay) {
                assertThatThrownBy(() -> waiter.get(5, TimeUnit.SECONDS)).cause()
                        .isInstanceOf(BlockingHandover.PreDispatchRefusalException.class)
                        .hasCauseReference(failure);
            }
        } finally {
            releaseR1.countDown();
            waiters.forEach(Thread::interrupt);
            executor.shutdownNow();
        }
    }

    // Interrupting the wait is how a caller gives up on a replay that never ends.
    @Test
    void an_interrupted_catch_up_with_nothing_to_replay_returns_false_and_keeps_the_interrupt() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch foldingR1 = new CountDownLatch(1);
        CountDownLatch releaseR1 = new CountDownLatch(1);
        BlockingHandover<String, String> handover = holdingAt("R1", log, foldingR1, releaseR1, null);
        AtomicReference<Boolean> answer = new AtomicReference<>();
        AtomicBoolean interruptKept = new AtomicBoolean();
        Thread replay = new Thread(() -> handover.catchUp(source(List.of("R1"), false)), "replay");
        Thread nothingToReplay = new Thread(() -> {
            answer.set(handover.catchUp(source(List.of(), true)));
            interruptKept.set(Thread.currentThread().isInterrupted());
        }, "nothing to replay");
        try {
            replay.start();
            awaitLatch(foldingR1);
            nothingToReplay.start();
            awaitWaiting(nothingToReplay);

            nothingToReplay.interrupt();
            nothingToReplay.join(5_000);

            assertThat(answer.get()).isFalse();
            assertThat(interruptKept.get()).isTrue();
        } finally {
            releaseR1.countDown();
            replay.join(5_000);
        }
    }

    // The interrupted call set the running replay to go live before it waited, and that stays set when it gives up,
    // so R1 still takes the handover live when it is stopped.
    @Test
    void a_replay_stopped_after_an_interrupted_catch_up_with_nothing_to_replay_still_goes_live() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch foldingR1 = new CountDownLatch(1);
        CountDownLatch releaseR1 = new CountDownLatch(1);
        BlockingHandover<String, String> handover = holdingAt("R1", log, foldingR1, releaseR1, null);
        FakeSource stoppedAfterR1 = source(List.of("R1", "R2"), false);
        stoppedAfterR1.stopAfter(1);
        AtomicReference<Boolean> replayAnswer = new AtomicReference<>();
        AtomicReference<Boolean> answer = new AtomicReference<>();
        Thread replay = new Thread(() -> replayAnswer.set(handover.catchUp(stoppedAfterR1)), "replay");
        Thread nothingToReplay = new Thread(() -> answer.set(handover.catchUp(source(List.of(), true))), "nothing to replay");
        try {
            replay.start();
            awaitLatch(foldingR1);
            nothingToReplay.start();
            awaitWaiting(nothingToReplay);
            nothingToReplay.interrupt();
            nothingToReplay.join(5_000);
            assertThat(answer.get()).isFalse();

            releaseR1.countDown();
            replay.join(5_000);

            assertThat(replayAnswer.get()).as("R1 was stopped before R2").isFalse();
            assertThat(handover.acceptIfLive("L1")).isTrue();
            assertThat(log).containsExactly("R1", "L1");
        } finally {
            releaseR1.countDown();
            replay.join(5_000);
        }
    }

    // A catch-up with nothing to replay waits for R1, which holds the turn when it is called, and not for R2 taking
    // the turn after it. Whichever of that call and R2 wakes first when R1 ends, the call returns while R2 is still
    // replaying and acceptIfLive(..) still refuses.
    @Test
    void a_catch_up_with_nothing_to_replay_does_not_wait_for_a_replay_that_takes_the_turn_after_the_one_it_waited_for() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch foldingR1 = new CountDownLatch(1);
        CountDownLatch releaseR1 = new CountDownLatch(1);
        CountDownLatch foldingR2 = new CountDownLatch(1);
        CountDownLatch releaseR2 = new CountDownLatch(1);
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            if (payload.equals("R1")) {
                foldingR1.countDown();
                awaitLatch(releaseR1);
            } else if (payload.equals("R2")) {
                foldingR2.countDown();
                awaitLatch(releaseR2);
            }
            log.add(payload);
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        FutureTask<Boolean> firstReplay = new FutureTask<>(() -> handover.catchUp(source(List.of("R1"), false)));
        FutureTask<Boolean> secondReplay = new FutureTask<>(() -> handover.catchUp(source(List.of("R2"), false)));
        FutureTask<Boolean> nothingToReplay = new FutureTask<>(() -> handover.catchUp(source(List.of(), true)));
        Thread first = new Thread(firstReplay, "first replay");
        Thread second = new Thread(secondReplay, "second replay");
        Thread waiting = new Thread(nothingToReplay, "nothing to replay");
        try {
            first.start();
            awaitLatch(foldingR1);
            second.start();
            awaitWaiting(second);
            waiting.start();
            awaitWaiting(waiting);

            releaseR1.countDown();

            assertThat(firstReplay.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(nothingToReplay.get(5, TimeUnit.SECONDS)).isTrue();
            awaitLatch(foldingR2);
            assertThat(handover.acceptIfLive("L1")).as("while R2 is replaying").isFalse();
            releaseR2.countDown();
            assertThat(secondReplay.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(handover.acceptIfLive("L2")).isTrue();
            assertThat(log).containsExactly("R1", "R2", "L2");
        } finally {
            releaseR1.countDown();
            releaseR2.countDown();
            first.join(5_000);
            second.join(5_000);
            waiting.join(5_000);
        }
    }

    // A catch-up with nothing to replay called from inside the running replay's own fold would wait for a fold that
    // cannot end until it returns, so it returns without waiting. This is the one case where it reports live early.
    @Test
    void a_catch_up_with_nothing_to_replay_from_inside_the_running_replays_fold_returns_without_waiting() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Boolean> answers = new CopyOnWriteArrayList<>();
        AtomicReference<BlockingHandover<String, String>> self = new AtomicReference<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            log.add(payload);
            if (payload.equals("R1")) {
                answers.add(self.get().catchUp(source(List.of(), true)));
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        self.set(handover);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> replaying = executor.submit(() -> handover.catchUp(source(List.of("R1"), false)));

            assertThat(replaying.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(answers).containsExactly(true);
            assertThat(handover.acceptIfLive("L1")).isTrue();
            assertThat(log).containsExactly("R1", "L1");
        } finally {
            executor.shutdownNow();
        }
    }

    // A catch-up with nothing to replay called from a replay callback would wait for the replay that is calling it, so
    // it returns without waiting, and that replay goes live once it ends.
    @Test
    void a_catch_up_with_nothing_to_replay_from_replay_started_returns_without_waiting() throws Exception {
        FakeSource replayed = source(List.of("R1"), false);
        assertThatACatchUpFromTheReplayReturnsWithoutWaiting(replayed, goLive -> replayed.onReplayStarted = goLive, true, "R1", "L1");
    }

    @Test
    void a_catch_up_with_nothing_to_replay_from_replay_completed_returns_without_waiting() throws Exception {
        FakeSource replayed = source(List.of("R1"), false);
        assertThatACatchUpFromTheReplayReturnsWithoutWaiting(replayed, goLive -> replayed.onReplayCompleted = goLive, true, "R1", "L1");
    }

    // The replay is stopped before its first fold, and goes live when it ends because the call from its callback asked
    // it to.
    @Test
    void a_catch_up_with_nothing_to_replay_from_replay_abandoned_returns_without_waiting() throws Exception {
        FakeSource replayed = source(List.of("R1"), false);
        replayed.stopAfter(0);
        assertThatACatchUpFromTheReplayReturnsWithoutWaiting(replayed, goLive -> replayed.onReplayAbandoned = goLive, false, "L1");
    }

    private static void assertThatACatchUpFromTheReplayReturnsWithoutWaiting(FakeSource replayed, Consumer<Runnable> calling,
                                                                            boolean replayReturns, String... folded) throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Boolean> answers = new CopyOnWriteArrayList<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(log::add, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        calling.accept(() -> answers.add(handover.catchUp(source(List.of(), true))));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> replaying = executor.submit(() -> handover.catchUp(replayed));

            assertThat(replaying).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(replayReturns);
            assertThat(answers).containsExactly(true);
            assertThat(handover.acceptIfLive("L1")).isTrue();
            assertThat(log).containsExactly(folded);
        } finally {
            executor.shutdownNow();
        }
    }

    // A catch-up with something to replay called from code this handover is running would wait for a replay that
    // cannot start before that code returns, so it returns true at once and the replay runs once that code returns.
    @Test
    void a_catch_up_with_something_to_replay_from_a_live_fold_replays_once_the_fold_returns() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Boolean> answers = new CopyOnWriteArrayList<>();
        FakeSource asked = source(List.of("R1"), false);
        AtomicReference<BlockingHandover<String, String>> self = new AtomicReference<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            log.add(payload);
            if (payload.equals("L1")) {
                answers.add(self.get().catchUp(asked));
                log.add("fold returns");
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        self.set(handover);
        handover.catchUp(source(List.of(), true));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> accepting = executor.submit(() -> handover.accept("L1"));

            assertThat(accepting).succeedsWithin(Duration.ofSeconds(5));
            assertThat(answers).containsExactly(true);
            assertThat(asked.replayCallCount).isEqualTo(1);
            assertThat(asked.markCaughtUpCallCount()).isEqualTo(1);
            handover.accept("R1");
            assertThat(handover.acceptIfLive("L2")).isTrue();
            assertThat(log).containsExactly("L1", "fold returns", "R1", "L2");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void a_catch_up_with_something_to_replay_from_a_replayed_fold_replays_once_the_catch_up_running_it_returns() throws Exception {
        FakeSource running = source(List.of("R1"), false);
        assertThatACatchUpFromOwnCodeReplaysOnceThatCodeReturns(running, (onR1, ask) -> onR1.set(ask), true, "R1", "asked", "R2");
    }

    @Test
    void a_catch_up_with_something_to_replay_from_replay_started_replays_once_the_catch_up_running_it_returns() throws Exception {
        FakeSource running = source(List.of("R1"), false);
        assertThatACatchUpFromOwnCodeReplaysOnceThatCodeReturns(running, (onR1, ask) -> running.onReplayStarted = ask, true, "asked", "R1", "R2");
    }

    @Test
    void a_catch_up_with_something_to_replay_from_replay_completed_replays_once_the_catch_up_running_it_returns() throws Exception {
        FakeSource running = source(List.of("R1"), false);
        assertThatACatchUpFromOwnCodeReplaysOnceThatCodeReturns(running, (onR1, ask) -> running.onReplayCompleted = ask, true, "R1", "asked", "R2");
    }

    @Test
    void a_catch_up_with_something_to_replay_from_history_done_replays_once_the_catch_up_running_it_returns() throws Exception {
        FakeSource running = source(List.of("R1"), false);
        assertThatACatchUpFromOwnCodeReplaysOnceThatCodeReturns(running, (onR1, ask) -> running.onHistoryDone = ask, true, "R1", "asked", "R2");
    }

    @Test
    void a_catch_up_with_something_to_replay_from_history_done_of_a_catch_up_with_nothing_to_replay_replays_once_it_returns() throws Exception {
        FakeSource running = source(List.of(), true);
        assertThatACatchUpFromOwnCodeReplaysOnceThatCodeReturns(running, (onR1, ask) -> running.onHistoryDone = ask, true, "asked", "R2");
    }

    @Test
    void a_catch_up_with_something_to_replay_from_mark_caught_up_replays_once_the_catch_up_running_it_returns() throws Exception {
        FakeSource running = source(List.of("R1"), false);
        assertThatACatchUpFromOwnCodeReplaysOnceThatCodeReturns(running, (onR1, ask) -> running.onMarkCaughtUp = ask, true, "R1", "asked", "R2");
    }

    @Test
    void a_catch_up_with_something_to_replay_from_replay_abandoned_replays_once_the_catch_up_running_it_returns() throws Exception {
        FakeSource running = source(List.of("R1"), false);
        running.stopAfter(0);
        assertThatACatchUpFromOwnCodeReplaysOnceThatCodeReturns(running, (onR1, ask) -> running.onReplayAbandoned = ask, false, "asked", "R2");
    }

    private static void assertThatACatchUpFromOwnCodeReplaysOnceThatCodeReturns(
            FakeSource running, BiConsumer<AtomicReference<Runnable>, Runnable> calling, boolean runningReturns, String... folded) throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Boolean> answers = new CopyOnWriteArrayList<>();
        AtomicReference<Runnable> onR1 = new AtomicReference<>(() -> {
        });
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            log.add(payload);
            if (payload.equals("R1")) {
                onR1.get().run();
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        FakeSource asked = source(List.of("R2"), false);
        calling.accept(onR1, () -> {
            answers.add(handover.catchUp(asked));
            log.add("asked");
        });
        handover.catchUp(source(List.of(), true));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> catchingUp = executor.submit(() -> handover.catchUp(running));

            assertThat(catchingUp).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(runningReturns);
            assertThat(answers).containsExactly(true);
            assertThat(asked.replayCallCount).isEqualTo(1);
            assertThat(asked.markCaughtUpCallCount()).isEqualTo(1);
            assertThat(handover.acceptIfLive("L1")).isTrue();
            assertThat(log).containsExactly(concat(folded, "L1"));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void a_catch_up_with_something_to_replay_from_already_delivered_by_replay_replays_once_that_callback_returns() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Boolean> answers = new CopyOnWriteArrayList<>();
        BlockingHandover<String, String> handover = handover(log);
        FakeSource replayed = source(List.of("R1"), false);
        FakeSource asked = source(List.of("R2"), false);
        handover.catchUp(source(List.of(), true));
        handover.catchUp(replayed);
        replayed.onAlreadyDeliveredByReplay = () -> {
            answers.add(handover.catchUp(asked));
            log.add("asked");
        };
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> accepting = executor.submit(() -> handover.accept("R1"));

            assertThat(accepting).succeedsWithin(Duration.ofSeconds(5));
            assertThat(answers).containsExactly(true);
            assertThat(replayed.alreadyDeliveredByReplay).containsExactly("R1");
            assertThat(asked.replayCallCount).isEqualTo(1);
            assertThat(log).containsExactly("R1", "asked", "R2");
        } finally {
            executor.shutdownNow();
        }
    }

    // Asking twice before the replay starts asks for one catch-up, and that replay reads everything the second one would.
    @Test
    void two_catch_ups_asked_for_by_own_code_before_the_replay_starts_replay_once() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Boolean> answers = new CopyOnWriteArrayList<>();
        BlockingHandover<String, String> handover = handover(log);
        FakeSource running = source(List.of("R1"), false);
        FakeSource first = source(List.of("R1", "R2"), false);
        FakeSource second = source(List.of("R1", "R2"), false);
        running.onReplayStarted = () -> {
            answers.add(handover.catchUp(first));
            answers.add(handover.catchUp(second));
        };
        handover.catchUp(source(List.of(), true));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> catchingUp = executor.submit(() -> handover.catchUp(running));

            assertThat(catchingUp).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(answers).containsExactly(true, true);
            assertThat(first.replayCallCount + second.replayCallCount).isEqualTo(1);
            assertThat(log).containsExactly("R1", "R1", "R2");
        } finally {
            executor.shutdownNow();
        }
    }

    // The replay a replay's own code asked for asks again, and that one runs too once the first returns.
    @Test
    void a_catch_up_asked_for_by_the_replay_own_code_asked_for_also_replays() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        BlockingHandover<String, String> handover = handover(log);
        FakeSource running = source(List.of("R1"), false);
        FakeSource asked = source(List.of("R2"), false);
        FakeSource askedByAsked = source(List.of("R3"), false);
        running.onReplayStarted = () -> handover.catchUp(asked);
        asked.onReplayStarted = () -> handover.catchUp(askedByAsked);
        handover.catchUp(source(List.of(), true));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> catchingUp = executor.submit(() -> handover.catchUp(running));

            assertThat(catchingUp).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(log).containsExactly("R1", "R2", "R3");
        } finally {
            executor.shutdownNow();
        }
    }

    // A caller that waits for a catch-up that then fails is refused rather than replaying into a view it was told to
    // stop using, and one own code asked for is refused the same way.
    @Test
    void a_catch_up_asked_for_by_own_code_does_not_replay_when_the_catch_up_running_that_code_fails() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        BlockingHandover<String, String> handover = handover(log);
        FakeSource running = source(List.of("R1"), false);
        running.replayFailure = new RuntimeException("replay failed");
        FakeSource asked = source(List.of("R2"), false);
        running.onReplayStarted = () -> handover.catchUp(asked);
        handover.catchUp(source(List.of(), true));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> catchingUp = executor.submit(() -> handover.catchUp(running));

            assertThat(catchingUp).failsWithin(Duration.ofSeconds(5)).withThrowableThat().havingCause().withMessage("replay failed");
            assertThat(asked.replayCallCount).isZero();
            assertThat(log).isEmpty();
        } finally {
            executor.shutdownNow();
        }
    }

    // The caller of catchUp(..) is told a catch-up it ran failed, including one its code asked for, so startup code
    // does not take a view that stopped halfway for caught up.
    @Test
    void a_catch_up_that_runs_a_failing_replay_its_own_code_asked_for_throws_that_failure() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        BlockingHandover<String, String> handover = handover(log);
        FakeSource running = source(List.of("R1"), false);
        FakeSource asked = source(List.of("R2"), false);
        asked.replayFailure = new RuntimeException("asked replay failed");
        running.onReplayStarted = () -> handover.catchUp(asked);
        handover.catchUp(source(List.of(), true));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> catchingUp = executor.submit(() -> handover.catchUp(running));

            assertThat(catchingUp).failsWithin(Duration.ofSeconds(5)).withThrowableThat().havingCause().withMessage("asked replay failed");
            assertThat(asked.replayCallCount).isEqualTo(1);
            assertThat(handover.refusesPermanently()).isTrue();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void an_accept_that_runs_a_failing_replay_its_fold_asked_for_returns_normally_and_the_handover_refuses_after() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        FakeSource asked = source(List.of("R1"), false);
        asked.replayFailure = new RuntimeException("asked replay failed");
        AtomicReference<BlockingHandover<String, String>> self = new AtomicReference<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            log.add(payload);
            if (payload.equals("L1")) {
                self.get().catchUp(asked);
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        self.set(handover);
        handover.catchUp(source(List.of(), true));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> accepting = executor.submit(() -> handover.accept("L1"));

            assertThat(accepting).succeedsWithin(Duration.ofSeconds(5));
            assertThat(asked.replayCallCount).isEqualTo(1);
            assertThat(handover.refusesPermanently()).isTrue();
            assertThat(log).containsExactly("L1");
        } finally {
            executor.shutdownNow();
        }
    }

    // The first failure stays the recorded one, so only a record of the most recent failure can tell that another
    // catch-up failed after the ask on a handover that had already failed.
    @Test
    void a_catch_up_asked_for_by_own_code_does_not_replay_when_the_catch_up_running_that_code_fails_on_a_handover_that_already_failed() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        BlockingHandover<String, String> handover = handover(log);
        FakeSource firstFailure = source(List.of("R0"), false);
        firstFailure.replayFailure = new RuntimeException("first replay failed");
        assertThatThrownBy(() -> handover.catchUp(firstFailure)).hasMessage("first replay failed");
        FakeSource running = source(List.of("R1"), false);
        running.replayFailure = new RuntimeException("second replay failed");
        FakeSource asked = source(List.of("R2"), false);
        running.onReplayStarted = () -> handover.catchUp(asked);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> catchingUp = executor.submit(() -> handover.catchUp(running));

            assertThat(catchingUp).failsWithin(Duration.ofSeconds(5)).withThrowableThat().havingCause().withMessage("second replay failed");
            assertThat(asked.replayCallCount).isZero();
            assertThat(log).isEmpty();
        } finally {
            executor.shutdownNow();
        }
    }

    // The asked replay runs its source's isAlreadyCaughtUp() before it takes its turn, and an ask from there is for
    // the replay about to start, which replays from the newer source.
    @Test
    void an_ask_after_the_asked_replay_is_taken_but_before_it_starts_runs_no_second_replay() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        BlockingHandover<String, String> handover = handover(log);
        FakeSource running = source(List.of("R1"), false);
        FakeSource asked = source(List.of("R2"), false);
        FakeSource askedAgain = source(List.of("R3"), false);
        running.onReplayStarted = () -> {
            handover.catchUp(asked);
            asked.onIsAlreadyCaughtUp = () -> handover.catchUp(askedAgain);
        };
        handover.catchUp(source(List.of(), true));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> catchingUp = executor.submit(() -> handover.catchUp(running));

            assertThat(catchingUp).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(asked.replayCallCount).isZero();
            assertThat(askedAgain.replayCallCount).isEqualTo(1);
            assertThat(log).containsExactly("R1", "R3");
        } finally {
            executor.shutdownNow();
        }
    }

    // A catch-up fails between two asks that join one replay. The ask before the failure is refused and the one after
    // it is not, the same as when each had asked for a replay of its own.
    @Test
    void an_ask_that_joins_a_pending_one_after_a_catch_up_failed_between_them_still_replays() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        FakeSource askedBeforeFailure = source(List.of("R1"), false);
        FakeSource askedAfterFailure = source(List.of("R2"), false);
        CountDownLatch firstAsked = new CountDownLatch(1);
        CountDownLatch secondFolding = new CountDownLatch(1);
        CountDownLatch failed = new CountDownLatch(1);
        CountDownLatch secondAsked = new CountDownLatch(1);
        AtomicReference<BlockingHandover<String, String>> handoverRef = new AtomicReference<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            log.add(payload);
            if (payload.equals("L1")) {
                handoverRef.get().catchUp(askedBeforeFailure);
                firstAsked.countDown();
                awaitOrFail(secondAsked);
            } else if (payload.equals("L2")) {
                secondFolding.countDown();
                awaitOrFail(failed);
                handoverRef.get().catchUp(askedAfterFailure);
                secondAsked.countDown();
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handoverRef.set(handover);
        handover.catchUp(source(List.of(), true));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> first = executor.submit(() -> handover.accept("L1"));
            Future<?> second = executor.submit(() -> handover.accept("L2"));
            awaitOrFail(firstAsked);
            awaitOrFail(secondFolding);
            FakeSource failing = source(List.of(), false);
            failing.onIsAlreadyCaughtUp = () -> {
                throw new IllegalStateException("marker lookup failed");
            };
            assertThatThrownBy(() -> handover.catchUp(failing)).hasMessage("marker lookup failed");
            failed.countDown();

            assertThat(first).succeedsWithin(Duration.ofSeconds(5));
            assertThat(second).succeedsWithin(Duration.ofSeconds(5));
            assertThat(askedBeforeFailure.replayCallCount).isZero();
            assertThat(askedAfterFailure.replayCallCount).isEqualTo(1);
            assertThat(log).containsExactlyInAnyOrder("L1", "L2", "R2");
        } finally {
            executor.shutdownNow();
        }
    }

    // The asked catch-up finds the history caught up by the time it runs, and an ask made while it looked still gets
    // a replay after it, since that lookup answered only the asks made before it.
    @Test
    void an_ask_made_while_the_asked_catch_up_finds_the_history_caught_up_still_replays() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        BlockingHandover<String, String> handover = handover(log);
        FakeSource running = source(List.of("R1"), false);
        FakeSource askedAgain = source(List.of("R3"), false);
        AtomicInteger lookups = new AtomicInteger();
        BlockingHandover.Source<String> caughtUpWhenRun = new BlockingHandover.Source<>() {
            @Override
            public boolean isAlreadyCaughtUp() {
                if (lookups.incrementAndGet() == 1) {
                    return false;
                }
                handover.catchUp(askedAgain);
                return true;
            }

            @Override
            public Stream<String> replay() {
                return Stream.of("R2");
            }

            @Override
            public void markCaughtUp() {
            }
        };
        running.onReplayStarted = () -> handover.catchUp(caughtUpWhenRun);
        handover.catchUp(source(List.of(), true));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> catchingUp = executor.submit(() -> handover.catchUp(running));

            assertThat(catchingUp).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(lookups).hasValue(2);
            assertThat(askedAgain.replayCallCount).isEqualTo(1);
            assertThat(log).containsExactly("R1", "R3");
        } finally {
            executor.shutdownNow();
        }
    }

    // The asked catch-up finds the history caught up while another thread's replay runs, waits for that replay, and
    // that replay fails. The ask is refused without a report, so the catch-up whose own replay asked for it and
    // succeeded returns normally, and only the failed one throws.
    @Test
    void a_catch_up_whose_asked_catch_up_is_refused_by_another_threads_failed_replay_returns_normally() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch otherReplayStarted = new CountDownLatch(1);
        AtomicBoolean lookedUp = new AtomicBoolean();
        AtomicReference<Thread> askingThread = new AtomicReference<>();
        AtomicReference<BlockingHandover<String, String>> handoverRef = new AtomicReference<>();
        AtomicInteger lookups = new AtomicInteger();
        AtomicInteger historyDone = new AtomicInteger();
        BlockingHandover.Source<String> caughtUpWhenRun = new BlockingHandover.Source<>() {
            @Override
            public boolean isAlreadyCaughtUp() {
                if (lookups.incrementAndGet() == 1) {
                    return false;
                }
                awaitOrFail(otherReplayStarted);
                lookedUp.set(true);
                return true;
            }

            @Override
            public Stream<String> replay() {
                return Stream.of("R2");
            }

            @Override
            public void markCaughtUp() {
            }

            @Override
            public void historyDone() {
                historyDone.incrementAndGet();
            }
        };
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            log.add(payload);
            if (payload.equals("R0")) {
                handoverRef.get().catchUp(caughtUpWhenRun);
            } else if (payload.equals("R1")) {
                awaitWaiting(askingThread.get(), lookedUp);
                throw new IllegalStateException("other replay failed");
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handoverRef.set(handover);
        FakeSource asking = source(List.of("R0"), false);
        FakeSource other = source(List.of("R1"), false);
        other.onReplayStarted = otherReplayStarted::countDown;
        CountDownLatch askingReplayStarted = new CountDownLatch(1);
        asking.onReplayStarted = askingReplayStarted::countDown;
        FutureTask<Boolean> askingCatchUp = new FutureTask<>(() -> handover.catchUp(asking));
        // Asks for its catch-up once the asking replay holds the turn, so it replays after that one.
        FutureTask<Boolean> otherCatchUp = new FutureTask<>(() -> {
            awaitOrFail(askingReplayStarted);
            return handover.catchUp(other);
        });
        Thread asker = new Thread(askingCatchUp, "asking");
        askingThread.set(asker);
        Thread otherThread = new Thread(otherCatchUp, "other");
        asker.start();
        otherThread.start();

        try {
            assertThat(otherCatchUp).failsWithin(Duration.ofSeconds(5))
                    .withThrowableThat().havingCause().withMessage("other replay failed");
            assertThat(askingCatchUp).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(lookups).hasValue(2);
            assertThat(historyDone).hasValue(0);
            assertThat(log).containsExactly("R0", "R1");
        } finally {
            asker.interrupt();
            otherThread.interrupt();
        }
    }

    // The thread that runs a catch-up two threads asked for is interrupted before the replay starts. It warns and
    // gives up its own ask, and the ask of the other thread is run by the next call that leaves this handover, here
    // the live delivery of that other thread.
    @Test
    void an_interrupt_of_the_thread_running_an_asked_catch_up_leaves_the_ask_of_another_thread_for_the_next_call() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        FakeSource askedFirst = source(List.of("R1"), false);
        FakeSource askedByOther = source(List.of("R2"), false);
        CountDownLatch firstAsked = new CountDownLatch(1);
        CountDownLatch otherAsked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger lookups = new AtomicInteger();
        askedByOther.onIsAlreadyCaughtUp = () -> {
            if (lookups.incrementAndGet() == 2) {
                // The run of the first thread looks up the latest ask, then waits for the other thread's delivery.
                Thread.currentThread().interrupt();
            }
        };
        AtomicReference<BlockingHandover<String, String>> handoverRef = new AtomicReference<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            log.add(payload);
            if (payload.equals("L1")) {
                handoverRef.get().catchUp(askedFirst);
                firstAsked.countDown();
                awaitOrFail(otherAsked);
            } else if (payload.equals("L2")) {
                awaitOrFail(firstAsked);
                handoverRef.get().catchUp(askedByOther);
                otherAsked.countDown();
                awaitOrFail(release);
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handoverRef.set(handover);
        handover.catchUp(source(List.of(), true));
        AtomicBoolean interruptedAfter = new AtomicBoolean();
        FutureTask<Void> first = new FutureTask<>(() -> {
            handover.accept("L1");
            interruptedAfter.set(Thread.currentThread().isInterrupted());
        }, null);
        FutureTask<Void> second = new FutureTask<>(() -> handover.accept("L2"), null);
        Thread firstThread = new Thread(first, "first");
        Thread secondThread = new Thread(second, "second");
        firstThread.start();
        secondThread.start();

        try {
            assertThat(first).succeedsWithin(Duration.ofSeconds(5));
            assertThat(interruptedAfter).isTrue();
            release.countDown();
            assertThat(second).succeedsWithin(Duration.ofSeconds(5));
            assertThat(askedByOther.replayCallCount).isEqualTo(1);
            assertThat(askedFirst.replayCallCount).isZero();
            assertThat(log).containsExactlyInAnyOrder("L1", "L2", "R2");
        } finally {
            release.countDown();
            firstThread.interrupt();
            secondThread.interrupt();
        }
    }

    // The ask of the second thread stays when the first thread's run is interrupted. The second thread is then
    // interrupted too, and keeps delivering while its view asks each time. Nothing is running, so none of its runs
    // has to wait, and the interrupt does not keep it from replaying.
    @Test
    void an_interrupted_thread_runs_the_asked_catch_ups_it_can_run_without_waiting() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        FakeSource askedFirst = source(List.of("R1"), false);
        FakeSource askedBySecond = source(List.of("R2"), false);
        Map<String, FakeSource> askedLater = Map.of(
                "L3", source(List.of("R3"), false),
                "L4", source(List.of("R4"), false),
                "L5", source(List.of("R5"), false));
        CountDownLatch firstAsked = new CountDownLatch(1);
        CountDownLatch secondAsked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger lookups = new AtomicInteger();
        askedBySecond.onIsAlreadyCaughtUp = () -> {
            if (lookups.incrementAndGet() == 2) {
                // The run of the first thread looks up the latest ask, then waits for the second thread's delivery.
                Thread.currentThread().interrupt();
            }
        };
        AtomicReference<BlockingHandover<String, String>> handoverRef = new AtomicReference<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            log.add(payload);
            if (payload.equals("L1")) {
                handoverRef.get().catchUp(askedFirst);
                firstAsked.countDown();
                awaitOrFail(secondAsked);
            } else if (payload.equals("L2")) {
                awaitOrFail(firstAsked);
                handoverRef.get().catchUp(askedBySecond);
                secondAsked.countDown();
                awaitOrFail(release);
                Thread.currentThread().interrupt();
            } else if (askedLater.containsKey(payload)) {
                handoverRef.get().catchUp(askedLater.get(payload));
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handoverRef.set(handover);
        handover.catchUp(source(List.of(), true));
        AtomicBoolean interruptedAfter = new AtomicBoolean();
        FutureTask<Void> first = new FutureTask<>(() -> handover.accept("L1"), null);
        FutureTask<Void> second = new FutureTask<>(() -> {
            handover.accept("L2");
            handover.accept("L3");
            handover.accept("L4");
            handover.accept("L5");
            interruptedAfter.set(Thread.currentThread().isInterrupted());
        }, null);
        Thread firstThread = new Thread(first, "first");
        Thread secondThread = new Thread(second, "second");
        firstThread.start();
        secondThread.start();

        try {
            assertThat(first).succeedsWithin(Duration.ofSeconds(5));
            release.countDown();
            assertThat(second).succeedsWithin(Duration.ofSeconds(5));
            assertThat(interruptedAfter).isTrue();
            assertThat(askedFirst.replayCallCount).isZero();
            assertThat(askedBySecond.replayCallCount).isEqualTo(1);
            assertThat(askedLater.values()).allSatisfy(asked -> assertThat(asked.replayCallCount).isEqualTo(1));
            assertThat(log.subList(0, 2)).containsExactlyInAnyOrder("L1", "L2");
            assertThat(log.subList(2, log.size())).containsExactly("R2", "L3", "R3", "L4", "R4", "L5", "R5");
        } finally {
            release.countDown();
            firstThread.interrupt();
            secondThread.interrupt();
        }
    }

    // A catch-up fails after the view asked and before the asked run finds the history caught up. The ask is refused
    // the same as one whose run waits for that catch-up, so the run does not tell the source its history was read.
    @Test
    void an_asked_catch_up_that_finds_the_history_caught_up_after_another_catch_up_failed_is_refused() throws Exception {
        ExecutorService otherThread = Executors.newSingleThreadExecutor();
        AtomicReference<BlockingHandover<String, String>> handoverRef = new AtomicReference<>();
        FakeSource failing = source(List.of("R1"), false);
        failing.replayFailure = new IllegalStateException("other catch-up failed");
        AtomicInteger lookups = new AtomicInteger();
        AtomicInteger historyDone = new AtomicInteger();
        BlockingHandover.Source<String> caughtUpAfterTheFailure = new BlockingHandover.Source<>() {
            @Override
            public boolean isAlreadyCaughtUp() {
                if (lookups.incrementAndGet() == 1) {
                    return false;
                }
                Future<Boolean> failed = otherThread.submit(() -> handoverRef.get().catchUp(failing));
                assertThat(failed).failsWithin(Duration.ofSeconds(5));
                return true;
            }

            @Override
            public Stream<String> replay() {
                return Stream.of("R2");
            }

            @Override
            public void markCaughtUp() {
            }

            @Override
            public void historyDone() {
                historyDone.incrementAndGet();
            }
        };
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            if (payload.equals("L1")) {
                handoverRef.get().catchUp(caughtUpAfterTheFailure);
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handoverRef.set(handover);
        handover.catchUp(source(List.of(), true));

        try {
            handover.accept("L1");

            assertThat(lookups).hasValue(2);
            assertThat(failing.replayCallCount).isEqualTo(1);
            assertThat(historyDone).hasValue(0);
            assertThat(handover.isReadyForLiveDelivery()).isFalse();
        } finally {
            otherThread.shutdownNow();
        }
    }

    // The first thread's run is interrupted, gives up its own ask and keeps that of the second thread. A third thread
    // that is interrupted too delivers next and takes that ask over. Its marker lookup fails on the interrupted thread,
    // which puts the ask back for the second thread rather than failing the handover.
    @Test
    void an_interrupted_thread_that_takes_over_a_catch_up_another_thread_asked_for_does_not_fail_the_handover() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        FakeSource askedFirst = source(List.of("R1"), false);
        FakeSource askedBySecond = source(List.of("R2"), false);
        CountDownLatch firstAsked = new CountDownLatch(1);
        CountDownLatch secondAsked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger lookups = new AtomicInteger();
        askedBySecond.onIsAlreadyCaughtUp = () -> {
            if (lookups.incrementAndGet() == 2) {
                // The run of the first thread looks up the latest ask, then waits for the second thread's delivery.
                Thread.currentThread().interrupt();
            } else if (Thread.currentThread().isInterrupted()) {
                throw new IllegalStateException("store read on an interrupted thread");
            }
        };
        AtomicReference<BlockingHandover<String, String>> handoverRef = new AtomicReference<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            log.add(payload);
            if (payload.equals("L1")) {
                handoverRef.get().catchUp(askedFirst);
                firstAsked.countDown();
                awaitOrFail(secondAsked);
            } else if (payload.equals("L2")) {
                awaitOrFail(firstAsked);
                handoverRef.get().catchUp(askedBySecond);
                secondAsked.countDown();
                awaitOrFail(release);
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handoverRef.set(handover);
        handover.catchUp(source(List.of(), true));
        FutureTask<Void> first = new FutureTask<>(() -> handover.accept("L1"), null);
        FutureTask<Void> second = new FutureTask<>(() -> handover.accept("L2"), null);
        FutureTask<Boolean> interrupted = new FutureTask<>(() -> {
            Thread.currentThread().interrupt();
            return handover.acceptReportingDelivery("L3");
        });
        Thread firstThread = new Thread(first, "first");
        Thread secondThread = new Thread(second, "second");
        Thread interruptedThread = new Thread(interrupted, "interrupted");
        firstThread.start();
        secondThread.start();

        try {
            assertThat(first).succeedsWithin(Duration.ofSeconds(5));
            interruptedThread.start();
            assertThat(interrupted).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            // The third lookup is the interrupted thread's, so it did take the ask over and its lookup failed.
            assertThat(lookups).hasValue(3);
            assertThat(handover.refusesPermanently()).isFalse();
            release.countDown();
            assertThat(second).succeedsWithin(Duration.ofSeconds(5));
            assertThat(askedBySecond.replayCallCount).isEqualTo(1);
            assertThat(handover.refusesPermanently()).isFalse();
            assertThat(log).contains("R2");
        } finally {
            release.countDown();
            firstThread.interrupt();
            secondThread.interrupt();
        }
    }

    // The first thread asks first, so the replay is its to run, and a second thread asks for the same replay. The
    // first thread is interrupted by the time it would run it, and its marker lookup fails there, so it puts the
    // replay back and the second thread runs it.
    @Test
    void an_interrupted_thread_whose_catch_up_lookup_fails_leaves_it_to_another_thread_that_asked_for_it() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        FakeSource askedFirst = source(List.of("R1"), false);
        FakeSource askedBySecond = source(List.of("R2"), false);
        CountDownLatch firstAsked = new CountDownLatch(1);
        CountDownLatch secondAsked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger lookups = new AtomicInteger();
        askedBySecond.onIsAlreadyCaughtUp = () -> {
            lookups.incrementAndGet();
            if (Thread.currentThread().isInterrupted()) {
                throw new IllegalStateException("store read on an interrupted thread");
            }
        };
        AtomicReference<BlockingHandover<String, String>> handoverRef = new AtomicReference<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            log.add(payload);
            if (payload.equals("L1")) {
                handoverRef.get().catchUp(askedFirst);
                firstAsked.countDown();
                awaitOrFail(secondAsked);
                Thread.currentThread().interrupt();
            } else if (payload.equals("L2")) {
                awaitOrFail(firstAsked);
                handoverRef.get().catchUp(askedBySecond);
                secondAsked.countDown();
                awaitOrFail(release);
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handoverRef.set(handover);
        handover.catchUp(source(List.of(), true));
        FutureTask<Void> first = new FutureTask<>(() -> handover.accept("L1"), null);
        FutureTask<Void> second = new FutureTask<>(() -> handover.accept("L2"), null);
        Thread firstThread = new Thread(first, "first");
        Thread secondThread = new Thread(second, "second");
        firstThread.start();
        secondThread.start();

        try {
            assertThat(first).succeedsWithin(Duration.ofSeconds(5));
            // The first lookup is the second thread's ask, and the second is the interrupted run of the first thread.
            assertThat(lookups).hasValue(2);
            assertThat(handover.refusesPermanently()).isFalse();
            release.countDown();
            assertThat(second).succeedsWithin(Duration.ofSeconds(5));
            assertThat(askedFirst.replayCallCount + askedBySecond.replayCallCount).isEqualTo(1);
            assertThat(handover.refusesPermanently()).isFalse();
            assertThat(log).endsWith("R2");
        } finally {
            release.countDown();
            firstThread.interrupt();
            secondThread.interrupt();
        }
    }

    // The ask of the second thread is left unowned. A third thread takes it over and waits for a fold on a fourth, so
    // the second thread's own delivery returns before the replay of its ask has run, and the third thread runs it.
    @Test
    void a_left_ask_replays_before_the_call_that_takes_it_over_returns() throws Exception {
        FakeSource askedFirst = source(List.of("R1"), false);
        FakeSource askedBySecond = source(List.of("R2"), false);
        CountDownLatch firstAsked = new CountDownLatch(1);
        CountDownLatch secondAsked = new CountDownLatch(1);
        CountDownLatch firstReturned = new CountDownLatch(1);
        CountDownLatch otherFolding = new CountDownLatch(1);
        CountDownLatch releaseOther = new CountDownLatch(1);
        AtomicReference<Thread> takingOver = new AtomicReference<>();
        AtomicInteger lookups = new AtomicInteger();
        askedBySecond.onIsAlreadyCaughtUp = () -> {
            if (lookups.incrementAndGet() == 2) {
                Thread.currentThread().interrupt();
            }
        };
        AtomicReference<BlockingHandover<String, String>> handoverRef = new AtomicReference<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            if (payload.equals("L0")) {
                otherFolding.countDown();
                awaitOrFail(releaseOther);
            } else if (payload.equals("L1")) {
                handoverRef.get().catchUp(askedFirst);
                firstAsked.countDown();
                awaitOrFail(secondAsked);
            } else if (payload.equals("L2")) {
                awaitOrFail(firstAsked);
                handoverRef.get().catchUp(askedBySecond);
                secondAsked.countDown();
                awaitOrFail(firstReturned);
                awaitWaiting(takingOver.get());
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handoverRef.set(handover);
        handover.catchUp(source(List.of(), true));
        FutureTask<Void> other = new FutureTask<>(() -> handover.accept("L0"), null);
        FutureTask<Void> first = new FutureTask<>(() -> handover.accept("L1"), null);
        FutureTask<Void> second = new FutureTask<>(() -> handover.accept("L2"), null);
        FutureTask<Boolean> takeOver = new FutureTask<>(() -> handover.acceptReportingDelivery("L3"));
        Thread otherThread = new Thread(other, "other");
        Thread firstThread = new Thread(first, "first");
        Thread secondThread = new Thread(second, "second");
        Thread takingOverThread = new Thread(takeOver, "taking over");
        takingOver.set(takingOverThread);
        otherThread.start();
        awaitOrFail(otherFolding);
        firstThread.start();
        secondThread.start();

        try {
            assertThat(first).succeedsWithin(Duration.ofSeconds(5));
            takingOverThread.start();
            firstReturned.countDown();
            assertThat(second).succeedsWithin(Duration.ofSeconds(5));
            int replaysWhenSecondReturned = askedBySecond.replayCallCount;
            releaseOther.countDown();
            assertThat(takeOver).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);

            assertThat(replaysWhenSecondReturned).isZero();
            assertThat(askedBySecond.replayCallCount).isEqualTo(1);
        } finally {
            releaseOther.countDown();
            firstReturned.countDown();
            otherThread.interrupt();
            firstThread.interrupt();
            secondThread.interrupt();
        }
    }

    // The fold of L1 asks for a catch-up while acceptIfLive runs. The caller is not made to wait for it, and its fold of
    // R1 runs on a thread of its own, held here until acceptIfLive has returned.
    @Test
    void acceptIfLive_returns_while_the_catch_up_its_fold_asked_for_replays_on_another_thread() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        AtomicReference<String> replayThread = new AtomicReference<>();
        CountDownLatch foldingR1 = new CountDownLatch(1);
        CountDownLatch releaseR1 = new CountDownLatch(1);
        CountDownLatch marked = new CountDownLatch(1);
        FakeSource asked = source(List.of("R1"), false);
        asked.onMarkCaughtUp = marked::countDown;
        AtomicReference<BlockingHandover<String, String>> handoverRef = new AtomicReference<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            log.add(payload);
            if (payload.equals("L1")) {
                handoverRef.get().catchUp(asked);
            } else if (payload.equals("R1")) {
                replayThread.set(Thread.currentThread().getName());
                foldingR1.countDown();
                holdQuietly(releaseR1);
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handoverRef.set(handover);
        handover.catchUp(source(List.of(), true));
        FutureTask<Boolean> accepting = new FutureTask<>(() -> handover.acceptIfLive("L1"));
        Thread caller = new Thread(accepting, "accept-if-live-caller");

        try {
            caller.start();

            assertThat(accepting).as("acceptIfLive while R1 is held").succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            awaitOrFail(foldingR1);
            assertThat(replayThread.get()).isNotEqualTo("accept-if-live-caller");
            assertThat(handover.isReadyForLiveDelivery()).as("while the replay is held").isFalse();
            releaseR1.countDown();
            awaitOrFail(marked);
            awaitReady(handover);
            assertThat(asked.replayCallCount).isEqualTo(1);
            assertThat(handover.acceptIfLive("L2")).isTrue();
            assertThat(log).containsExactly("L1", "R1", "L2");
        } finally {
            releaseR1.countDown();
            caller.interrupt();
        }
    }

    // The ask of the second thread is left unowned while its fold still runs. acceptIfLive finds it and hands it to a
    // thread of its own rather than running it, though that replay waits for the folds of L0 and L2. The replay runs
    // once they end.
    @Test
    void acceptIfLive_hands_a_left_ask_to_another_thread_and_returns_before_it_replays() throws Exception {
        FakeSource askedFirst = source(List.of("R1"), false);
        FakeSource askedBySecond = source(List.of("R2"), false);
        CountDownLatch firstAsked = new CountDownLatch(1);
        CountDownLatch secondAsked = new CountDownLatch(1);
        CountDownLatch otherFolding = new CountDownLatch(1);
        CountDownLatch releaseOther = new CountDownLatch(1);
        CountDownLatch takeOverReturned = new CountDownLatch(1);
        CountDownLatch marked = new CountDownLatch(1);
        AtomicReference<String> replayThread = new AtomicReference<>();
        AtomicInteger lookups = new AtomicInteger();
        askedBySecond.onIsAlreadyCaughtUp = () -> {
            if (lookups.incrementAndGet() == 2) {
                Thread.currentThread().interrupt();
            }
        };
        askedBySecond.onMarkCaughtUp = marked::countDown;
        AtomicReference<BlockingHandover<String, String>> handoverRef = new AtomicReference<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            if (payload.equals("L0")) {
                otherFolding.countDown();
                holdQuietly(releaseOther);
            } else if (payload.equals("L1")) {
                handoverRef.get().catchUp(askedFirst);
                firstAsked.countDown();
                awaitOrFail(secondAsked);
            } else if (payload.equals("L2")) {
                awaitOrFail(firstAsked);
                handoverRef.get().catchUp(askedBySecond);
                secondAsked.countDown();
                holdQuietly(takeOverReturned);
            } else if (payload.equals("R2")) {
                replayThread.set(Thread.currentThread().getName());
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handoverRef.set(handover);
        handover.catchUp(source(List.of(), true));
        FutureTask<Void> other = new FutureTask<>(() -> handover.accept("L0"), null);
        FutureTask<Void> first = new FutureTask<>(() -> handover.accept("L1"), null);
        FutureTask<Void> second = new FutureTask<>(() -> handover.accept("L2"), null);
        FutureTask<Boolean> takeOver = new FutureTask<>(() -> handover.acceptIfLive("L3"));
        Thread otherThread = new Thread(other, "other");
        Thread firstThread = new Thread(first, "first");
        Thread secondThread = new Thread(second, "second");
        Thread takingOverThread = new Thread(takeOver, "taking over");
        otherThread.start();
        awaitOrFail(otherFolding);
        firstThread.start();
        secondThread.start();

        try {
            assertThat(first).succeedsWithin(Duration.ofSeconds(5));
            takingOverThread.start();
            assertThat(takeOver).as("acceptIfLive while L0 and L2 are folded").succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            int replaysWhenItReturned = askedBySecond.replayCallCount;
            takeOverReturned.countDown();
            assertThat(second).succeedsWithin(Duration.ofSeconds(5));
            releaseOther.countDown();
            awaitOrFail(marked);
            awaitReady(handover);

            assertThat(replaysWhenItReturned).isZero();
            assertThat(askedBySecond.replayCallCount).isEqualTo(1);
            assertThat(replayThread.get()).isNotIn("taking over", "second", "first", "other");
        } finally {
            releaseOther.countDown();
            takeOverReturned.countDown();
            otherThread.interrupt();
            firstThread.interrupt();
            secondThread.interrupt();
        }
    }

    // A catch-up whose marker lookup throws fails while the replay a live fold asked for runs. The failure came after
    // the ask, but the replay had taken its turn, so the ask is not refused and the replay goes on to mark caught up.
    @Test
    void a_catch_up_failure_while_an_asked_replay_runs_does_not_refuse_its_ask() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        ExecutorService otherThread = Executors.newSingleThreadExecutor();
        FakeSource asked = source(List.of("R1", "R2"), false);
        FakeSource failingLookup = source(List.of(), false);
        failingLookup.onIsAlreadyCaughtUp = () -> {
            throw new IllegalStateException("lookup failed");
        };
        AtomicReference<BlockingHandover<String, String>> handoverRef = new AtomicReference<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            log.add(payload);
            if (payload.equals("L1")) {
                handoverRef.get().catchUp(asked);
            } else if (payload.equals("R1")) {
                Future<Boolean> failed = otherThread.submit(() -> handoverRef.get().catchUp(failingLookup));
                assertThat(failed).failsWithin(Duration.ofSeconds(5));
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handoverRef.set(handover);
        handover.catchUp(source(List.of(), true));

        try {
            handover.accept("L1");

            assertThat(handover.refusesPermanently()).isTrue();
            assertThat(asked.replayCallCount).isEqualTo(1);
            assertThat(asked.markCaughtUpCallCount()).isEqualTo(1);
            assertThat(log).containsExactly("L1", "R1", "R2");
        } finally {
            otherThread.shutdownNow();
        }
    }

    // The second thread's view is interrupted and asks for a catch-up whose marker lookup fails for it. That failure
    // reaches its view, and the ask the first thread made before it still replays.
    @Test
    void a_catch_up_lookup_that_fails_on_an_interrupted_thread_refuses_no_ask_of_another_thread() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        FakeSource askedByFirst = source(List.of("R1"), false);
        FakeSource askedBySecond = source(List.of("R2"), false);
        askedByFirst.failWhenInterrupted = true;
        askedBySecond.failWhenInterrupted = true;
        CountDownLatch firstAsked = new CountDownLatch(1);
        CountDownLatch secondDone = new CountDownLatch(1);
        AtomicReference<Throwable> thrownIntoSecond = new AtomicReference<>();
        AtomicReference<BlockingHandover<String, String>> handoverRef = new AtomicReference<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            log.add(payload);
            if (payload.equals("L1")) {
                handoverRef.get().catchUp(askedByFirst);
                firstAsked.countDown();
                awaitOrFail(secondDone);
            } else if (payload.equals("L2")) {
                awaitOrFail(firstAsked);
                Thread.currentThread().interrupt();
                try {
                    handoverRef.get().catchUp(askedBySecond);
                } catch (Throwable e) {
                    thrownIntoSecond.set(e);
                } finally {
                    secondDone.countDown();
                }
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handoverRef.set(handover);
        handover.catchUp(source(List.of(), true));
        FutureTask<Void> first = new FutureTask<>(() -> handover.accept("L1"), null);
        FutureTask<Void> second = new FutureTask<>(() -> handover.accept("L2"), null);
        Thread firstThread = new Thread(first, "first");
        Thread secondThread = new Thread(second, "second");
        firstThread.start();
        secondThread.start();

        try {
            assertThat(second).succeedsWithin(Duration.ofSeconds(5));
            assertThat(first).succeedsWithin(Duration.ofSeconds(5));

            assertThat(thrownIntoSecond.get()).hasMessage(INTERRUPTED_READ);
            assertThat(handover.refusesPermanently()).isFalse();
            assertThat(askedByFirst.replayCallCount).isEqualTo(1);
            assertThat(log).containsExactlyInAnyOrder("L1", "L2", "R1");
            handover.accept("L3");
            assertThat(log).endsWith("L3");
        } finally {
            secondDone.countDown();
        }
    }

    // The thread is interrupted after its view asked, so the marker lookup of its own run fails. The catch-up stays
    // asked for, and the next call runs it.
    @Test
    void an_asked_catch_up_whose_lookup_fails_on_an_interrupted_thread_is_left_for_the_next_call() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        FakeSource asked = source(List.of("R1"), false);
        asked.failWhenInterrupted = true;
        AtomicReference<BlockingHandover<String, String>> handoverRef = new AtomicReference<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            log.add(payload);
            if (payload.equals("L1")) {
                handoverRef.get().catchUp(asked);
                Thread.currentThread().interrupt();
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handoverRef.set(handover);
        handover.catchUp(source(List.of(), true));
        FutureTask<Void> interrupted = new FutureTask<>(() -> handover.accept("L1"), null);
        new Thread(interrupted, "interrupted").start();

        assertThat(interrupted).succeedsWithin(Duration.ofSeconds(5));
        assertThat(handover.refusesPermanently()).isFalse();
        assertThat(asked.replayCallCount).isZero();
        handover.accept("L2");

        assertThat(handover.refusesPermanently()).isFalse();
        assertThat(asked.replayCallCount).isEqualTo(1);
        assertThat(asked.markCaughtUpCallCount()).isEqualTo(1);
        assertThat(log).containsExactly("L1", "L2", "R1");
    }

    // The first thread takes the catch-up over and its lookup passes. The second thread asks for the same replay, and
    // the first thread is interrupted before the replay opens, so opening the replay fails. Both asks are left for
    // the next call.
    @Test
    void an_interrupt_after_the_takeover_leaves_every_ask_for_the_next_call() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        FakeSource askedByFirst = source(List.of("R1"), false);
        FakeSource askedBySecond = source(List.of("R2"), false);
        askedByFirst.failWhenInterrupted = true;
        askedBySecond.failWhenInterrupted = true;
        CountDownLatch tookOver = new CountDownLatch(1);
        CountDownLatch secondReturned = new CountDownLatch(1);
        AtomicInteger lookups = new AtomicInteger();
        askedByFirst.onIsAlreadyCaughtUp = () -> {
            if (lookups.incrementAndGet() == 2) {
                tookOver.countDown();
                awaitOrFail(secondReturned);
                Thread.currentThread().interrupt();
            }
        };
        AtomicReference<BlockingHandover<String, String>> handoverRef = new AtomicReference<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            log.add(payload);
            if (payload.equals("L1")) {
                handoverRef.get().catchUp(askedByFirst);
            } else if (payload.equals("L2")) {
                handoverRef.get().catchUp(askedBySecond);
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handoverRef.set(handover);
        handover.catchUp(source(List.of(), true));
        FutureTask<Void> first = new FutureTask<>(() -> handover.accept("L1"), null);
        Thread firstThread = new Thread(first, "first");
        firstThread.start();

        try {
            awaitOrFail(tookOver);
            FutureTask<Void> second = new FutureTask<>(() -> handover.accept("L2"), null);
            new Thread(second, "second").start();
            assertThat(second).succeedsWithin(Duration.ofSeconds(5));
            secondReturned.countDown();
            assertThat(first).succeedsWithin(Duration.ofSeconds(5));
            assertThat(handover.refusesPermanently()).isFalse();
            assertThat(log).doesNotContain("R2");
            handover.accept("L3");

            assertThat(handover.refusesPermanently()).isFalse();
            assertThat(log).containsExactly("L1", "L2", "L3", "R2");
            assertThat(askedBySecond.markCaughtUpCallCount()).isEqualTo(1);
        } finally {
            secondReturned.countDown();
        }
    }

    // The second thread runs the replay both threads asked for and is interrupted by a fold of it, so the next read
    // of the history fails. The asks are left for the next call, which replays the whole history.
    @Test
    void an_interrupt_during_an_asked_replay_leaves_its_asks_for_the_next_call() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        FakeSource askedByFirst = source(List.of("R1"), false);
        FakeSource askedBySecond = source(List.of("R2", "R3"), false);
        askedByFirst.failWhenInterrupted = true;
        askedBySecond.failWhenInterrupted = true;
        CountDownLatch firstAsked = new CountDownLatch(1);
        CountDownLatch secondAsked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean interruptOnce = new AtomicBoolean(true);
        AtomicReference<BlockingHandover<String, String>> handoverRef = new AtomicReference<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            log.add(payload);
            if (payload.equals("L1")) {
                handoverRef.get().catchUp(askedByFirst);
                firstAsked.countDown();
                awaitOrFail(secondAsked);
                Thread.currentThread().interrupt();
            } else if (payload.equals("L2")) {
                awaitOrFail(firstAsked);
                handoverRef.get().catchUp(askedBySecond);
                secondAsked.countDown();
                awaitOrFail(release);
            } else if (payload.equals("R2") && interruptOnce.getAndSet(false)) {
                Thread.currentThread().interrupt();
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handoverRef.set(handover);
        handover.catchUp(source(List.of(), true));
        FutureTask<Void> first = new FutureTask<>(() -> handover.accept("L1"), null);
        FutureTask<Void> second = new FutureTask<>(() -> handover.accept("L2"), null);
        Thread firstThread = new Thread(first, "first");
        Thread secondThread = new Thread(second, "second");
        firstThread.start();

        try {
            // Started once the first thread has asked, so L1 is in the log before L2. The handover orders the two
            // live folds in no way, and started together either thread can log first.
            awaitOrFail(firstAsked);
            secondThread.start();
            assertThat(first).succeedsWithin(Duration.ofSeconds(5));
            assertThat(handover.refusesPermanently()).isFalse();
            release.countDown();
            assertThat(second).succeedsWithin(Duration.ofSeconds(5));
            assertThat(handover.refusesPermanently()).isFalse();
            assertThat(log).containsExactly("L1", "L2", "R2");
            handover.accept("L3");

            assertThat(handover.refusesPermanently()).isFalse();
            assertThat(log).containsExactly("L1", "L2", "R2", "L3", "R2", "R3");
            assertThat(askedBySecond.markCaughtUpCallCount()).isEqualTo(1);
        } finally {
            release.countDown();
        }
    }

    // A fold of the asked replay asks again and interrupts its thread, so the next read of the history fails. The ask
    // it made and the asks of the replay are both left for the next call.
    @Test
    void an_ask_made_by_a_replay_that_fails_on_an_interrupt_is_left_for_the_next_call_too() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        FakeSource asked = source(List.of("R1", "R2"), false);
        FakeSource askedByReplay = source(List.of("R3"), false);
        asked.failWhenInterrupted = true;
        askedByReplay.failWhenInterrupted = true;
        AtomicBoolean askOnce = new AtomicBoolean(true);
        AtomicReference<BlockingHandover<String, String>> handoverRef = new AtomicReference<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            log.add(payload);
            if (payload.equals("L1")) {
                handoverRef.get().catchUp(asked);
            } else if (payload.equals("R1") && askOnce.getAndSet(false)) {
                handoverRef.get().catchUp(askedByReplay);
                Thread.currentThread().interrupt();
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handoverRef.set(handover);
        handover.catchUp(source(List.of(), true));
        FutureTask<Void> interrupted = new FutureTask<>(() -> handover.accept("L1"), null);
        new Thread(interrupted, "interrupted").start();

        assertThat(interrupted).succeedsWithin(Duration.ofSeconds(5));
        assertThat(log).containsExactly("L1", "R1");
        assertThat(handover.refusesPermanently()).isFalse();
        handover.accept("L2");

        assertThat(log).containsExactly("L1", "R1", "L2", "R1", "R2");
        assertThat(asked.markCaughtUpCallCount()).isEqualTo(1);
    }

    // An InterruptedException can come from another thread, a pool task its shutdown interrupted say, while the thread
    // running the catch-up is not interrupted. The view fails the same way on every replay, so the failure is recorded.
    @Test
    void a_catch_up_failure_with_an_interrupted_exception_as_its_cause_on_a_thread_that_is_not_interrupted_is_recorded() {
        AtomicInteger folds = new AtomicInteger();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            folds.incrementAndGet();
            if (payload.equals("R1")) {
                throw new IllegalStateException("remote call failed",
                        new ExecutionException(new InterruptedException("worker interrupted by pool shutdown")));
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);

        Throwable thrown = catchThrowable(() -> handover.catchUp(source(List.of("R0", "R1"), false)));
        boolean interruptedAfter = Thread.interrupted();

        assertThat(thrown).hasRootCauseInstanceOf(InterruptedException.class);
        assertThat(handover.refusesPermanently()).isTrue();
        assertThat(interruptedAfter).isFalse();
        assertThat(folds).hasValue(2);
    }

    // The first payload of the drain interrupts its thread, so the view fails the next one. The payloads the drain did
    // not deliver go back into the buffer and the handover is not live, so none of them is dropped and no later
    // payload is delivered ahead of them.
    @Test
    void a_drain_that_fails_on_an_interrupt_keeps_the_payloads_it_did_not_deliver_for_the_next_catch_up() {
        List<String> log = new CopyOnWriteArrayList<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            if (Thread.currentThread().isInterrupted()) {
                throw new IllegalStateException("write on an interrupted thread");
            }
            log.add(payload);
            if (payload.equals("L1")) {
                Thread.currentThread().interrupt();
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        assertThat(handover.acceptReportingDelivery("L1")).isTrue();
        assertThat(handover.acceptReportingDelivery("L2")).isTrue();
        assertThat(handover.acceptReportingDelivery("L3")).isTrue();

        Throwable thrown = catchThrowable(() -> handover.catchUp(source(List.of(), false)));
        boolean interruptedAfter = Thread.interrupted();

        assertThat(thrown).hasMessage("write on an interrupted thread");
        assertThat(interruptedAfter).isTrue();
        assertThat(handover.refusesPermanently()).isFalse();
        assertThat(handover.isReadyForLiveDelivery()).isFalse();
        assertThat(handover.acceptIfLive("L4")).isFalse();
        assertThat(handover.catchUp(source(List.of(), false))).isTrue();
        assertThat(log).containsExactly("L1", "L2", "L3");
        assertThat(handover.acceptIfLive("L4")).isTrue();
        assertThat(log).containsExactly("L1", "L2", "L3", "L4");
    }

    // A caller of accept(..) waits for its payload. The drain the interrupt failed did not apply L2, so its caller is
    // answered as when a catch-up is stopped before the handover goes live, and offers it again.
    @Test
    void a_drain_that_fails_on_an_interrupt_answers_a_waiting_caller_it_did_not_deliver_to_as_stopped() throws InterruptedException {
        List<String> log = new CopyOnWriteArrayList<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            if (Thread.currentThread().isInterrupted()) {
                throw new IllegalStateException("write on an interrupted thread");
            }
            log.add(payload);
            if (payload.equals("L1")) {
                Thread.currentThread().interrupt();
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        AtomicReference<Throwable> secondFailure = new AtomicReference<>();
        Thread first = new Thread(() -> firstFailure.set(catchThrowable(() -> handover.accept("L1"))), "first");
        Thread second = new Thread(() -> secondFailure.set(catchThrowable(() -> handover.accept("L2"))), "second");
        first.start();
        awaitWaiting(first);
        second.start();
        awaitWaiting(second);

        Throwable thrown = catchThrowable(() -> handover.catchUp(source(List.of(), false)));
        boolean interruptedAfter = Thread.interrupted();
        first.join(5_000);
        second.join(5_000);

        assertThat(thrown).hasMessage("write on an interrupted thread");
        assertThat(interruptedAfter).isTrue();
        assertThat(firstFailure.get()).isNull();
        assertThat(secondFailure.get()).isInstanceOf(BlockingHandover.PreDispatchRefusalException.class)
                .hasMessage(HandoverMessages.stoppedBeforeApplied(NOUN));
        assertThat(handover.refusesPermanently()).isFalse();
        assertThat(handover.isReadyForLiveDelivery()).isFalse();
        assertThat(handover.catchUp(source(List.of(), false))).isTrue();
        handover.accept("L2");
        assertThat(log).containsExactly("L1", "L2");
    }

    // Every payload is delivered before the marker is written, so a marker write that fails on an interrupt leaves the
    // handover live. With no marker written, the next catch-up replays the whole history again.
    @Test
    void a_marker_write_that_fails_on_an_interrupt_leaves_the_handover_live_and_the_next_catch_up_replays_again() {
        List<String> log = new CopyOnWriteArrayList<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(log::add, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        FakeSource interruptedMarker = source(List.of("R1"), false);
        AtomicBoolean interruptOnce = new AtomicBoolean(true);
        interruptedMarker.onMarkCaughtUp = () -> {
            if (interruptOnce.getAndSet(false)) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("marker write on an interrupted thread");
            }
        };

        Throwable thrown = catchThrowable(() -> handover.catchUp(interruptedMarker));
        boolean interruptedAfter = Thread.interrupted();

        assertThat(thrown).hasMessage("marker write on an interrupted thread");
        assertThat(interruptedAfter).isTrue();
        assertThat(handover.refusesPermanently()).isFalse();
        assertThat(handover.isReadyForLiveDelivery()).isTrue();
        assertThat(handover.catchUp(interruptedMarker)).isTrue();
        assertThat(log).containsExactly("R1", "R1");
        assertThat(interruptedMarker.markCaughtUpCallCount()).isEqualTo(2);
    }

    // Clients throw an InterruptedIOException for a timeout too, one that no interrupt caused, so it fails the catch-up
    // like any other failure.
    @Test
    void a_catch_up_failure_caused_by_a_timeout_is_recorded() {
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        FakeSource timedOut = source(List.of("R1"), false);
        timedOut.replayFailure = new UncheckedIOException(new InterruptedIOException("timeout"));

        Throwable thrown = catchThrowable(() -> handover.catchUp(timedOut));
        boolean interruptedAfter = Thread.interrupted();

        assertThat(thrown).isSameAs(timedOut.replayFailure);
        assertThat(interruptedAfter).isFalse();
        assertThat(handover.refusesPermanently()).isTrue();
    }

    private static void awaitWaiting(Thread thread, AtomicBoolean ready) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!(ready.get() && thread.getState() == Thread.State.WAITING)) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(thread.getName() + " did not start waiting");
            }
            Thread.onSpinWait();
        }
    }

    private static void awaitOrFail(CountDownLatch latch) {
        try {
            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    // Held for longer than any assertion waits, so a call that waits for the fold fails on its own assertion.
    private static void holdQuietly(CountDownLatch latch) {
        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void awaitReady(BlockingHandover<String, String> handover) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!handover.isReadyForLiveDelivery()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("The handover did not become live");
            }
            Thread.onSpinWait();
        }
    }

    private static String[] concat(String[] first, String... second) {
        return Stream.concat(Stream.of(first), Stream.of(second)).toArray(String[]::new);
    }

    // A replay that starts waits for the live fold that is running, but it has already stopped live delivery and is
    // not running yet, so the fold's catch-up goes live the way one from any other thread would at that point.
    // acceptIfLive(..) accepts until the replay starts, and the replay goes live again once it ends.
    @Test
    void a_catch_up_with_nothing_to_replay_from_a_live_fold_a_starting_replay_waits_for_takes_the_handover_live() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Boolean> answers = new CopyOnWriteArrayList<>();
        CountDownLatch foldingL1 = new CountDownLatch(1);
        CountDownLatch releaseL1 = new CountDownLatch(1);
        ExecutorService otherThread = Executors.newSingleThreadExecutor();
        AtomicReference<BlockingHandover<String, String>> self = new AtomicReference<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            log.add(payload);
            if (payload.equals("L1")) {
                foldingL1.countDown();
                awaitLatch(releaseL1);
                answers.add(self.get().catchUp(source(List.of(), true)));
                try {
                    answers.add(otherThread.submit(() -> self.get().acceptIfLive("L2")).get(5, TimeUnit.SECONDS));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        self.set(handover);
        handover.catchUp(source(List.of(), true));
        FutureTask<Boolean> folding = new FutureTask<>(() -> handover.acceptIfLive("L1"));
        FutureTask<Boolean> replaying = new FutureTask<>(() -> handover.catchUp(source(List.of("R"), false)));
        Thread live = new Thread(folding, "live-delivery");
        Thread replay = new Thread(replaying, "replay");
        try {
            live.start();
            awaitLatch(foldingL1);
            replay.start();
            awaitWaiting(replay);

            releaseL1.countDown();

            assertThat(folding).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(replaying).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(answers).as("what the fold's catch-up and the acceptIfLive(..) after it returned").containsExactly(true, true);
            assertThat(log).containsExactly("L1", "L2", "R");
            assertThat(handover.acceptIfLive("L3")).isTrue();
        } finally {
            releaseL1.countDown();
            live.interrupt();
            replay.interrupt();
            otherThread.shutdownNow();
        }
    }

    // The same from Source.alreadyDeliveredByReplay(..), which a starting replay waits for the way it waits for a live
    // fold.
    @Test
    void a_catch_up_with_nothing_to_replay_from_already_delivered_by_replay_a_starting_replay_waits_for_takes_the_handover_live() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Boolean> answers = new CopyOnWriteArrayList<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(log::add, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        CountDownLatch reportingR1 = new CountDownLatch(1);
        CountDownLatch releaseReport = new CountDownLatch(1);
        ExecutorService otherThread = Executors.newSingleThreadExecutor();
        FakeSource replayed = source(List.of("R1"), false);
        replayed.onAlreadyDeliveredByReplay = () -> {
            reportingR1.countDown();
            awaitLatch(releaseReport);
            answers.add(handover.catchUp(source(List.of(), true)));
            try {
                answers.add(otherThread.submit(() -> handover.acceptIfLive("L2")).get(5, TimeUnit.SECONDS));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        handover.catchUp(replayed);
        FutureTask<Boolean> reporting = new FutureTask<>(() -> handover.acceptIfLive("R1"));
        FutureTask<Boolean> replaying = new FutureTask<>(() -> handover.catchUp(source(List.of("R2"), false)));
        Thread live = new Thread(reporting, "live-delivery");
        Thread replay = new Thread(replaying, "replay");
        try {
            live.start();
            awaitLatch(reportingR1);
            replay.start();
            awaitWaiting(replay);

            releaseReport.countDown();

            assertThat(reporting).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(replaying).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(answers).as("what the callback's catch-up and the acceptIfLive(..) after it returned").containsExactly(true, true);
            assertThat(log).containsExactly("R1", "L2", "R2");
            assertThat(handover.acceptIfLive("L1")).isTrue();
        } finally {
            releaseReport.countDown();
            live.interrupt();
            replay.interrupt();
            otherThread.shutdownNow();
        }
    }

    // Records every payload it folds, and holds the fold of the given payload until released, throwing afterwards
    // when given a failure.
    private static BlockingHandover<String, String> holdingAt(String held, List<String> log, CountDownLatch reached, CountDownLatch release,
                                                              RuntimeException failure) {
        return BlockingHandover.create(payload -> {
            if (payload.equals(held)) {
                reached.countDown();
                try {
                    release.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if (failure != null) {
                    throw failure;
                }
            }
            log.add(payload);
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
    }

    /**
     * A catch-up that gives up on an interrupt answers for its own caller. Marking the handover stopped while another
     * catch-up is replaying would answer for that one too, and the payloads arriving after it would be dropped instead
     * of joining the replay's buffer.
     */
    @Test
    void an_interrupted_wait_behind_another_replay_leaves_that_replay_taking_payloads() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch foldingR1 = new CountDownLatch(1);
        CountDownLatch releaseR1 = new CountDownLatch(1);
        CountDownLatch secondReplayStarted = new CountDownLatch(1);
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            log.add(payload);
            if (payload.equals("R1")) {
                foldingR1.countDown();
                awaitLatch(releaseR1);
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        FakeSource second = source(List.of("R3"), false);
        second.onReplayStarted = secondReplayStarted::countDown;
        Thread replay = new Thread(() -> handover.catchUp(source(List.of("R1", "R2"), false)), "replay");
        Thread waiting = new Thread(() -> handover.catchUp(second), "waiting-catch-up");

        replay.start();
        awaitLatch(foldingR1);
        waiting.start();
        assertThat(reachedWithin(secondReplayStarted, 300)).isFalse();
        waiting.interrupt();
        waiting.join(5_000);
        handover.acceptReportingDelivery("L1");
        releaseR1.countDown();
        replay.join(5_000);

        assertThat(log).as("the payload joined the running replay's buffer").containsExactly("R1", "R2", "L1");
    }

    /**
     * The same as the replay above, for a catch-up with nothing to replay that is part way through going live. It owns
     * the handover too, so an interrupted catch-up waiting behind it must not mark the handover stopped and have the
     * payloads that arrive next dropped rather than drained by it.
     */
    @Test
    void an_interrupted_wait_behind_a_catch_up_going_live_leaves_that_catch_up_taking_payloads() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch transitionReached = new CountDownLatch(1);
        CountDownLatch releaseTransition = new CountDownLatch(1);
        CountDownLatch replayStarted = new CountDownLatch(1);
        BlockingHandover<String, String> handover = BlockingHandover.create(
                log::add, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        FakeSource goingLive = source(List.of(), true);
        goingLive.onHistoryDone = () -> {
            transitionReached.countDown();
            awaitLatch(releaseTransition);
        };
        FakeSource replaying = source(List.of("R1"), false);
        replaying.onReplayStarted = replayStarted::countDown;
        Thread goLive = new Thread(() -> handover.catchUp(goingLive), "go-live");
        Thread waiting = new Thread(() -> handover.catchUp(replaying), "waiting-catch-up");
        ExecutorService outside = Executors.newSingleThreadExecutor();
        try {
            goLive.start();
            awaitLatch(transitionReached);
            waiting.start();
            assertThat(reachedWithin(replayStarted, 300)).isFalse();
            waiting.interrupt();
            waiting.join(5_000);
            // A payload fed from outside waits for the drain, and is dropped at once when the interrupted catch-up
            // marked the handover stopped.
            Future<Boolean> fed = outside.submit(() -> handover.acceptReportingDelivery("L1"));
            assertStillWaiting(fed, "L1 fed while the catch-up going live drains");
            releaseTransition.countDown();
            goLive.join(5_000);

            assertThat(fed).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(log).as("the payload was delivered by the handover the catch-up that was going live made live").containsExactly("L1");
        } finally {
            releaseTransition.countDown();
            outside.shutdownNow();
        }
    }

    /**
     * A catch-up that failed leaves the handover refusing every payload and its caller told to replace it. A replay
     * waiting behind that failure must not start, since it would fold a history into a view nobody is using any more.
     */
    @Test
    void a_replay_queued_behind_a_failed_catch_up_does_not_start() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch foldingR1 = new CountDownLatch(1);
        CountDownLatch releaseR1 = new CountDownLatch(1);
        CountDownLatch queuedReplayStarted = new CountDownLatch(1);
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            log.add(payload);
            if (payload.equals("R1")) {
                foldingR1.countDown();
                awaitLatch(releaseR1);
            }
            if (payload.equals("R2")) {
                throw new IllegalStateException("fold failed");
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        FakeSource queued = source(List.of("R3"), false);
        queued.onReplayStarted = queuedReplayStarted::countDown;
        AtomicReference<Throwable> failed = new AtomicReference<>();
        AtomicReference<Throwable> queuedOutcome = new AtomicReference<>();
        Thread failing = new Thread(() -> failed.set(catchThrowable(() -> handover.catchUp(source(List.of("R1", "R2"), false)))), "failing");
        Thread waiting = new Thread(() -> queuedOutcome.set(catchThrowable(() -> handover.catchUp(queued))), "queued");

        failing.start();
        awaitLatch(foldingR1);
        waiting.start();
        assertThat(reachedWithin(queuedReplayStarted, 300)).isFalse();
        releaseR1.countDown();
        failing.join(5_000);
        waiting.join(5_000);

        assertThat(failed.get()).isInstanceOf(IllegalStateException.class);
        assertThat(queuedOutcome.get()).as("the queued catch-up was refused rather than run")
                .isInstanceOf(BlockingHandover.PreDispatchRefusalException.class);
        assertThat(log).containsExactly("R1", "R2");
    }

    /**
     * A catch-up revives a handover a previous one stopped, and one waiting for its turn has to revive it again when
     * it gets the turn. The catch-up it waited for can stop in between, and a handover left stopped drops the payloads
     * arriving during the replay that follows rather than buffering them for it.
     */
    @Test
    void a_replay_that_waited_for_a_stopped_catch_up_takes_in_the_payloads_that_arrive_during_it() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch foldingR1 = new CountDownLatch(1);
        CountDownLatch releaseR1 = new CountDownLatch(1);
        CountDownLatch foldingR3 = new CountDownLatch(1);
        CountDownLatch releaseR3 = new CountDownLatch(1);
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            log.add(payload);
            if (payload.equals("R1")) {
                foldingR1.countDown();
                awaitLatch(releaseR1);
            }
            if (payload.equals("R3")) {
                foldingR3.countDown();
                awaitLatch(releaseR3);
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        FakeSource stopping = source(List.of("R1", "R2"), false);
        stopping.stopAfter(1);
        Thread first = new Thread(() -> handover.catchUp(stopping), "stopping");
        Thread second = new Thread(() -> handover.catchUp(source(List.of("R3"), false)), "queued");

        first.start();
        awaitLatch(foldingR1);
        second.start();
        releaseR1.countDown();
        first.join(5_000);
        awaitLatch(foldingR3);
        handover.acceptReportingDelivery("L1");
        releaseR3.countDown();
        second.join(5_000);

        assertThat(log).as("the payload was buffered for the replay that was running")
                .containsExactly("R1", "R3", "L1");
    }

    // A handler written in Kotlin can throw a checked exception without declaring it. The handover has to record it
    // like any other replay failure, or it keeps buffering live payloads and returning normally, which acknowledges
    // them into a replay that is never coming back.
    @Test
    void a_checked_exception_from_a_replayed_fold_is_recorded_and_a_later_live_payload_is_refused() {
        assertThatAFoldFailureIsRecordedAndALaterLivePayloadRefused(new IOException("the view is down"));
    }

    @Test
    void a_runtime_exception_from_a_replayed_fold_is_recorded_and_a_later_live_payload_is_refused() {
        assertThatAFoldFailureIsRecordedAndALaterLivePayloadRefused(new IllegalStateException("the view is down"));
    }

    // The failure the drain hits after a failed replay on a live handover is attached to the replay failure rather
    // than replacing it, and the replay failure is still recorded, whatever kind of exception the drain threw.
    @Test
    void a_checked_exception_draining_the_buffer_after_a_failed_replay_does_not_keep_the_failure_from_being_recorded() {
        RuntimeException replayFailure = new IllegalStateException("replay boom");
        Exception drainFailure = new IOException("the view is down");

        assertThatADrainFailureAfterAFailedReplayLeavesTheFailureRecorded(replayFailure, drainFailure);

        assertThat(replayFailure.getSuppressed()).containsExactly(drainFailure);
    }

    @Test
    void a_runtime_exception_draining_the_buffer_after_a_failed_replay_does_not_keep_the_failure_from_being_recorded() {
        RuntimeException replayFailure = new IllegalStateException("replay boom");
        Exception drainFailure = new IllegalStateException("the view is down");

        assertThatADrainFailureAfterAFailedReplayLeavesTheFailureRecorded(replayFailure, drainFailure);

        assertThat(replayFailure.getSuppressed()).containsExactly(drainFailure);
    }

    // A view that throws one shared exception object, a Kotlin object declaration or a cached instance, throws the
    // same instance from the replay and from the drain. Java refuses to suppress an exception under itself.
    @Test
    void the_same_exception_instance_from_the_replay_and_the_drain_does_not_keep_the_failure_from_being_recorded() {
        RuntimeException sharedFailure = new IllegalStateException("the view is down");

        assertThatADrainFailureAfterAFailedReplayLeavesTheFailureRecorded(sharedFailure, sharedFailure);

        assertThat(sharedFailure.getSuppressed()).isEmpty();
    }

    @Test
    void a_replay_abandoned_that_throws_a_checked_exception_does_not_mask_the_failure_that_triggered_it() {
        assertThatAThrowingReplayAbandonedDoesNotMaskTheFailure(new IOException("replayAbandoned boom"));
    }

    @Test
    void a_replay_abandoned_that_throws_a_runtime_exception_does_not_mask_the_failure_that_triggered_it_or_go_unrecorded() {
        assertThatAThrowingReplayAbandonedDoesNotMaskTheFailure(new IllegalStateException("replayAbandoned boom"));
    }

    // A payload reserved for the drain stays reserved when alreadyDeliveredByReplay throws before it is delivered, and
    // a later catch-up waits for every reserved payload before it replays, so it would wait for good.
    @Test
    void a_checked_exception_from_already_delivered_by_replay_changes_only_its_payload_and_does_not_leave_a_later_catch_up_waiting_for_good() throws Exception {
        assertThatAThrowingAlreadyDeliveredByReplayReleasesTheDrain(new IOException("the listener is down"));
    }

    @Test
    void a_runtime_exception_from_already_delivered_by_replay_changes_only_its_payload_and_does_not_leave_a_later_catch_up_waiting_for_good() throws Exception {
        assertThatAThrowingAlreadyDeliveredByReplayReleasesTheDrain(new IllegalStateException("the listener is down"));
    }

    // The drain holds the handover not live until a round finds the buffer empty, so a payload fed from outside while
    // it delivers waits for the drain to end, which is what lets the drain find the buffer empty however fast payloads
    // come, and is then delivered after every payload the drain took. Live meanwhile, L1 would reach deliver ahead of B2.
    @Test
    void a_payload_fed_while_the_drain_delivers_is_delivered_after_every_payload_the_drain_took() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch deliveringB1 = new CountDownLatch(1);
        CountDownLatch releaseB1 = new CountDownLatch(1);
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            if (payload.equals("B1")) {
                deliveringB1.countDown();
                awaitLatch(releaseB1);
            }
            delivered.add(payload);
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handover.acceptReportingDelivery("B1");
        handover.acceptReportingDelivery("B2");
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> catchUp = executor.submit(() -> handover.catchUp(source(List.of("R1"), false)));
            awaitLatch(deliveringB1);

            Future<Boolean> fed = executor.submit(() -> handover.acceptReportingDelivery("L1"));
            assertStillWaiting(fed, "L1 fed from outside while the drain delivers B1");
            assertThat(delivered).as("delivered while the drain holds B1").containsExactly("R1");
            releaseB1.countDown();

            assertThat(catchUp).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(fed).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(delivered).containsExactly("R1", "B1", "B2", "L1");
        } finally {
            releaseB1.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void acceptIfLive_reports_false_without_buffering_while_the_drain_delivers() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch deliveringB1 = new CountDownLatch(1);
        CountDownLatch releaseB1 = new CountDownLatch(1);
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            if (payload.equals("B1")) {
                deliveringB1.countDown();
                awaitLatch(releaseB1);
            }
            delivered.add(payload);
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handover.acceptReportingDelivery("B1");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> catchUp = executor.submit(() -> handover.catchUp(source(List.of(), true)));
            awaitLatch(deliveringB1);

            assertThat(handover.acceptIfLive("L1")).as("acceptIfLive while the drain delivers B1").isFalse();
            assertThat(handover.isReadyForLiveDelivery()).as("live while the drain delivers").isFalse();
            releaseB1.countDown();

            assertThat(catchUp).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(delivered).as("L1 was refused, not buffered").containsExactly("B1");
        } finally {
            releaseB1.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void accept_from_inside_the_drains_own_delivery_is_refused_rather_than_waiting_for_the_drain_it_holds_up() {
        List<String> delivered = new ArrayList<>();
        AtomicReference<BlockingHandover<String, String>> self = new AtomicReference<>();
        AtomicReference<Throwable> thrownFromTheDelivery = new AtomicReference<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            delivered.add(payload);
            if (payload.equals("B1")) {
                thrownFromTheDelivery.set(catchThrowable(() -> self.get().accept("X")));
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        self.set(handover);
        handover.acceptReportingDelivery("B1");

        boolean caughtUp = handover.catchUp(source(List.of("R1"), false));

        assertThat(caughtUp).isTrue();
        assertThat(thrownFromTheDelivery.get()).isInstanceOf(BlockingHandover.PreDispatchRefusalException.class)
                .hasMessage(HandoverMessages.acceptedFromOwnDelivery(NOUN));
        assertThat(delivered).as("X was refused, so it is not delivered").containsExactly("R1", "B1");
    }

    @Test
    void a_catch_up_asked_for_from_inside_the_drains_own_delivery_replays_once_the_drain_has_ended() {
        List<String> log = new ArrayList<>();
        List<Boolean> answers = new ArrayList<>();
        AtomicReference<BlockingHandover<String, String>> self = new AtomicReference<>();
        FakeSource asked = source(List.of("R2"), false);
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            log.add(payload);
            if (payload.equals("B1")) {
                answers.add(self.get().catchUp(asked));
                log.add("asked from B1");
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        self.set(handover);
        handover.acceptReportingDelivery("B1");
        handover.acceptReportingDelivery("B2");

        assertThat(handover.catchUp(source(List.of("R1"), false))).isTrue();

        assertThat(answers).as("the ask is answered at once").containsExactly(true);
        assertThat(log).as("the asked replay runs after the drain delivered everything it took").containsExactly("R1", "B1", "asked from B1", "B2", "R2");
        assertThat(asked.replayCallCount).isEqualTo(1);
        assertThat(asked.markCaughtUpCallCount()).isEqualTo(1);
        assertThat(handover.isReadyForLiveDelivery()).isTrue();
    }

    @Test
    void a_stop_on_the_path_of_a_catch_up_with_nothing_to_replay_returns_false_and_keeps_the_buffer_for_the_next_catch_up() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String, String> handover = handover(delivered);
        handover.acceptReportingDelivery("B1");
        handover.acceptReportingDelivery("B2");
        handover.acceptReportingDelivery("B3");
        FakeSource stopped = source(List.of(), true);
        stopped.stopAfter(1);

        assertThat(handover.catchUp(stopped)).as("the stopped catch-up").isFalse();

        assertThat(delivered).as("only what it delivered before it was stopped").containsExactly("B1");
        assertThat(handover.isReadyForLiveDelivery()).isFalse();
        assertThat(handover.acceptReportingDelivery("L1")).as("a payload fed after the stop is not buffered").isFalse();
        assertThat(handover.catchUp(source(List.of(), true))).isTrue();
        assertThat(delivered).as("the next catch-up delivers the kept payloads in order, once").containsExactly("B1", "B2", "B3");
        assertThat(handover.isReadyForLiveDelivery()).isTrue();
    }

    @Test
    void a_stop_that_reaches_the_drain_of_a_handover_that_was_live_before_delivers_the_rest_stays_live_and_writes_no_marker() {
        List<String> delivered = new ArrayList<>();
        AtomicReference<BlockingHandover<String, String>> self = new AtomicReference<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            delivered.add(payload);
            if (payload.equals("R1")) {
                self.get().acceptReportingDelivery("B1");
                self.get().acceptReportingDelivery("B2");
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        self.set(handover);
        handover.catchUp(source(List.of(), true));
        FakeSource replaying = source(List.of("R1"), false);
        // The replay asks before R1 and once more after it, so the third answer is the drain's first
        replaying.stopAfter(2);

        assertThat(handover.catchUp(replaying)).as("the stopped catch-up").isFalse();

        assertThat(delivered).as("the drain delivers the rest of what it held").containsExactly("R1", "B1", "B2");
        assertThat(replaying.markCaughtUpCallCount()).as("marker writes").isZero();
        assertThat(handover.isReadyForLiveDelivery()).as("live").isTrue();
        assertThat(handover.acceptIfLive("L1")).isTrue();
    }

    @Test
    void a_replay_stopped_on_a_handover_that_goes_live_calls_replay_abandoned_once_when_the_drain_after_it_throws() {
        AtomicReference<BlockingHandover<String, String>> self = new AtomicReference<>();
        IllegalStateException drainFailure = new IllegalStateException("the drain failed");
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            if (payload.equals("B1")) {
                throw drainFailure;
            }
            if (payload.equals("R1")) {
                self.get().acceptReportingDelivery("B1");
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        self.set(handover);
        handover.catchUp(source(List.of(), true));
        FakeSource replaying = source(List.of("R1", "R2"), false);
        replaying.stopAfter(1);

        Throwable thrown = catchThrowable(() -> handover.catchUp(replaying));

        assertThat(thrown).isSameAs(drainFailure);
        assertThat(replaying.replayAbandonedCallCount).as("replayAbandoned() calls").isEqualTo(1);
        assertThat(replaying.replayCompletedCallCount).isZero();
    }

    // The thread that starts the asked catch-up fails once. The ask is then left for a later call, which starts it.
    @Test
    void a_failed_start_of_the_thread_an_asked_catch_up_runs_on_leaves_later_asked_catch_ups_replaying() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        AtomicReference<BlockingHandover<String, String>> self = new AtomicReference<>();
        CountDownLatch marked = new CountDownLatch(1);
        FakeSource asked = source(List.of("R1"), false);
        asked.onMarkCaughtUp = marked::countDown;
        IllegalStateException startFailure = new IllegalStateException("no thread for you");
        AtomicInteger starts = new AtomicInteger();
        BlockingHandover<String, String> handover = new BlockingHandover<>(payload -> {
            log.add(payload);
            if (payload.equals("L1")) {
                self.get().catchUp(asked);
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN, null, thread -> {
            if (starts.getAndIncrement() == 0) {
                throw startFailure;
            }
            thread.start();
        });
        self.set(handover);
        handover.catchUp(source(List.of(), true));

        Throwable thrown = catchThrowable(() -> handover.acceptIfLive("L1"));

        assertThat(thrown).as("what the acceptIfLive that hit the failed start threw").isSameAs(startFailure);
        assertThat(asked.replayCallCount).as("replays before a later call took the ask over").isZero();

        assertThat(handover.acceptIfLive("L2")).isTrue();

        awaitOrFail(marked);
        awaitReady(handover);
        assertThat(starts).as("thread starts, the failed one and the one that took the ask over").hasValue(2);
        assertThat(asked.replayCallCount).isEqualTo(1);
        assertThat(log).containsExactly("L1", "L2", "R1");
    }

    private static void awaitLatchThroughInterrupts(CountDownLatch latch) {
        boolean interrupted = false;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        try {
            while (latch.getCount() > 0 && System.nanoTime() < deadline) {
                try {
                    latch.await(100, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void assertStillWaiting(Future<?> future, String what) {
        try {
            Object returned = future.get(300, TimeUnit.MILLISECONDS);
            throw new AssertionError(what + " returned " + returned + " although what it waits for was still running, so it did not wait for it");
        } catch (TimeoutException stillWaiting) {
            // Still waiting is what is expected
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(what + ": interrupted while waiting", e);
        } catch (ExecutionException e) {
            throw new AssertionError(what + " threw " + e.getCause() + " although what it waits for was still running, so it did not wait for it", e.getCause());
        }
    }

    // A drain of two buffered payloads that holds the fold of the first until released.
    private static final class HeldDrain {
        private final List<String> log = new CopyOnWriteArrayList<>();
        private final CountDownLatch deliveringB1 = new CountDownLatch(1);
        private final CountDownLatch releaseB1 = new CountDownLatch(1);
        private final ExecutorService executor = Executors.newFixedThreadPool(3);
        private final BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            if (payload.equals("B1")) {
                deliveringB1.countDown();
                awaitLatch(releaseB1);
            }
            log.add(payload);
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);

        private Future<Boolean> startDrainHeldAtB1() {
            handover.acceptReportingDelivery("B1");
            handover.acceptReportingDelivery("B2");
            Future<Boolean> catchUp = executor.submit(() -> handover.catchUp(source(List.of(), true)));
            awaitLatch(deliveringB1);
            return catchUp;
        }

        private void close() {
            releaseB1.countDown();
            executor.shutdownNow();
        }
    }

    // The drain after each kind of replay or catch-up holds the handover not live until it has delivered what the
    // buffer holds, and a payload fed from outside the handover's own code meanwhile waits for it to end. That wait is
    // what lets the drain find the buffer empty however fast payloads come. Each test below holds one kind of drain
    // inside a delivery and checks that an outside offer neither returns nor reaches deliver until the drain is released.

    @Test
    void an_outside_offer_waits_for_the_drain_of_a_catch_up_with_nothing_to_replay_and_a_catch_up_from_its_fold_starts_no_second_drain() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch deliveringB1 = new CountDownLatch(1);
        CountDownLatch releaseB1 = new CountDownLatch(1);
        AtomicReference<BlockingHandover<String, String>> self = new AtomicReference<>();
        AtomicInteger secondDrains = new AtomicInteger();
        FakeSource fromTheFold = source(List.of(), true);
        fromTheFold.onHistoryDone = secondDrains::incrementAndGet;
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            if (payload.equals("B1")) {
                deliveringB1.countDown();
                try {
                    self.get().catchUp(fromTheFold);
                } catch (RuntimeException ignored) {
                    // What the catch-up from the fold answers is not what this test is about, only that no second drain starts.
                }
                awaitLatch(releaseB1);
            }
            log.add(payload);
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        self.set(handover);
        handover.acceptReportingDelivery("B1");
        handover.acceptReportingDelivery("B2");
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> catchUp = executor.submit(() -> handover.catchUp(source(List.of(), true)));
            awaitLatch(deliveringB1);

            Future<Boolean> fed = executor.submit(() -> handover.acceptReportingDelivery("L1"));

            assertStillWaiting(fed, "L1 fed from outside while the drain delivers B1");
            assertThat(log).as("delivered while the drain holds B1").isEmpty();
            releaseB1.countDown();
            assertThat(catchUp).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(fed).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(log).containsExactly("B1", "B2", "L1");
            assertThat(secondDrains).as("drains the catch-up from the fold started").hasValue(0);
            assertThat(handover.isReadyForLiveDelivery()).isTrue();
        } finally {
            releaseB1.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void an_outside_offer_waits_for_the_drain_an_interrupted_catch_up_runs_on_a_handover_that_was_live() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch deliveringL1 = new CountDownLatch(1);
        CountDownLatch releaseL1 = new CountDownLatch(1);
        CountDownLatch deliveringL2 = new CountDownLatch(1);
        CountDownLatch releaseL2 = new CountDownLatch(1);
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            if (payload.equals("L1")) {
                deliveringL1.countDown();
                awaitLatch(releaseL1);
            }
            if (payload.equals("L2")) {
                deliveringL2.countDown();
                // The thread that folds L2 is the interrupted one, and the interrupt must not end the hold.
                awaitLatchThroughInterrupts(releaseL2);
            }
            log.add(payload);
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handover.catchUp(source(List.of(), true));
        FakeSource replaying = source(List.of("R1"), false);
        Thread delivering = new Thread(() -> handover.accept("L1"), "live-delivery");
        Thread replay = new Thread(() -> handover.catchUp(replaying), "replay");
        ExecutorService outside = Executors.newSingleThreadExecutor();
        try {
            delivering.start();
            awaitLatch(deliveringL1);
            replay.start();
            // Waiting for the live delivery above, which is when a payload goes into the buffer.
            awaitWaiting(replay);
            handover.acceptReportingDelivery("L2");
            replay.interrupt();
            awaitLatch(deliveringL2);

            Future<Boolean> fed = outside.submit(() -> handover.acceptReportingDelivery("L3"));

            assertStillWaiting(fed, "L3 fed from outside while the interrupted catch-up drains L2");
            assertThat(log).as("delivered while the drain holds L2").isEmpty();
            releaseL2.countDown();
            replay.join(5_000);
            assertThat(fed).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            releaseL1.countDown();
            delivering.join(5_000);
            assertThat(log).containsSubsequence("L2", "L3").contains("L1");
        } finally {
            releaseL1.countDown();
            releaseL2.countDown();
            replay.interrupt();
            outside.shutdownNow();
        }
    }

    @Test
    void an_outside_offer_waits_for_the_drain_after_a_stopped_replay_on_a_handover_that_was_live() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch deliveringB1 = new CountDownLatch(1);
        CountDownLatch releaseB1 = new CountDownLatch(1);
        AtomicReference<BlockingHandover<String, String>> self = new AtomicReference<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            if (payload.equals("R1")) {
                self.get().acceptReportingDelivery("B1");
                self.get().acceptReportingDelivery("B2");
            }
            if (payload.equals("B1")) {
                deliveringB1.countDown();
                awaitLatch(releaseB1);
            }
            log.add(payload);
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        self.set(handover);
        handover.catchUp(source(List.of(), true));
        FakeSource replaying = source(List.of("R1"), false);
        // The replay asks before R1 and answers true, and the stop lands in the replay's own ask after R1
        replaying.stopAfter(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> catchUp = executor.submit(() -> handover.catchUp(replaying));
            awaitLatch(deliveringB1);

            Future<Boolean> fed = executor.submit(() -> handover.acceptReportingDelivery("L1"));

            assertStillWaiting(fed, "L1 fed from outside while the drain after the stopped replay delivers B1");
            assertThat(log).as("delivered while the drain holds B1").containsExactly("R1");
            releaseB1.countDown();
            assertThat(catchUp).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(false);
            assertThat(fed).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(log).containsExactly("R1", "B1", "B2", "L1");
            assertThat(replaying.markCaughtUpCallCount()).as("marker writes").isZero();
            assertThat(handover.isReadyForLiveDelivery()).isTrue();
        } finally {
            releaseB1.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void an_outside_offer_waits_for_the_drain_after_a_failed_replay_on_a_handover_that_was_live() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch deliveringB1 = new CountDownLatch(1);
        CountDownLatch releaseB1 = new CountDownLatch(1);
        AtomicReference<BlockingHandover<String, String>> self = new AtomicReference<>();
        IllegalStateException replayFailure = new IllegalStateException("the replay failed");
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            if (payload.equals("R1")) {
                self.get().acceptReportingDelivery("B1");
            }
            if (payload.equals("R2")) {
                throw replayFailure;
            }
            if (payload.equals("B1")) {
                deliveringB1.countDown();
                awaitLatch(releaseB1);
            }
            log.add(payload);
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        self.set(handover);
        handover.catchUp(source(List.of(), true));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> catchUp = executor.submit(() -> handover.catchUp(source(List.of("R1", "R2"), false)));
            awaitLatch(deliveringB1);

            Future<Boolean> fed = executor.submit(() -> handover.acceptReportingDelivery("L1"));

            assertStillWaiting(fed, "L1 fed from outside while the drain after the failed replay delivers B1");
            assertThat(log).as("delivered while the drain holds B1").containsExactly("R1");
            releaseB1.countDown();
            assertThatThrownBy(() -> catchUp.get(5, TimeUnit.SECONDS)).as("the failed catch-up").isInstanceOf(ExecutionException.class).hasCause(replayFailure);
            assertThat(log).as("the drain delivered what the replay buffered before it failed").containsExactly("R1", "B1");
            assertThatThrownBy(() -> fed.get(5, TimeUnit.SECONDS)).as("how the wait of L1 ended").isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(BlockingHandover.PreDispatchRefusalException.class);
            assertThat(log).as("nothing delivered after the drain of the failed replay").containsExactly("R1", "B1");
        } finally {
            releaseB1.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void an_outside_offer_that_waited_for_the_drain_after_a_failed_replay_is_refused_before_the_failed_catch_up_handles_its_failure() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch deliveringB1 = new CountDownLatch(1);
        CountDownLatch releaseB1 = new CountDownLatch(1);
        CountDownLatch drainEnded = new CountDownLatch(1);
        CountDownLatch releaseCatchUp = new CountDownLatch(1);
        AtomicReference<BlockingHandover<String, String>> self = new AtomicReference<>();
        IllegalStateException replayFailure = new IllegalStateException("the replay failed");
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            if (payload.equals("R1")) {
                self.get().acceptReportingDelivery("B1");
            }
            if (payload.equals("R2")) {
                throw replayFailure;
            }
            if (payload.equals("B1")) {
                deliveringB1.countDown();
                awaitLatch(releaseB1);
            }
            log.add(payload);
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        self.set(handover);
        handover.runAfterDrainOfFailedReplay(() -> {
            drainEnded.countDown();
            awaitLatch(releaseCatchUp);
        });
        handover.catchUp(source(List.of(), true));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> catchUp = executor.submit(() -> handover.catchUp(source(List.of("R1", "R2"), false)));
            awaitLatch(deliveringB1);
            Future<Boolean> fed = executor.submit(() -> handover.acceptReportingDelivery("L1"));
            assertStillWaiting(fed, "L1 fed from outside while the drain after the failed replay delivers B1");

            releaseB1.countDown();
            awaitLatch(drainEnded);

            assertThatThrownBy(() -> fed.get(5, TimeUnit.SECONDS)).as("how the wait of L1 ended while the failed catch-up is held").isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(BlockingHandover.PreDispatchRefusalException.class);
            assertThatThrownBy(() -> handover.acceptIfLive("L2")).as("a live-only offer while the failed catch-up is held").isInstanceOf(BlockingHandover.PreDispatchRefusalException.class);
            assertThat(handover.isReadyForLiveDelivery()).as("ready for live delivery while the failed catch-up is held").isFalse();
            assertThat(log).as("nothing delivered after the drain of the failed replay").containsExactly("R1", "B1");

            releaseCatchUp.countDown();
            assertThatThrownBy(() -> catchUp.get(5, TimeUnit.SECONDS)).as("the failed catch-up").isInstanceOf(ExecutionException.class).hasCause(replayFailure);
            assertThat(log).as("nothing delivered once the failed catch-up ended").containsExactly("R1", "B1");
        } finally {
            releaseB1.countDown();
            releaseCatchUp.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void a_waiting_accept_fed_from_outside_during_the_drain_waits_for_it_and_is_then_applied() throws Exception {
        HeldDrain drain = new HeldDrain();
        try {
            Future<Boolean> catchUp = drain.startDrainHeldAtB1();

            Future<?> accepted = drain.executor.submit(() -> drain.handover.accept("L1"));

            assertStillWaiting(accepted, "L1 accepted from outside while the drain delivers B1");
            assertThat(drain.log).as("delivered while the drain holds B1").isEmpty();
            drain.releaseB1.countDown();
            assertThat(catchUp).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(accepted).succeedsWithin(Duration.ofSeconds(5));
            assertThat(drain.log).containsExactly("B1", "B2", "L1");
        } finally {
            drain.close();
        }
    }

    @Test
    void a_pre_interrupted_offer_without_waiting_returns_during_the_drain_and_its_payload_is_delivered_after_the_earlier_ones() throws Exception {
        HeldDrain drain = new HeldDrain();
        try {
            Future<Boolean> catchUp = drain.startDrainHeldAtB1();

            Future<Boolean> fed = drain.executor.submit(() -> {
                Thread.currentThread().interrupt();
                return drain.handover.acceptReportingDelivery("L1");
            });

            assertThat(fed).as("the pre-interrupted offer, before the drain ended").succeedsWithin(Duration.ofSeconds(2)).isEqualTo(true);
            assertThat(drain.log).as("delivered while the drain holds B1").isEmpty();
            drain.releaseB1.countDown();
            assertThat(catchUp).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(drain.log).containsExactly("B1", "B2", "L1");
        } finally {
            drain.close();
        }
    }

    @Test
    void a_pre_interrupted_waiting_accept_during_the_drain_throws_and_its_payload_is_never_delivered() throws Exception {
        HeldDrain drain = new HeldDrain();
        try {
            Future<Boolean> catchUp = drain.startDrainHeldAtB1();

            Future<?> accepted = drain.executor.submit(() -> {
                Thread.currentThread().interrupt();
                drain.handover.accept("L1");
            });

            assertThatThrownBy(() -> accepted.get(2, TimeUnit.SECONDS)).as("the pre-interrupted accept, before the drain ended")
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(BlockingHandover.PreDispatchRefusalException.class)
                    .hasStackTraceContaining(HandoverMessages.interruptedBeforeApplied(NOUN));
            drain.releaseB1.countDown();
            assertThat(catchUp).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(drain.log).containsExactly("B1", "B2");
        } finally {
            drain.close();
        }
    }

    // A fold of the drain that feeds its own handover waits for nothing, since the drain it would wait for is the code it runs in.
    @Test
    void a_drain_fold_that_feeds_its_own_handover_without_waiting_does_not_wait_for_its_drain_and_the_drain_goes_live() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        AtomicReference<BlockingHandover<String, String>> self = new AtomicReference<>();
        AtomicReference<Boolean> reported = new AtomicReference<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            delivered.add(payload);
            if (payload.equals("B1")) {
                reported.set(self.get().acceptReportingDelivery("F1"));
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        self.set(handover);
        handover.acceptReportingDelivery("B1");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> catchUp = executor.submit(() -> handover.catchUp(source(List.of(), true)));

            assertThat(catchUp).as("the catch-up whose fold feeds its handover").succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(reported).hasValue(true);
            assertThat(delivered).containsExactly("B1", "F1");
            assertThat(handover.isReadyForLiveDelivery()).isTrue();
        } finally {
            executor.shutdownNow();
        }
    }

    // Waiting for the other handover's drain from inside the first one's fold would hold the first drain up behind the second.
    @Test
    void a_drain_fold_that_feeds_a_second_handover_that_is_draining_does_not_wait_for_it_and_the_first_drain_goes_live() throws Exception {
        List<String> firstLog = new CopyOnWriteArrayList<>();
        List<String> secondLog = new CopyOnWriteArrayList<>();
        CountDownLatch secondDelivering = new CountDownLatch(1);
        CountDownLatch releaseSecond = new CountDownLatch(1);
        AtomicReference<Boolean> reported = new AtomicReference<>();
        BlockingHandover<String, String> second = BlockingHandover.create(payload -> {
            if (payload.equals("Y1")) {
                secondDelivering.countDown();
                // Held for longer than any assertion waits, so a fold that waits for this one fails on its own assertion.
                holdQuietly(releaseSecond);
            }
            secondLog.add(payload);
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        BlockingHandover<String, String> first = BlockingHandover.create(payload -> {
            firstLog.add(payload);
            if (payload.equals("X1")) {
                reported.set(second.acceptReportingDelivery("Z1"));
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        second.acceptReportingDelivery("Y1");
        first.acceptReportingDelivery("X1");
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> secondCatchUp = executor.submit(() -> second.catchUp(source(List.of(), true)));
            awaitLatch(secondDelivering);

            Future<Boolean> firstCatchUp = executor.submit(() -> first.catchUp(source(List.of(), true)));

            assertThat(firstCatchUp).as("the first catch-up while the second handover drains").succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(reported).as("what the second handover answered the fold").hasValue(true);
            assertThat(first.isReadyForLiveDelivery()).isTrue();
            assertThat(firstLog).containsExactly("X1");
            assertThat(secondLog).as("delivered by the second handover while its drain holds Y1").isEmpty();
            releaseSecond.countDown();
            assertThat(secondCatchUp).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(secondLog).containsExactly("Y1", "Z1");
        } finally {
            releaseSecond.countDown();
            executor.shutdownNow();
        }
    }

    // Each drain would wait for the other one's drain, which is held up behind the first, so neither can end.
    @Test
    void two_drains_whose_folds_wait_for_each_other_are_both_refused_and_both_go_live() throws Exception {
        List<String> firstLog = new CopyOnWriteArrayList<>();
        List<String> secondLog = new CopyOnWriteArrayList<>();
        CountDownLatch firstDelivering = new CountDownLatch(1);
        CountDownLatch secondDelivering = new CountDownLatch(1);
        CountDownLatch firstAnswered = new CountDownLatch(1);
        CountDownLatch secondAnswered = new CountDownLatch(1);
        AtomicReference<BlockingHandover<String, String>> first = new AtomicReference<>();
        AtomicReference<BlockingHandover<String, String>> second = new AtomicReference<>();
        AtomicReference<Throwable> thrownByFirst = new AtomicReference<>();
        AtomicReference<Throwable> thrownBySecond = new AtomicReference<>();
        first.set(BlockingHandover.create(payload -> {
            firstLog.add(payload);
            if (payload.equals("X1")) {
                firstDelivering.countDown();
                awaitLatch(secondDelivering);
                thrownByFirst.set(catchThrowable(() -> second.get().accept("Z1")));
                firstAnswered.countDown();
                awaitLatch(secondAnswered);
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN));
        second.set(BlockingHandover.create(payload -> {
            secondLog.add(payload);
            if (payload.equals("Y1")) {
                secondDelivering.countDown();
                awaitLatch(firstDelivering);
                thrownBySecond.set(catchThrowable(() -> first.get().accept("W1")));
                secondAnswered.countDown();
                awaitLatch(firstAnswered);
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN));
        first.get().acceptReportingDelivery("X1");
        second.get().acceptReportingDelivery("Y1");
        ExecutorService executor = Executors.newFixedThreadPool(2, runnable -> new Thread(runnable, "reciprocal-drain"));
        try {
            Future<Boolean> firstCatchUp = executor.submit(() -> first.get().catchUp(source(List.of(), true)));
            Future<Boolean> secondCatchUp = executor.submit(() -> second.get().catchUp(source(List.of(), true)));

            assertThat(firstCatchUp).as("the first catch-up while its drain feeds the second handover").succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(secondCatchUp).as("the second catch-up while its drain feeds the first handover").succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(thrownByFirst.get()).as("what the first drain's accept(..) threw").isInstanceOf(BlockingHandover.PreDispatchRefusalException.class)
                    .hasMessage(HandoverMessages.acceptedFromAnotherHandover(NOUN));
            assertThat(thrownBySecond.get()).as("what the second drain's accept(..) threw").isInstanceOf(BlockingHandover.PreDispatchRefusalException.class)
                    .hasMessage(HandoverMessages.acceptedFromAnotherHandover(NOUN));
            assertThat(first.get().isReadyForLiveDelivery()).isTrue();
            assertThat(second.get().isReadyForLiveDelivery()).isTrue();
            assertThat(firstLog).as("Z1 was refused, so it is not delivered").containsExactly("X1");
            assertThat(secondLog).as("W1 was refused, so it is not delivered").containsExactly("Y1");
        } finally {
            executor.shutdownNow();
        }
    }

    // The same wait, from the folds of two replays that are each inside their replay.
    @Test
    void two_replays_whose_folds_wait_for_each_other_are_both_refused_and_both_go_live() throws Exception {
        List<String> firstLog = new CopyOnWriteArrayList<>();
        List<String> secondLog = new CopyOnWriteArrayList<>();
        CountDownLatch firstReplaying = new CountDownLatch(1);
        CountDownLatch secondReplaying = new CountDownLatch(1);
        CountDownLatch firstAnswered = new CountDownLatch(1);
        CountDownLatch secondAnswered = new CountDownLatch(1);
        AtomicReference<BlockingHandover<String, String>> first = new AtomicReference<>();
        AtomicReference<BlockingHandover<String, String>> second = new AtomicReference<>();
        AtomicReference<Throwable> thrownByFirst = new AtomicReference<>();
        AtomicReference<Throwable> thrownBySecond = new AtomicReference<>();
        first.set(BlockingHandover.create(payload -> {
            firstLog.add(payload);
            if (payload.equals("R1")) {
                firstReplaying.countDown();
                awaitLatch(secondReplaying);
                thrownByFirst.set(catchThrowable(() -> second.get().accept("Z1")));
                firstAnswered.countDown();
                awaitLatch(secondAnswered);
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN));
        second.set(BlockingHandover.create(payload -> {
            secondLog.add(payload);
            if (payload.equals("S1")) {
                secondReplaying.countDown();
                awaitLatch(firstReplaying);
                thrownBySecond.set(catchThrowable(() -> first.get().accept("W1")));
                secondAnswered.countDown();
                awaitLatch(firstAnswered);
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN));
        ExecutorService executor = Executors.newFixedThreadPool(2, runnable -> new Thread(runnable, "reciprocal-replay"));
        try {
            Future<Boolean> firstCatchUp = executor.submit(() -> first.get().catchUp(source(List.of("R1"), false)));
            Future<Boolean> secondCatchUp = executor.submit(() -> second.get().catchUp(source(List.of("S1"), false)));

            assertThat(firstCatchUp).as("the first replay while its fold feeds the second handover").succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(secondCatchUp).as("the second replay while its fold feeds the first handover").succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(thrownByFirst.get()).as("what the first replay's accept(..) threw").isInstanceOf(BlockingHandover.PreDispatchRefusalException.class)
                    .hasMessage(HandoverMessages.acceptedFromAnotherHandover(NOUN));
            assertThat(thrownBySecond.get()).as("what the second replay's accept(..) threw").isInstanceOf(BlockingHandover.PreDispatchRefusalException.class)
                    .hasMessage(HandoverMessages.acceptedFromAnotherHandover(NOUN));
            assertThat(first.get().isReadyForLiveDelivery()).isTrue();
            assertThat(second.get().isReadyForLiveDelivery()).isTrue();
            assertThat(firstLog).containsExactly("R1");
            assertThat(secondLog).containsExactly("S1");
        } finally {
            executor.shutdownNow();
        }
    }

    // Each drain would wait for the other one's drain, which is held up behind the first, so neither can end.
    @Test
    void two_drains_whose_folds_catch_up_each_other_with_nothing_to_replay_both_get_true_and_both_go_live() throws Exception {
        List<String> firstLog = new CopyOnWriteArrayList<>();
        List<String> secondLog = new CopyOnWriteArrayList<>();
        CountDownLatch firstDraining = new CountDownLatch(1);
        CountDownLatch secondDraining = new CountDownLatch(1);
        AtomicReference<BlockingHandover<String, String>> first = new AtomicReference<>();
        AtomicReference<BlockingHandover<String, String>> second = new AtomicReference<>();
        AtomicReference<Boolean> answeredToFirst = new AtomicReference<>();
        AtomicReference<Boolean> answeredToSecond = new AtomicReference<>();
        first.set(BlockingHandover.create(payload -> {
            firstLog.add(payload);
            firstDraining.countDown();
            awaitLatch(secondDraining);
            answeredToFirst.set(second.get().catchUp(source(List.of(), true)));
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN));
        second.set(BlockingHandover.create(payload -> {
            secondLog.add(payload);
            secondDraining.countDown();
            awaitLatch(firstDraining);
            answeredToSecond.set(first.get().catchUp(source(List.of(), true)));
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN));
        first.get().acceptReportingDelivery("X1");
        second.get().acceptReportingDelivery("Y1");
        ExecutorService executor = Executors.newFixedThreadPool(2, daemonThreads("reciprocal-drain-catch-up"));
        try {
            Future<Boolean> firstCatchUp = executor.submit(() -> first.get().catchUp(source(List.of(), true)));
            Future<Boolean> secondCatchUp = executor.submit(() -> second.get().catchUp(source(List.of(), true)));

            assertThat(firstCatchUp).as("the first catch-up while its drain fold catches up the second handover").succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(secondCatchUp).as("the second catch-up while its drain fold catches up the first handover").succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(answeredToFirst.get()).as("what the catch-up from the first drain's fold returned").isTrue();
            assertThat(answeredToSecond.get()).as("what the catch-up from the second drain's fold returned").isTrue();
            assertThat(first.get().isReadyForLiveDelivery()).isTrue();
            assertThat(second.get().isReadyForLiveDelivery()).isTrue();
            assertThat(firstLog).containsExactly("X1");
            assertThat(secondLog).containsExactly("Y1");
        } finally {
            executor.shutdownNow();
        }
    }

    // A live fold catches up a draining handover whose drain fold asks the live handover for a replay.
    @Test
    void a_live_fold_that_catches_up_a_draining_handover_with_nothing_to_replay_does_not_wait_for_the_drain_that_asks_it_for_a_replay() throws Exception {
        List<String> drainingLog = new CopyOnWriteArrayList<>();
        List<String> liveLog = new CopyOnWriteArrayList<>();
        CountDownLatch drainFolding = new CountDownLatch(1);
        CountDownLatch liveFolding = new CountDownLatch(1);
        AtomicReference<BlockingHandover<String, String>> draining = new AtomicReference<>();
        AtomicReference<BlockingHandover<String, String>> live = new AtomicReference<>();
        AtomicReference<Boolean> answeredToLiveFold = new AtomicReference<>();
        AtomicReference<Boolean> answeredToDrainFold = new AtomicReference<>();
        draining.set(BlockingHandover.create(payload -> {
            drainingLog.add(payload);
            drainFolding.countDown();
            awaitLatch(liveFolding);
            answeredToDrainFold.set(live.get().catchUp(source(List.of("H1"), false)));
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN));
        live.set(BlockingHandover.create(payload -> {
            liveLog.add(payload);
            if (payload.equals("L1")) {
                liveFolding.countDown();
                awaitLatch(drainFolding);
                answeredToLiveFold.set(draining.get().catchUp(source(List.of(), true)));
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN));
        assertThat(live.get().catchUp(source(List.of(), true))).isTrue();
        draining.get().acceptReportingDelivery("X1");
        ExecutorService executor = Executors.newFixedThreadPool(2, daemonThreads("live-fold-and-drain"));
        try {
            Future<Boolean> drainingCatchUp = executor.submit(() -> draining.get().catchUp(source(List.of(), true)));
            Future<?> liveAccept = executor.submit(() -> live.get().accept("L1"));

            assertThat(liveAccept).as("the live fold while it catches up the draining handover").succeedsWithin(Duration.ofSeconds(5));
            assertThat(drainingCatchUp).as("the drain while its fold asks the live handover for a replay").succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(answeredToLiveFold.get()).as("what the catch-up from the live fold returned").isTrue();
            assertThat(answeredToDrainFold.get()).as("what the replay asked for from the drain's fold returned").isTrue();
            assertThat(draining.get().isReadyForLiveDelivery()).isTrue();
            assertThat(live.get().isReadyForLiveDelivery()).isTrue();
            assertThat(drainingLog).containsExactly("X1");
            assertThat(liveLog).containsExactly("L1", "H1");
        } finally {
            executor.shutdownNow();
        }
    }

    // A handler holding a lock catches up a handover whose drain fold needs that lock.
    @Test
    void a_catch_up_with_nothing_to_replay_from_a_handler_holding_a_lock_returns_true_while_a_drain_that_needs_the_lock_runs() throws Exception {
        List<String> drainingLog = new CopyOnWriteArrayList<>();
        ReentrantLock userLock = new ReentrantLock();
        CountDownLatch holdsLock = new CountDownLatch(1);
        CountDownLatch drainFolding = new CountDownLatch(1);
        CountDownLatch answered = new CountDownLatch(1);
        CountDownLatch releaseLock = new CountDownLatch(1);
        AtomicReference<Boolean> answeredToHandler = new AtomicReference<>();
        BlockingHandover<String, String> draining = BlockingHandover.create(payload -> {
            drainFolding.countDown();
            awaitLatch(holdsLock);
            assertThat(acquiredWithin5Seconds(userLock)).as("the lock the other handler holds").isTrue();
            userLock.unlock();
            drainingLog.add(payload);
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        BlockingHandover<String, String> live = BlockingHandover.create(payload -> {
            holdsLock.countDown();
            awaitLatch(drainFolding);
            answeredToHandler.set(draining.catchUp(source(List.of(), true)));
            answered.countDown();
            awaitLatch(releaseLock);
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        assertThat(live.catchUp(source(List.of(), true))).isTrue();
        draining.acceptReportingDelivery("X1");
        ExecutorService executor = Executors.newFixedThreadPool(2, daemonThreads("lock-holder-and-drain"));
        try {
            Future<Boolean> drainingCatchUp = executor.submit(() -> draining.catchUp(source(List.of(), true)));
            Future<?> holder = executor.submit(() -> {
                userLock.lock();
                try {
                    live.accept("L1");
                } finally {
                    userLock.unlock();
                }
            });

            awaitLatch(answered);
            assertThat(answeredToHandler.get()).as("what the catch-up from the handler holding the lock returned").isTrue();
            assertThat(draining.isReadyForLiveDelivery()).as("the drain still waits for the lock").isFalse();
            assertThat(drainingLog).isEmpty();
            releaseLock.countDown();
            assertThat(holder).succeedsWithin(Duration.ofSeconds(5));
            assertThat(drainingCatchUp).as("the drain once the lock is free").succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(draining.isReadyForLiveDelivery()).isTrue();
            assertThat(drainingLog).containsExactly("X1");
        } finally {
            releaseLock.countDown();
            executor.shutdownNow();
        }
    }

    // A fold of another handover catches up a handover whose replay has ended and whose drain fold waits for that fold.
    @Test
    void a_catch_up_with_nothing_to_replay_from_another_handovers_fold_does_not_wait_for_the_turn_of_a_replay_that_is_draining() throws Exception {
        List<String> replayingLog = new CopyOnWriteArrayList<>();
        CountDownLatch drainFolding = new CountDownLatch(1);
        CountDownLatch callerAnswered = new CountDownLatch(1);
        AtomicReference<Boolean> answeredToCaller = new AtomicReference<>();
        BlockingHandover<String, String> replaying = BlockingHandover.create(payload -> {
            replayingLog.add(payload);
            drainFolding.countDown();
            awaitLatch(callerAnswered);
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        BlockingHandover<String, String> caller = BlockingHandover.create(payload -> {
            awaitLatch(drainFolding);
            answeredToCaller.set(replaying.catchUp(source(List.of(), true)));
            callerAnswered.countDown();
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        assertThat(caller.catchUp(source(List.of(), true))).isTrue();
        replaying.acceptReportingDelivery("X1");
        ExecutorService executor = Executors.newFixedThreadPool(2, daemonThreads("replay-drain-and-caller"));
        try {
            Future<Boolean> replay = executor.submit(() -> replaying.catchUp(source(List.of(), false)));
            Future<?> callerFold = executor.submit(() -> caller.accept("L1"));

            assertThat(callerFold).as("the fold calling the catch-up while the replay drains").succeedsWithin(Duration.ofSeconds(5));
            assertThat(replay).as("the replay once the drain's fold is no longer held up").succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(answeredToCaller.get()).as("what the catch-up from the other handover's fold returned").isTrue();
            assertThat(replaying.isReadyForLiveDelivery()).isTrue();
            assertThat(replayingLog).containsExactly("X1");
        } finally {
            callerAnswered.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void a_drain_fold_that_feeds_a_live_handover_and_waits_has_it_fold_the_payload_on_the_calling_thread() {
        List<String> firstLog = new ArrayList<>();
        List<String> secondLog = new ArrayList<>();
        AtomicReference<List<String>> secondLogWhenAcceptReturned = new AtomicReference<>();
        AtomicReference<Thread> secondFoldedOn = new AtomicReference<>();
        AtomicReference<Thread> firstFoldedOn = new AtomicReference<>();
        BlockingHandover<String, String> second = BlockingHandover.create(payload -> {
            secondLog.add(payload);
            secondFoldedOn.set(Thread.currentThread());
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        assertThat(second.catchUp(source(List.of(), true))).isTrue();
        BlockingHandover<String, String> first = BlockingHandover.create(payload -> {
            firstLog.add(payload);
            if (payload.equals("X1")) {
                firstFoldedOn.set(Thread.currentThread());
                second.accept("Z1");
                secondLogWhenAcceptReturned.set(new ArrayList<>(secondLog));
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        first.acceptReportingDelivery("X1");

        boolean caughtUp = first.catchUp(source(List.of(), true));

        assertThat(caughtUp).isTrue();
        assertThat(secondLogWhenAcceptReturned.get()).as("what the live handover had delivered when accept(..) returned").containsExactly("Z1");
        assertThat(secondFoldedOn.get()).as("the thread that folded Z1").isSameAs(firstFoldedOn.get());
        assertThat(firstLog).containsExactly("X1");
    }

    // The replay's own drain still delivers when it fails, and a catch-up with nothing to replay that comes in meanwhile
    // answers for the replay, so it throws what the replay's drain threw rather than returning that the handover is live.
    @Test
    void a_catch_up_with_nothing_to_replay_waits_for_the_drain_of_a_replay_that_then_fails_and_throws_its_failure() throws Exception {
        CountDownLatch deliveringB1 = new CountDownLatch(1);
        CountDownLatch releaseB1 = new CountDownLatch(1);
        IllegalStateException drainFailure = new IllegalStateException("the drain failed");
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            if (payload.equals("B1")) {
                deliveringB1.countDown();
                awaitLatch(releaseB1);
                throw drainFailure;
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handover.acceptReportingDelivery("B1");
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> replaying = executor.submit(() -> handover.catchUp(source(List.of("R1"), false)));
            awaitLatch(deliveringB1);

            Future<Boolean> caughtUp = executor.submit(() -> handover.catchUp(source(List.of(), true)));

            assertStillWaiting(caughtUp, "the catch-up with nothing to replay while the drain of the replay delivers");
            releaseB1.countDown();
            assertThatThrownBy(() -> replaying.get(5, TimeUnit.SECONDS)).as("the replay whose drain failed").isInstanceOf(ExecutionException.class).hasCause(drainFailure);
            assertThatThrownBy(() -> caughtUp.get(5, TimeUnit.SECONDS)).as("the catch-up that waited for it").isInstanceOf(ExecutionException.class).hasRootCause(drainFailure);
            assertThat(handover.isReadyForLiveDelivery()).isFalse();
        } finally {
            releaseB1.countDown();
            executor.shutdownNow();
        }
    }

    // The drain clears draining when it goes live, and replayRunning ended before it, so while the marker is written only
    // the replay turn is still held. A catch-up with nothing to replay that comes in then answers for the replay, which
    // can still fail there, so it waits for the marker write and throws what that threw.
    @Test
    void a_catch_up_with_nothing_to_replay_waits_for_the_marker_write_of_a_replay_and_throws_when_it_fails() throws Exception {
        CountDownLatch marking = new CountDownLatch(1);
        CountDownLatch releaseMarker = new CountDownLatch(1);
        IllegalStateException markerFailure = new IllegalStateException("the marker write failed");
        FakeSource replaying = source(List.of("R1"), false);
        replaying.onMarkCaughtUp = () -> {
            marking.countDown();
            awaitLatch(releaseMarker);
            throw markerFailure;
        };
        BlockingHandover<String, String> handover = handover(new CopyOnWriteArrayList<>());
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> replay = executor.submit(() -> handover.catchUp(replaying));
            awaitLatch(marking);
            assertThat(handover.isReadyForLiveDelivery()).as("live while the marker is written").isTrue();

            Future<Boolean> caughtUp = executor.submit(() -> handover.catchUp(source(List.of(), true)));

            assertStillWaiting(caughtUp, "the catch-up with nothing to replay while the replay's marker is written");
            releaseMarker.countDown();
            assertThatThrownBy(() -> replay.get(5, TimeUnit.SECONDS)).as("the replay whose marker write failed").isInstanceOf(ExecutionException.class).hasCause(markerFailure);
            assertThatThrownBy(() -> caughtUp.get(5, TimeUnit.SECONDS)).as("the catch-up that waited for the marker write").isInstanceOf(ExecutionException.class).hasRootCause(markerFailure);
        } finally {
            releaseMarker.countDown();
            executor.shutdownNow();
        }
    }

    // A catch-up that comes in as the drain goes live and is interrupted has to find the handover live. It would find it
    // not live, and leave it stopped for the catch-up that is told it went live, if draining outlived the go-live. Whether
    // a call enters in that gap is up to the monitor, so several interrupted calls with a history to replay queue for it all
    // through the drain.
    @Test
    void a_catch_up_that_comes_in_interrupted_as_the_drain_goes_live_leaves_the_handover_live() throws Exception {
        int interruptedCallers = 4;
        ExecutorService threads = Executors.newFixedThreadPool(interruptedCallers);
        try {
            for (int attempt = 0; attempt < 300; attempt++) {
                BlockingHandover<String, String> handover = handover(new CopyOnWriteArrayList<>());
                for (int i = 0; i < 50; i++) {
                    handover.acceptReportingDelivery("B" + i);
                }
                AtomicBoolean marked = new AtomicBoolean();
                CountDownLatch draining = new CountDownLatch(1);
                FakeSource drain = source(List.of(), true);
                drain.onHistoryDone = draining::countDown;
                drain.onMarkCaughtUp = () -> marked.set(true);
                List<Future<?>> callers = new ArrayList<>();
                for (int i = 0; i < interruptedCallers; i++) {
                    callers.add(threads.submit(() -> {
                        awaitLatch(draining);
                        while (!marked.get()) {
                            Thread.currentThread().interrupt();
                            try {
                                handover.catchUp(source(List.of("X"), false));
                            } catch (RuntimeException ignored) {
                                // Only the state the handover ends in is checked.
                            }
                        }
                        Thread.interrupted();
                    }));
                }

                boolean caughtUp = handover.catchUp(drain);
                marked.set(true);
                for (Future<?> caller : callers) {
                    assertThat(caller).succeedsWithin(Duration.ofSeconds(5));
                }

                assertThat(caughtUp).as("the catch-up that drained, attempt " + attempt).isTrue();
                assertThat(handover.isReadyForLiveDelivery()).as("live after the interrupted catch-ups that came in as the drain went live, attempt " + attempt).isTrue();
            }
        } finally {
            threads.shutdownNow();
        }
    }

    // Every case below fills the buffer (cap 2) of a handover that has no replay and no drain yet. A payload fed without
    // waiting that finds it full is in nobody's buffer. The replay that follows reads its event from the store, but a
    // recording source learns of that append only from the drain, so dropping the payload leaves its append unrecorded.
    // The catch-up fails instead, before a replay as during one.

    @Test
    void a_payload_fed_without_waiting_that_finds_the_buffer_full_before_any_replay_is_refused_and_fails_the_replay_that_follows() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(
                delivered::add, payload -> payload, new CatchupThenLiveOptions(CatchupThenLiveOptions.DEFAULT_DEDUP_CACHE_SIZE, 2), NOUN);
        handover.acceptReportingDelivery("L1");
        handover.acceptReportingDelivery("L2");

        Throwable fed = catchThrowable(() -> handover.acceptReportingDelivery("L3"));
        FakeSource replaying = source(List.of("L1", "L2", "L3"), false);
        Throwable caughtUp = catchThrowable(() -> handover.catchUp(replaying));

        assertSoftly(softly -> {
            softly.assertThat(fed).as("what feeding L3 to the full buffer threw").isInstanceOf(BlockingHandover.PreDispatchRefusalException.class).hasMessage(HandoverMessages.bufferOverflow(2));
            softly.assertThat(caughtUp).as("what the replay of the whole history threw").isNotNull().hasRootCauseMessage(HandoverMessages.bufferOverflow(2));
            softly.assertThat(replaying.markCaughtUpCallCount()).as("marker writes").isZero();
            softly.assertThat(handover.isReadyForLiveDelivery()).isFalse();
            softly.assertThat(catchThrowable(() -> handover.acceptReportingDelivery("L4"))).as("what feeding a later payload threw").isInstanceOf(BlockingHandover.PreDispatchRefusalException.class);
        });
    }

    @Test
    void a_payload_fed_without_waiting_that_finds_the_buffer_full_before_any_replay_is_refused_and_fails_a_catch_up_that_goes_live_without_replaying() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(
                delivered::add, payload -> payload, new CatchupThenLiveOptions(CatchupThenLiveOptions.DEFAULT_DEDUP_CACHE_SIZE, 2), NOUN);
        handover.acceptReportingDelivery("L1");
        handover.acceptReportingDelivery("L2");
        Throwable fed = catchThrowable(() -> handover.acceptReportingDelivery("L3"));
        FakeSource caughtUp = source(List.of(), true);

        Throwable thrown = catchThrowable(() -> handover.catchUp(caughtUp));

        assertSoftly(softly -> {
            softly.assertThat(fed).as("what feeding L3 to the full buffer threw").isInstanceOf(BlockingHandover.PreDispatchRefusalException.class).hasMessage(HandoverMessages.bufferOverflow(2));
            softly.assertThat(thrown).as("what the catch-up that reads no history threw, since L3 is in nobody's buffer").isNotNull().hasRootCauseMessage(HandoverMessages.bufferOverflow(2));
            softly.assertThat(caughtUp.markCaughtUpCallCount()).as("marker writes").isZero();
            softly.assertThat(handover.isReadyForLiveDelivery()).isFalse();
            softly.assertThat(catchThrowable(() -> handover.acceptReportingDelivery("L4"))).as("what feeding a later payload threw").isInstanceOf(BlockingHandover.PreDispatchRefusalException.class);
        });
    }

    @Test
    void a_waiting_accept_that_finds_the_buffer_full_before_any_replay_is_refused_alone_and_the_replay_that_follows_goes_live() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(
                delivered::add, payload -> payload, new CatchupThenLiveOptions(CatchupThenLiveOptions.DEFAULT_DEDUP_CACHE_SIZE, 2), NOUN);
        handover.acceptReportingDelivery("L1");
        handover.acceptReportingDelivery("L2");

        Throwable refused = catchThrowable(() -> handover.accept("L3"));
        FakeSource replaying = source(List.of("R1"), false);
        Throwable caughtUp = catchThrowable(() -> handover.catchUp(replaying));

        assertThat(refused).as("what accepting L3 threw").isInstanceOf(BlockingHandover.PreDispatchRefusalException.class).hasMessage(HandoverMessages.bufferOverflow(2));
        assertThat(caughtUp).as("what the catch-up threw for a payload that was refused to its own caller").isNull();
        assertThat(delivered).containsExactly("R1", "L1", "L2");
        assertThat(handover.isReadyForLiveDelivery()).isTrue();
    }

    // A buffer that overflows mid-replay means a payload fed without waiting is in no buffer and may be past what the replay
    // has read, so the replay stops at the next payload rather than folding a history it cannot go live after.
    @Test
    void an_overflow_during_a_replay_abandons_it_before_the_next_payload_is_folded() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch foldingR1 = new CountDownLatch(1);
        CountDownLatch releaseR1 = new CountDownLatch(1);
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            if (payload.equals("R1")) {
                foldingR1.countDown();
                awaitLatch(releaseR1);
            }
            delivered.add(payload);
        }, payload -> payload, new CatchupThenLiveOptions(CatchupThenLiveOptions.DEFAULT_DEDUP_CACHE_SIZE, 2), NOUN);
        FakeSource replaying = source(List.of("R1", "R2", "R3", "R4"), false);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> catchUp = executor.submit(() -> handover.catchUp(replaying));
            awaitLatch(foldingR1);
            handover.acceptReportingDelivery("L1");
            handover.acceptReportingDelivery("L2");
            Throwable overflow = catchThrowable(() -> handover.acceptReportingDelivery("L3"));
            releaseR1.countDown();

            Throwable caughtUp = catchThrowable(() -> catchUp.get(5, TimeUnit.SECONDS));
            assertSoftly(softly -> {
                softly.assertThat(overflow).as("what the payload that found the buffer full threw").isInstanceOf(IllegalStateException.class).hasMessage(HandoverMessages.bufferOverflow(2));
                softly.assertThat(caughtUp).as("the catch-up that overflowed").isInstanceOf(ExecutionException.class).hasRootCauseMessage(HandoverMessages.bufferOverflow(2));
                softly.assertThat(delivered).as("folded before the replay noticed the overflow").containsExactly("R1");
                softly.assertThat(replaying.replayAbandonedCallCount).as("replayAbandoned() calls").isEqualTo(1);
                softly.assertThat(replaying.replayCompletedCallCount).as("replayCompleted() calls").isZero();
                softly.assertThat(replaying.markCaughtUpCallCount()).as("marker writes").isZero();
            });
        } finally {
            releaseR1.countDown();
            executor.shutdownNow();
        }
    }

    // The unanswered payloads of a round that a stop interrupts go back ahead of what was fed meanwhile, so the next
    // catch-up delivers everything in feed order.
    @Test
    void a_stop_in_the_middle_of_a_round_keeps_the_payloads_it_did_not_deliver_ahead_of_one_a_handler_fed_meanwhile() {
        List<String> delivered = new ArrayList<>();
        AtomicReference<BlockingHandover<String, String>> self = new AtomicReference<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            delivered.add(payload);
            if (payload.equals("B1")) {
                self.get().acceptReportingDelivery("F1");
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        self.set(handover);
        handover.acceptReportingDelivery("B1");
        handover.acceptReportingDelivery("B2");
        handover.acceptReportingDelivery("B3");
        FakeSource stopped = source(List.of(), true);
        stopped.stopAfter(1);

        boolean stoppedCatchUp = handover.catchUp(stopped);
        List<String> deliveredBeforeStop = List.copyOf(delivered);
        boolean nextCatchUp = handover.catchUp(source(List.of(), true));

        assertSoftly(softly -> {
            softly.assertThat(stoppedCatchUp).as("the stopped catch-up").isFalse();
            softly.assertThat(deliveredBeforeStop).as("only what it delivered before it was stopped").containsExactly("B1");
            softly.assertThat(nextCatchUp).as("the next catch-up").isTrue();
            softly.assertThat(delivered).as("the next catch-up delivers what was kept in feed order, once").containsExactly("B1", "B2", "B3", "F1");
        });
    }

    @Test
    void a_stop_of_a_catch_up_with_nothing_to_replay_on_a_handover_that_was_live_leaves_it_live() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String, String> handover = handover(delivered);
        handover.catchUp(source(List.of(), true));
        FakeSource stopped = source(List.of(), true);
        stopped.stopAfter(0);

        boolean stoppedCatchUp = handover.catchUp(stopped);
        boolean liveAccepted = handover.acceptIfLive("L1");

        assertSoftly(softly -> {
            softly.assertThat(stoppedCatchUp).as("the stopped catch-up").isFalse();
            softly.assertThat(stopped.markCaughtUpCallCount()).as("marker writes").isZero();
            softly.assertThat(handover.isReadyForLiveDelivery()).as("live").isTrue();
            softly.assertThat(liveAccepted).as("a live payload accepted after it").isTrue();
            softly.assertThat(delivered).containsExactly("L1");
        });
    }

    // When the delivery of the payload and the start of the thread a catch-up it asked for runs on both fail, the
    // caller learns about the delivery, which is what the payload owes, and the failed start is kept beside it.
    @Test
    void a_failed_delivery_that_asked_for_a_catch_up_whose_thread_fails_to_start_throws_the_delivery_failure_with_the_start_failure_suppressed() {
        AtomicReference<BlockingHandover<String, String>> self = new AtomicReference<>();
        FakeSource asked = source(List.of("R1"), false);
        IllegalStateException deliveryFailure = new IllegalStateException("the delivery failed");
        IllegalStateException startFailure = new IllegalStateException("no thread for you");
        BlockingHandover<String, String> handover = new BlockingHandover<>(payload -> {
            self.get().catchUp(asked);
            throw deliveryFailure;
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN, null, thread -> {
            throw startFailure;
        });
        self.set(handover);
        handover.catchUp(source(List.of(), true));

        Throwable thrown = catchThrowable(() -> handover.acceptIfLive("L1"));

        assertThat(thrown).as("what the acceptIfLive whose delivery and thread start failed threw").isSameAs(deliveryFailure);
        assertThat(thrown.getSuppressed()).as("what was suppressed on it").containsExactly(startFailure);
    }

    // A caught-up catch-up from another handover's code that is made right after a stop ended the drain finds no drain
    // left to leave going live to. So when it returns true the handover is live, rather than stopped behind its answer.
    @Test
    void a_catch_up_from_another_handovers_code_after_a_stop_in_the_middle_of_a_round_leaves_the_handover_live_when_it_returns_true() {
        assertThatACatchUpFromAnotherHandoversCodeAfterAStopLeavesTheHandoverLiveWhenItReturnsTrue(List.of("B1", "B2"));
    }

    @Test
    void a_catch_up_from_another_handovers_code_after_a_stop_at_the_empty_buffer_leaves_the_handover_live_when_it_returns_true() {
        assertThatACatchUpFromAnotherHandoversCodeAfterAStopLeavesTheHandoverLiveWhenItReturnsTrue(List.of());
    }

    private static void assertThatACatchUpFromAnotherHandoversCodeAfterAStopLeavesTheHandoverLiveWhenItReturnsTrue(List<String> buffered) {
        List<String> delivered = new CopyOnWriteArrayList<>();
        BlockingHandover<String, String> handover = handover(delivered);
        buffered.forEach(handover::acceptReportingDelivery);
        AtomicReference<Boolean> returnedFromOtherCode = new AtomicReference<>();
        AtomicReference<Throwable> thrownFromOtherCode = new AtomicReference<>();
        AtomicBoolean hookRan = new AtomicBoolean();
        AtomicBoolean callEnded = new AtomicBoolean();
        BlockingHandover<String, String> other = BlockingHandover.create(
                payload -> returnedFromOtherCode.set(handover.catchUp(source(List.of(), true))), payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        other.catchUp(source(List.of(), true));
        handover.runAfterDrainStopped(() -> {
            hookRan.set(true);
            Thread caller = new Thread(() -> thrownFromOtherCode.set(catchThrowable(() -> other.acceptIfLive("trigger"))), "catch-up-from-another-handover");
            caller.setDaemon(true);
            caller.start();
            try {
                caller.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            callEnded.set(!caller.isAlive());
        });
        FakeSource stopping = source(List.of(), true);
        stopping.stopAfter(0);

        handover.catchUp(stopping);

        assertSoftly(softly -> {
            softly.assertThat(hookRan).as("the drain was stopped").isTrue();
            softly.assertThat(callEnded).as("the catch-up from another handover's code ended within the bound").isTrue();
            softly.assertThat(thrownFromOtherCode.get()).as("what the catch-up from another handover's code threw").isNull();
            softly.assertThat(returnedFromOtherCode.get() == Boolean.TRUE && !handover.isReadyForLiveDelivery())
                    .as("the catch-up from another handover's code returned true and left the handover not live").isFalse();
            if (returnedFromOtherCode.get() == Boolean.TRUE) {
                softly.assertThat(delivered).as("every payload the stop put back, delivered once").containsExactlyElementsOf(buffered);
            }
        });
    }

    // A drain that fails records the failure in the lock section that ends it, so a caught-up catch-up from outside
    // every handover that waited for it is refused rather than claiming a drain of its own and returning true. The
    // catch-up that failed records it as the latest failure in a later section, so a waiter that wakes in between
    // sees only the first. Waiters and repetitions make that wake likely, since nothing outside can hold the drain there.
    @Test
    void a_catch_up_that_waited_for_a_drain_that_failed_is_refused_and_the_handover_is_failed() throws Exception {
        for (int repetition = 0; repetition < 50; repetition++) {
            assertThatCatchUpsThatWaitedForADrainThatFailedAreRefused(4);
        }
    }

    private static void assertThatCatchUpsThatWaitedForADrainThatFailedAreRefused(int waiters) throws Exception {
        CountDownLatch deliveringB1 = new CountDownLatch(1);
        CountDownLatch releaseB1 = new CountDownLatch(1);
        IllegalStateException deliveryFailure = new IllegalStateException("the delivery failed");
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            deliveringB1.countDown();
            awaitLatch(releaseB1);
            throw deliveryFailure;
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handover.acceptReportingDelivery("B1");
        List<Boolean> returned = new CopyOnWriteArrayList<>();
        List<Throwable> thrown = new CopyOnWriteArrayList<>();
        List<Thread> waiting = new ArrayList<>();
        for (int i = 0; i < waiters; i++) {
            Thread thread = new Thread(() -> {
                try {
                    returned.add(handover.catchUp(source(List.of(), true)));
                } catch (Throwable e) {
                    thrown.add(e);
                }
            }, "catch-up-waiting-for-the-drain-" + i);
            thread.setDaemon(true);
            waiting.add(thread);
        }
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> drain = executor.submit(() -> handover.catchUp(source(List.of(), true)));
            awaitLatch(deliveringB1);
            for (Thread thread : waiting) {
                thread.start();
            }
            for (Thread thread : waiting) {
                awaitParked(thread);
            }

            releaseB1.countDown();
            for (Thread thread : waiting) {
                thread.join(5_000);
            }
            Throwable drainThrew = catchThrowable(() -> drain.get(5, TimeUnit.SECONDS));

            assertSoftly(softly -> {
                softly.assertThat(waiting).as("catch-ups still waiting for the drain").noneMatch(Thread::isAlive);
                softly.assertThat(returned).as("what the catch-ups that waited for the failed drain returned").isEmpty();
                softly.assertThat(thrown).as("what the catch-ups that waited for the failed drain threw").hasSize(waiters)
                        .allSatisfy(refused -> assertThat(refused).isInstanceOf(BlockingHandover.PreDispatchRefusalException.class));
                softly.assertThat(drainThrew).as("what the drain threw").isInstanceOf(ExecutionException.class).hasCause(deliveryFailure);
                softly.assertThat(handover.refusesPermanently()).as("refuses permanently").isTrue();
                softly.assertThat(handover.isReadyForLiveDelivery()).as("live").isFalse();
            });
        } finally {
            releaseB1.countDown();
            waiting.forEach(Thread::interrupt);
            executor.shutdownNow();
        }
    }

    private static void awaitParked(Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (thread.getState() != Thread.State.WAITING && thread.getState() != Thread.State.TIMED_WAITING) {
            if (!thread.isAlive()) {
                fail(thread.getName() + " ended without waiting");
            }
            if (System.nanoTime() > deadline) {
                fail(thread.getName() + " did not start waiting within 5 seconds, it is " + thread.getState());
            }
            Thread.onSpinWait();
        }
    }

    // A waiting accept(..) parked in the buffer is answered by the overflow a payload fed without waiting causes, since
    // that overflow fails the catch-up the parked payload would have been delivered by.
    @Test
    void a_waiting_accept_parked_in_the_buffer_is_refused_with_the_overflow_a_payload_fed_without_waiting_causes() throws Exception {
        BlockingHandover<String, String> handover = BlockingHandover.create(
                payload -> { }, payload -> payload, new CatchupThenLiveOptions(CatchupThenLiveOptions.DEFAULT_DEDUP_CACHE_SIZE, 2), NOUN);
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        Thread parked = new Thread(() -> thrown.set(catchThrowable(() -> handover.accept("L1"))), "waiting-accept");
        parked.setDaemon(true);
        try {
            parked.start();
            awaitParked(parked);
            handover.acceptReportingDelivery("L2");

            Throwable overflow = catchThrowable(() -> handover.acceptReportingDelivery("L3"));
            parked.join(5_000);

            assertSoftly(softly -> {
                softly.assertThat(overflow).as("what feeding L3 to the full buffer threw").isInstanceOf(BlockingHandover.PreDispatchRefusalException.class).hasMessage(HandoverMessages.bufferOverflow(2));
                softly.assertThat(parked.isAlive()).as("the waiting accept is still parked").isFalse();
                softly.assertThat(thrown.get()).as("what the parked accept threw").isInstanceOf(BlockingHandover.PreDispatchRefusalException.class)
                        .satisfiesAnyOf(
                                refused -> assertThat(refused).hasMessage(HandoverMessages.bufferOverflow(2)),
                                refused -> assertThat(refused).hasRootCauseMessage(HandoverMessages.bufferOverflow(2)));
            });
        } finally {
            parked.interrupt();
        }
    }

    private static void assertThatAFoldFailureIsRecordedAndALaterLivePayloadRefused(Exception foldFailure) {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            if (payload.equals("R2")) {
                sneakyThrow(foldFailure);
            }
            delivered.add(payload);
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);

        Throwable thrownByCatchUp = catchThrowable(() -> handover.catchUp(source(List.of("R1", "R2"), false)));
        Throwable thrownByAccept = catchThrowable(() -> handover.accept("L1"));

        assertThat(thrownByCatchUp).isSameAs(foldFailure);
        assertThat(thrownByAccept).as("a live payload after the failed catch-up")
                .isInstanceOf(BlockingHandover.PreDispatchRefusalException.class)
                .hasMessage(HandoverMessages.catchUpFailed(NOUN))
                .hasCauseReference(foldFailure);
        assertThat(handover.refusesPermanently()).isTrue();
        assertThat(delivered).containsExactly("R1");
    }

    private static void assertThatADrainFailureAfterAFailedReplayLeavesTheFailureRecorded(RuntimeException replayFailure, Exception drainFailure) {
        List<String> log = new ArrayList<>();
        AtomicReference<BlockingHandover<String, String>> self = new AtomicReference<>();
        AtomicBoolean offered = new AtomicBoolean();
        BlockingHandover<String, String> handover = BlockingHandover.create(payload -> {
            if (payload.equals("R2")) {
                throw replayFailure;
            }
            if (payload.equals("L1")) {
                sneakyThrow(drainFailure);
            }
            log.add(payload);
            if (payload.equals("R1") && offered.compareAndSet(false, true)) {
                assertThat(self.get().acceptReportingDelivery("L1")).isTrue();
            }
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        self.set(handover);
        handover.catchUp(source(List.of(), true));

        Throwable thrownByCatchUp = catchThrowable(() -> handover.catchUp(source(List.of("R1", "R2"), false)));
        Throwable thrownByAccept = catchThrowable(() -> handover.accept("L2"));

        assertThat(thrownByAccept).as("a live payload after the failed catch-up")
                .isInstanceOf(BlockingHandover.PreDispatchRefusalException.class)
                .hasCauseReference(replayFailure);
        assertThat(thrownByCatchUp).isSameAs(replayFailure);
        assertThat(handover.refusesPermanently()).isTrue();
        assertThat(log).containsExactly("R1");
    }

    private static void assertThatAThrowingReplayAbandonedDoesNotMaskTheFailure(Exception abandonFailure) {
        BlockingHandover<String, String> handover = handover(new ArrayList<>());
        RuntimeException replayFailure = new RuntimeException("replay boom");
        FakeSource source = source(List.of(), false);
        source.replayFailure = replayFailure;
        source.onReplayAbandoned = () -> sneakyThrow(abandonFailure);

        Throwable thrownByCatchUp = catchThrowable(() -> handover.catchUp(source));
        Throwable thrownByAccept = catchThrowable(() -> handover.accept("L1"));

        assertThat(thrownByAccept).as("a live payload after the failed catch-up")
                .isInstanceOf(BlockingHandover.PreDispatchRefusalException.class)
                .hasCauseReference(replayFailure);
        assertThat(thrownByCatchUp).isSameAs(replayFailure);
    }

    // The drain reports a held payload the replay already applied to the source. A report that fails on an interrupted
    // thread is the interrupt's, so the payload goes back with the rest and the drain of the next catch-up reports it again.
    @Test
    void a_report_of_an_applied_held_payload_that_fails_on_an_interrupt_is_made_again_by_the_drain_of_the_next_catch_up() {
        List<String> delivered = new CopyOnWriteArrayList<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(delivered::add, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handover.acceptReportingDelivery("1");
        handover.acceptReportingDelivery("L2");
        FakeSource replayed = source(List.of("1"), false);
        AtomicBoolean failOnce = new AtomicBoolean(true);
        IllegalStateException reportFailure = new IllegalStateException("report on an interrupted thread");
        replayed.onAlreadyDeliveredByReplay = () -> {
            if (failOnce.getAndSet(false)) {
                Thread.currentThread().interrupt();
                throw reportFailure;
            }
        };

        Throwable thrown = catchThrowable(() -> handover.catchUp(replayed));
        boolean interruptedAfter = Thread.interrupted();
        boolean liveAfterInterrupt = handover.isReadyForLiveDelivery();
        boolean refusesAfterInterrupt = handover.refusesPermanently();
        boolean nextCatchUp = handover.catchUp(source(List.of(), true));

        assertSoftly(softly -> {
            softly.assertThat(thrown).as("what the catch-up whose report failed on the interrupt threw").isSameAs(reportFailure);
            softly.assertThat(interruptedAfter).as("the interrupt stays on the thread").isTrue();
            softly.assertThat(liveAfterInterrupt).as("live after the interrupted drain").isFalse();
            softly.assertThat(refusesAfterInterrupt).as("refuses permanently after the interrupted drain").isFalse();
            softly.assertThat(nextCatchUp).as("the next catch-up").isTrue();
            softly.assertThat(replayed.alreadyDeliveredByReplay).as("reports of the applied payload, the second by the next drain").containsExactly("1", "1");
            softly.assertThat(delivered).as("every payload delivered once").containsExactly("1", "L2");
            softly.assertThat(handover.isReadyForLiveDelivery()).as("live after the next catch-up").isTrue();
        });
    }

    private static void assertThatAThrowingAlreadyDeliveredByReplayReleasesTheDrain(Exception listenerFailure) throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(delivered::add, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handover.acceptReportingDelivery("1");
        handover.acceptReportingDelivery("L2");
        FakeSource failing = source(List.of("1"), false);
        failing.onAlreadyDeliveredByReplay = () -> sneakyThrow(listenerFailure);

        AtomicReference<Boolean> caughtUp = new AtomicReference<>();
        Throwable thrownByCatchUp = catchThrowable(() -> caughtUp.set(handover.catchUp(failing)));
        assertThat(thrownByCatchUp).as("what the catch-up threw when the recording callback threw for one payload").isNull();
        assertThat(caughtUp.get()).as("the catch-up whose recording callback threw for one payload").isTrue();
        assertThat(failing.markCaughtUpCallCount()).as("marker writes").isEqualTo(1);
        assertThat(handover.isReadyForLiveDelivery()).as("live").isTrue();
        assertThat(handover.refusesPermanently()).as("refuses permanently").isFalse();
        assertThat(delivered).as("every other payload is delivered once").containsExactly("1", "L2");

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> later = executor.submit(() -> handover.catchUp(source(List.of(), false)));

            assertThat(later).as("a later catch-up is not left waiting for a reserved payload")
                    .succeedsWithin(Duration.ofSeconds(5));
        } finally {
            executor.shutdownNow();
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(Throwable failure) throws T {
        throw (T) failure;
    }

    private static java.util.concurrent.ThreadFactory daemonThreads(String name) {
        return runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            return thread;
        };
    }

    private static boolean acquiredWithin5Seconds(ReentrantLock lock) {
        try {
            return lock.tryLock(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static BlockingHandover<String, String> handover(List<String> delivered) {
        return BlockingHandover.create(delivered::add, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
    }

    private static FakeSource source(List<String> history, boolean alreadyCaughtUp) {
        return new FakeSource(history, alreadyCaughtUp);
    }

    private static final class FakeSource implements BlockingHandover.Source<String> {
        private final List<String> history;
        private final boolean alreadyCaughtUp;
        private RuntimeException replayFailure;
        private Runnable onMarkCaughtUp;
        private Runnable onReplayStarted;
        private Runnable onReplayCompleted;
        private Runnable onReplayAbandoned;
        private Runnable onAlreadyDeliveredByReplay;
        private Runnable onHistoryDone;
        private Runnable onIsAlreadyCaughtUp;
        // Throws on every store read while the thread is interrupted, the way the Mongo driver does.
        private boolean failWhenInterrupted;
        private int replayCallCount = 0;
        private int markCaughtUpCallCount = 0;
        private int stopAfter = Integer.MAX_VALUE;
        // Overrides stopAfter when set, for a stop that comes from something the test controls
        private BooleanSupplier keepReplayingWhen;
        private int keepReplayingCallCount = 0;
        private int replayStartedCallCount = 0;
        private int replayCompletedCallCount = 0;
        private int replayAbandonedCallCount = 0;
        private final List<String> alreadyDeliveredByReplay = new ArrayList<>();

        @Override
        public void alreadyDeliveredByReplay(String payload) {
            alreadyDeliveredByReplay.add(payload);
            if (onAlreadyDeliveredByReplay != null) {
                onAlreadyDeliveredByReplay.run();
            }
        }

        private void stopAfter(int deliveries) {
            this.stopAfter = deliveries;
        }

        @Override
        public boolean keepReplaying() {
            if (keepReplayingWhen != null) {
                keepReplayingCallCount++;
                return keepReplayingWhen.getAsBoolean();
            }
            return keepReplayingCallCount++ < stopAfter;
        }

        private int markCaughtUpCallCount() {
            return markCaughtUpCallCount;
        }

        private FakeSource(List<String> history, boolean alreadyCaughtUp) {
            this.history = history;
            this.alreadyCaughtUp = alreadyCaughtUp;
        }

        @Override
        public boolean isAlreadyCaughtUp() {
            failIfInterrupted();
            if (onIsAlreadyCaughtUp != null) {
                onIsAlreadyCaughtUp.run();
            }
            return alreadyCaughtUp;
        }

        @Override
        public void historyDone() {
            if (onHistoryDone != null) {
                onHistoryDone.run();
            }
        }

        @Override
        public Stream<String> replay() {
            replayCallCount++;
            if (replayFailure != null) {
                throw replayFailure;
            }
            failIfInterrupted();
            return history.stream().peek(payload -> failIfInterrupted());
        }

        private void failIfInterrupted() {
            if (failWhenInterrupted && Thread.currentThread().isInterrupted()) {
                throw new IllegalStateException(INTERRUPTED_READ);
            }
        }

        @Override
        public void markCaughtUp() {
            failIfInterrupted();
            markCaughtUpCallCount++;
            if (onMarkCaughtUp != null) {
                onMarkCaughtUp.run();
            }
        }

        @Override
        public void replayStarted() {
            replayStartedCallCount++;
            if (onReplayStarted != null) {
                onReplayStarted.run();
            }
        }

        @Override
        public void replayCompleted() {
            replayCompletedCallCount++;
            if (onReplayCompleted != null) {
                onReplayCompleted.run();
            }
        }

        @Override
        public void replayAbandoned() {
            replayAbandonedCallCount++;
            if (onReplayAbandoned != null) {
                onReplayAbandoned.run();
            }
        }
    }
}
