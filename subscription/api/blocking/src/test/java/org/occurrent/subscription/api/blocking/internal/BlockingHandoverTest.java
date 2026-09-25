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
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.internal.HandoverMessages;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.*;

@DisplayNameGeneration(ReplaceUnderscores.class)
class BlockingHandoverTest {

    private static final String NOUN = "thing";

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
    void exceeding_the_max_buffered_events_cap_while_replaying_fails_loud_with_the_documented_message() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(
                delivered::add, payload -> payload, new CatchupThenLiveOptions(CatchupThenLiveOptions.DEFAULT_DEDUP_CACHE_SIZE, 2), NOUN);

        handover.acceptReportingDelivery("L1");
        handover.acceptReportingDelivery("L2");

        Throwable thrown = catchThrowable(() -> handover.acceptReportingDelivery("L3"));

        assertThat(thrown).isInstanceOf(IllegalStateException.class)
                .hasMessage(HandoverMessages.bufferOverflow(2))
                .hasMessageContaining("(cap 2)");
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
                .hasMessage(HandoverMessages.notApplied(NOUN));
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
            awaitWaitingOrDone(accepting);

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
            awaitWaitingOrDone(accepting);
            releaseReplay.countDown();

            assertThat(catchUp.get(5, TimeUnit.SECONDS)).isFalse();
            accepting.join(5_000);
            assertThat(thrownByAccept.get()).as("what accept(..) threw once the replay stopped")
                    .isInstanceOf(BlockingHandover.PreDispatchRefusalException.class)
                    .hasMessage(HandoverMessages.notApplied(NOUN));
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
            awaitWaitingOrDone(accepting);
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
        awaitWaitingOrDone(accepting);

        handover.stopIfNotCatchingUp();

        accepting.join(5_000);
        assertThat(accepting.isAlive()).as("accept(..) released by the stop").isFalse();
        assertThat(thrownByAccept.get()).as("what accept(..) threw once the handover stopped")
                .isInstanceOf(BlockingHandover.PreDispatchRefusalException.class)
                .hasMessage(HandoverMessages.notApplied(NOUN));
        assertThat(delivered).isEmpty();
    }

    @Test
    void an_interrupted_accept_throws_and_leaves_its_payload_out_of_the_drain() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        BlockingHandover<String, String> handover = handover(delivered);
        AtomicReference<Throwable> thrownByAccept = new AtomicReference<>();
        Thread accepting = new Thread(() -> thrownByAccept.set(catchThrowable(() -> handover.accept("L1"))), "live-delivery");
        accepting.start();
        awaitWaitingOrDone(accepting);

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
                .hasMessage(HandoverMessages.acceptedFromOwnReplay(NOUN));
        assertThat(delivered).containsExactly("R1");
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

    // A waiting accept(..) parks in Object.wait(), and one that did not wait has already returned
    private static void awaitWaitingOrDone(Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (thread.isAlive() && thread.getState() != Thread.State.WAITING && System.nanoTime() < deadline) {
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
     * Two catch-ups with nothing to replay can be taking the handover live at the same time. A replay waits for both,
     * so the first to finish does not release it into a view the second is still draining into.
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
        FakeSource first = source(List.of(), true);
        first.onHistoryDone = () -> {
            firstReached.countDown();
            awaitLatch(releaseFirst);
        };
        FakeSource second = source(List.of(), true);
        second.onHistoryDone = () -> {
            secondReached.countDown();
            awaitLatch(releaseSecond);
        };
        CountDownLatch replayCompleted = new CountDownLatch(1);
        FakeSource replaying = source(List.of("R1"), false);
        replaying.onReplayStarted = replayStarted::countDown;
        replaying.onReplayCompleted = replayCompleted::countDown;
        ExecutorService threads = Executors.newFixedThreadPool(3);
        try {
            threads.submit(() -> handover.catchUp(first));
            awaitLatch(firstReached);
            threads.submit(() -> handover.catchUp(second));
            awaitLatch(secondReached);
            threads.submit(() -> handover.catchUp(replaying));
            assertThat(reachedWithin(replayStarted, 300)).isFalse();

            releaseFirst.countDown();

            assertThat(reachedWithin(replayStarted, 300))
                    .as("the second transition is still delivering").isFalse();

            releaseSecond.countDown();

            assertThat(replayStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(replayCompleted.await(5, TimeUnit.SECONDS)).isTrue();
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

        replay.start();
        awaitLatch(foldingR1);
        handover.acceptReportingDelivery("L1");
        assertThatThrownBy(() -> handover.catchUp(failingLookup)).isInstanceOf(IllegalStateException.class);
        handover.catchUp(source(List.of(), true));
        releaseR1.countDown();
        replay.join(5_000);

        assertThat(log).as("the buffered payload waited for the running replay").containsExactly("R1", "R2", "L1");
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

        goLive.start();
        awaitLatch(transitionReached);
        waiting.start();
        assertThat(reachedWithin(replayStarted, 300)).isFalse();
        waiting.interrupt();
        waiting.join(5_000);
        handover.acceptReportingDelivery("L1");
        releaseTransition.countDown();
        goLive.join(5_000);

        assertThat(log).as("the payload was drained by the catch-up that was going live").containsExactly("L1");
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
    void a_checked_exception_from_already_delivered_by_replay_does_not_leave_a_later_catch_up_waiting_for_good() throws Exception {
        assertThatAThrowingAlreadyDeliveredByReplayReleasesTheDrain(new IOException("the listener is down"));
    }

    @Test
    void a_runtime_exception_from_already_delivered_by_replay_does_not_leave_a_later_catch_up_waiting_for_good() throws Exception {
        assertThatAThrowingAlreadyDeliveredByReplayReleasesTheDrain(new IllegalStateException("the listener is down"));
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

    private static void assertThatAThrowingAlreadyDeliveredByReplayReleasesTheDrain(Exception listenerFailure) throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        BlockingHandover<String, String> handover = BlockingHandover.create(delivered::add, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handover.acceptReportingDelivery("1");
        handover.acceptReportingDelivery("L2");
        FakeSource failing = source(List.of("1"), false);
        failing.onAlreadyDeliveredByReplay = () -> sneakyThrow(listenerFailure);
        assertThat(catchThrowable(() -> handover.catchUp(failing))).isSameAs(listenerFailure);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            // A caller asking for a catch-up on a handover that already failed still gets one.
            Future<Boolean> later = executor.submit(() -> handover.catchUp(source(List.of(), false)));

            assertThat(later).as("a later catch-up on the failed handover")
                    .succeedsWithin(Duration.ofSeconds(5));
        } finally {
            executor.shutdownNow();
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(Throwable failure) throws T {
        throw (T) failure;
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
        private int replayCallCount = 0;
        private int markCaughtUpCallCount = 0;
        private int stopAfter = Integer.MAX_VALUE;
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
            return history.stream();
        }

        @Override
        public void markCaughtUp() {
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
