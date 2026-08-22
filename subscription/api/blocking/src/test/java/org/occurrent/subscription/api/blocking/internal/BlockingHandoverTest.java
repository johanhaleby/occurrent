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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.*;

@DisplayNameGeneration(ReplaceUnderscores.class)
class BlockingHandoverTest {

    private static final String NOUN = "thing";

    @Test
    void live_payloads_accepted_before_catch_up_are_buffered_and_delivered_after_the_replay_in_order() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String> handover = handover(delivered);

        handover.accept("L1");
        handover.accept("L2");

        handover.catchUp(source(List.of("R1", "R2"), false));

        assertThat(delivered).containsExactly("R1", "R2", "L1", "L2");

        handover.accept("L3");
        assertThat(delivered).containsExactly("R1", "R2", "L1", "L2", "L3");
    }

    @Test
    void the_buffer_is_drained_and_the_marker_is_recorded_only_after_every_buffered_live_payload_was_delivered() {
        List<String> log = Collections.synchronizedList(new ArrayList<>());
        BlockingHandover<String> handover = BlockingHandover.create(log::add, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);

        handover.accept("L1");
        FakeSource source = source(List.of("R1"), false);
        source.onMarkCaughtUp = () -> log.add("marker");

        handover.catchUp(source);

        // Load-bearing order for the blocking engine: replay, then the buffered live payload, then the marker.
        assertThat(log).containsExactly("R1", "L1", "marker");
    }

    @Test
    void when_already_caught_up_the_replay_is_skipped_the_marker_is_not_recorded_again_but_buffered_live_payloads_are_still_delivered() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String> handover = handover(delivered);

        handover.accept("L1");
        FakeSource source = source(List.of("R1"), true);

        handover.catchUp(source);

        assertThat(source.replayCallCount).isZero();
        assertThat(source.markCaughtUpCallCount).isZero();
        assertThat(delivered).containsExactly("L1");
    }

    @Test
    void a_payload_already_delivered_by_the_replay_is_not_delivered_again_whether_buffered_or_live() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String> handover = handover(delivered);

        // Buffered before the replay runs, but shares the replay's dedup id.
        handover.accept("1");
        handover.catchUp(source(List.of("1"), false));

        assertThat(delivered).containsExactly("1");

        // A second live copy of the same id, arriving after the engine has gone live, is skipped too.
        handover.accept("1");
        assertThat(delivered).containsExactly("1");
    }

    // The test above only repeats an id the replay already delivered, so nothing covered a repeat that was only ever
    // live. That case is the common one in production, because a push sink acknowledges after the fold, so the broker
    // sends the event again whenever a fold throws. Below, A sent twice in a row is folded once. A, B, C, A folds A
    // twice, because the cache only holds two ids here and B and C pushed A out of it.
    @Test
    void a_live_payload_sent_twice_is_folded_once_until_the_cache_forgets_it() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String> handover = BlockingHandover.create(
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
        BlockingHandover<String> handover = BlockingHandover.create(
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
        BlockingHandover<String> handover = BlockingHandover.create(
                delivered::add, payload -> payload, new CatchupThenLiveOptions(CatchupThenLiveOptions.DEFAULT_DEDUP_CACHE_SIZE, 2), NOUN);

        handover.accept("L1");
        handover.accept("L2");

        Throwable thrown = catchThrowable(() -> handover.accept("L3"));

        assertThat(thrown).isInstanceOf(IllegalStateException.class)
                .hasMessage(HandoverMessages.bufferOverflow(2))
                .hasMessageContaining("(cap 2)");
    }

    @Test
    void a_failed_catch_up_makes_a_subsequent_accept_fail_fast_with_the_original_failure_as_its_cause() {
        BlockingHandover<String> handover = handover(new ArrayList<>());

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
        BlockingHandover<String> handover = handover(new ArrayList<>());

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
        BlockingHandover<String> handover = handover(new ArrayList<>());

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
        BlockingHandover<String> handover = BlockingHandover.create(
                delivered::add, payload -> null, CatchupThenLiveOptions.defaults(), NOUN);

        // Without the guard this reaches BoundedIdCache, whose eviction queue rejects a null element, so it surfaces as
        // a bare NullPointerException from inside the cache after the payload was already folded.
        assertThatThrownBy(() -> handover.catchUp(source(List.of("R1"), false)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(HandoverMessages.dedupKeyRequired());

        BlockingHandover<String> live = BlockingHandover.create(
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
        BlockingHandover<String> handover = BlockingHandover.create(
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
        BlockingHandover<String> handover = BlockingHandover.create(
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

    // acceptIfLive(..): a caller that can redeliver, unlike accept(..)/acceptReportingDelivery(..), which keep
    // buffering when not live (proved unchanged by live_payloads_accepted_before_catch_up_are_buffered_and_delivered_after_the_replay_in_order
    // above, still exercised through accept(..) itself, the write path's only entry point).

    @Test
    void acceptIfLive_refuses_without_buffering_when_not_live() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String> handover = handover(delivered);

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
        BlockingHandover<String> handover = handover(delivered);
        handover.catchUp(source(List.of(), true));

        boolean landed = handover.acceptIfLive("L1");

        assertThat(landed).isTrue();
        assertThat(delivered).containsExactly("L1");
    }

    @Test
    void acceptIfLive_reports_true_for_a_key_an_earlier_attempt_already_delivered() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String> handover = handover(delivered);
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
        BlockingHandover<String> handover = BlockingHandover.create(
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
        BlockingHandover<String> handover = handover(new ArrayList<>());
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
        BlockingHandover<String> handover = handover(delivered);

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
        BlockingHandover<String> handover = handover(delivered);
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
        BlockingHandover<String> handover = handover(delivered);
        FakeSource stopped = source(List.of("R1", "R2"), false);
        stopped.stopAfter(1);
        handover.catchUp(stopped);

        // A failed catch-up throws from here. A stopped one must not, which is what lets a shared feed keep serving
        // its other projections.
        assertThat(catchThrowable(() -> handover.accept("L1"))).isNull();
        // Dropped rather than buffered, since nothing is coming to drain it.
        assertThat(delivered).containsExactly("R1");
    }

    @Test
    void a_later_catch_up_revives_a_handover_a_previous_one_stopped() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String> handover = handover(delivered);
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

    @Test
    void replay_lifecycle_is_started_then_completed_before_the_buffer_drain_and_the_marker() {
        List<String> log = Collections.synchronizedList(new ArrayList<>());
        BlockingHandover<String> handover = BlockingHandover.create(log::add, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handover.accept("L1");
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
        BlockingHandover<String> handover = handover(delivered);
        FakeSource source = source(List.of("R1"), true);

        handover.catchUp(source);

        assertThat(source.replayStartedCallCount).isZero();
        assertThat(source.replayCompletedCallCount).isZero();
        assertThat(source.replayAbandonedCallCount).isZero();
    }

    @Test
    void a_stopped_replay_calls_replay_abandoned_instead_of_replay_completed() {
        List<String> delivered = new ArrayList<>();
        BlockingHandover<String> handover = handover(delivered);
        FakeSource source = source(List.of("R1", "R2", "R3"), false);
        source.stopAfter(2);

        handover.catchUp(source);

        assertThat(source.replayStartedCallCount).isEqualTo(1);
        assertThat(source.replayCompletedCallCount).isZero();
        assertThat(source.replayAbandonedCallCount).isEqualTo(1);
    }

    @Test
    void a_failed_replay_calls_replay_abandoned_before_the_failure_propagates() {
        BlockingHandover<String> handover = handover(new ArrayList<>());
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
        BlockingHandover<String> handover = handover(new ArrayList<>());
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
        BlockingHandover<String> handover = BlockingHandover.create(
                payload -> {
                    if (replayFinished.get()) {
                        throw new RuntimeException("live drain boom");
                    }
                },
                payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);
        handover.accept("L1");
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
        BlockingHandover<String> handover = BlockingHandover.create(payload -> {
            if (payload.equals("L2") && failNext.getAndSet(false)) {
                throw new IllegalStateException("drain boom");
            }
            delivered.add(payload);
        }, payload -> payload, CatchupThenLiveOptions.defaults(), NOUN);

        handover.accept("L1");
        handover.accept("L2");
        handover.accept("L3");

        FakeSource source = source(List.of(), false);
        Throwable thrown = catchThrowable(() -> handover.catchUp(source));
        assertThat(thrown).hasMessage("drain boom");
        assertThat(delivered).as("the drain stopped at the payload that threw").containsExactly("L1");

        // The handover refuses every later payload because the catch-up failed, which is the documented answer, so
        // a second handover stands in for the replacement a caller builds after fixing the cause. What matters is
        // that L3 is not silently skipped, which is what a leaked reservation would cause.
        List<String> redelivered = new ArrayList<>();
        BlockingHandover<String> replacement = handover(redelivered);
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
        BlockingHandover<String> handover = handover(delivered);

        handover.accept("L1");
        handover.accept("L2");
        handover.catchUp(source(List.of(), false));

        handover.accept("L1");
        handover.accept("L2");

        assertThat(delivered).as("each payload was delivered once and the repeats were recognised")
                .containsExactly("L1", "L2");
    }

    private static BlockingHandover<String> handover(List<String> delivered) {
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
        private int replayCallCount = 0;
        private int markCaughtUpCallCount = 0;
        private int stopAfter = Integer.MAX_VALUE;
        private int keepReplayingCallCount = 0;
        private int replayStartedCallCount = 0;
        private int replayCompletedCallCount = 0;
        private int replayAbandonedCallCount = 0;

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
