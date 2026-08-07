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
        private int replayCallCount = 0;
        private int markCaughtUpCallCount = 0;
        private int stopAfter = Integer.MAX_VALUE;
        private int keepReplayingCallCount = 0;

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
    }
}
