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
import org.occurrent.subscription.internal.HandoverMessages;
import org.occurrent.subscription.internal.HandoverOptions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

@DisplayNameGeneration(ReplaceUnderscores.class)
class BlockingHandoverTest {

    private static final String NOUN = "thing";

    @Test
    void live_payloads_accepted_before_catch_up_are_buffered_and_delivered_after_the_replay_in_order() {
        List<String> replayDelivered = new ArrayList<>();
        List<String> liveDelivered = new ArrayList<>();
        BlockingHandover<String, Replayed> handover = handover(replayDelivered, liveDelivered);

        handover.accept("L1");
        handover.accept("L2");

        handover.catchUp(source(List.of(new Replayed("R1"), new Replayed("R2")), false));

        assertThat(replayDelivered).containsExactly("R1", "R2");
        assertThat(liveDelivered).containsExactly("L1", "L2");

        handover.accept("L3");
        assertThat(liveDelivered).containsExactly("L1", "L2", "L3");
    }

    @Test
    void the_buffer_is_drained_and_the_marker_is_recorded_only_after_every_buffered_live_payload_was_delivered() {
        List<String> log = Collections.synchronizedList(new ArrayList<>());
        BlockingHandover<String, Replayed> handover = BlockingHandover.create(
                live -> log.add("live:" + live), live -> live,
                replayed -> log.add("replayed:" + replayed.id()), Replayed::id,
                HandoverOptions.defaults(), NOUN);

        handover.accept("L1");
        FakeSource source = source(List.of(new Replayed("R1")), false);
        source.onMarkCaughtUp = () -> log.add("marker");

        handover.catchUp(source);

        // Load-bearing order for the blocking engine: replay, then the buffered live payload, then the marker.
        assertThat(log).containsExactly("replayed:R1", "live:L1", "marker");
    }

    @Test
    void when_already_caught_up_the_replay_is_skipped_the_marker_is_not_recorded_again_but_buffered_live_payloads_are_still_delivered() {
        List<String> replayDelivered = new ArrayList<>();
        List<String> liveDelivered = new ArrayList<>();
        BlockingHandover<String, Replayed> handover = handover(replayDelivered, liveDelivered);

        handover.accept("L1");
        FakeSource source = source(List.of(new Replayed("R1")), true);

        handover.catchUp(source);

        assertThat(source.replayCallCount).isZero();
        assertThat(source.markCaughtUpCallCount).isZero();
        assertThat(replayDelivered).isEmpty();
        assertThat(liveDelivered).containsExactly("L1");
    }

    @Test
    void a_payload_already_delivered_by_the_replay_is_not_delivered_again_whether_buffered_or_live() {
        List<String> replayDelivered = new ArrayList<>();
        List<String> liveDelivered = new ArrayList<>();
        BlockingHandover<String, Replayed> handover = handover(replayDelivered, liveDelivered);

        // Buffered before the replay runs, but shares the replay's dedup id.
        handover.accept("1");
        handover.catchUp(source(List.of(new Replayed("1")), false));

        assertThat(replayDelivered).containsExactly("1");
        assertThat(liveDelivered).isEmpty();

        // A second live copy of the same id, arriving after the engine has gone live, is skipped too.
        handover.accept("1");
        assertThat(liveDelivered).isEmpty();
    }

    @Test
    void exceeding_the_max_buffered_events_cap_while_replaying_fails_loud_with_the_documented_message() {
        List<String> replayDelivered = new ArrayList<>();
        List<String> liveDelivered = new ArrayList<>();
        BlockingHandover<String, Replayed> handover = BlockingHandover.create(
                liveDelivered::add, live -> live,
                replayed -> replayDelivered.add(replayed.id()), Replayed::id,
                new HandoverOptions(HandoverOptions.DEFAULT_DEDUP_CACHE_SIZE, 2), NOUN);

        handover.accept("L1");
        handover.accept("L2");

        Throwable thrown = catchThrowable(() -> handover.accept("L3"));

        assertThat(thrown).isInstanceOf(IllegalStateException.class)
                .hasMessage(HandoverMessages.bufferOverflow(2))
                .hasMessageContaining("(cap 2)");
    }

    @Test
    void a_failed_catch_up_makes_a_subsequent_accept_fail_fast_with_the_original_failure_as_its_cause() {
        List<String> replayDelivered = new ArrayList<>();
        List<String> liveDelivered = new ArrayList<>();
        BlockingHandover<String, Replayed> handover = handover(replayDelivered, liveDelivered);

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
        List<String> replayDelivered = new ArrayList<>();
        List<String> liveDelivered = new ArrayList<>();
        BlockingHandover<String, Replayed> handover = handover(replayDelivered, liveDelivered);

        assertThatThrownBy(() -> handover.accept(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("payload cannot be null");
        assertThatThrownBy(() -> handover.catchUp(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("source cannot be null");
    }

    // --- helpers ---

    private static BlockingHandover<String, Replayed> handover(List<String> replayDelivered, List<String> liveDelivered) {
        return BlockingHandover.create(
                liveDelivered::add, live -> live,
                replayed -> replayDelivered.add(replayed.id()), Replayed::id,
                HandoverOptions.defaults(), NOUN);
    }

    private static FakeSource source(List<Replayed> history, boolean alreadyCaughtUp) {
        return new FakeSource(history, alreadyCaughtUp);
    }

    private record Replayed(String id) {
    }

    private static final class FakeSource implements BlockingHandover.Source<Replayed> {
        private final List<Replayed> history;
        private final boolean alreadyCaughtUp;
        private RuntimeException replayFailure;
        private Runnable onMarkCaughtUp;
        private int replayCallCount = 0;
        private int markCaughtUpCallCount = 0;

        private FakeSource(List<Replayed> history, boolean alreadyCaughtUp) {
            this.history = history;
            this.alreadyCaughtUp = alreadyCaughtUp;
        }

        @Override
        public boolean isAlreadyCaughtUp() {
            return alreadyCaughtUp;
        }

        @Override
        public Stream<Replayed> replay() {
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
