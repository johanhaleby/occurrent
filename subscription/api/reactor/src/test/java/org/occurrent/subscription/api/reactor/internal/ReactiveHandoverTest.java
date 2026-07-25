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

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.internal.HandoverMessages;
import org.occurrent.subscription.CatchupThenLiveOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactiveHandoverTest {

    @Test
    void live_payloads_accepted_before_catch_up_are_buffered_and_delivered_after_the_replay_in_order() {
        List<String> delivered = new ArrayList<>();
        ReactiveHandover<String> handover = handover(delivered);

        handover.accept("L1").subscribe();
        handover.accept("L2").subscribe();

        handover.catchUp(source(List.of("R1", "R2"), false)).subscribe();

        assertThat(delivered).containsExactly("R1", "R2", "L1", "L2");

        handover.accept("L3").subscribe();
        assertThat(delivered).containsExactly("R1", "R2", "L1", "L2", "L3");
    }

    @Test
    void the_returned_mono_completes_and_the_marker_is_persisted_before_the_buffered_live_payloads_are_folded() {
        List<String> log = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String> handover = ReactiveHandover.create(
                payload -> Mono.fromRunnable(() -> log.add(payload)), payload -> payload, CatchupThenLiveOptions.defaults());

        handover.accept("L1").subscribe();
        FakeSource source = source(List.of("R1"), false);
        source.onMarkCaughtUp = () -> log.add("marker");

        handover.catchUp(source).subscribe();

        // Load-bearing order for the reactor engine, the mirror image of the blocking one: replay, then the marker,
        // then the buffered live payload.
        assertThat(log).containsExactly("R1", "marker", "L1");
    }

    @Test
    void when_already_caught_up_the_replay_is_skipped_the_marker_is_not_recorded_again_but_buffered_live_payloads_are_still_delivered() {
        List<String> delivered = new ArrayList<>();
        ReactiveHandover<String> handover = handover(delivered);

        handover.accept("L1").subscribe();
        FakeSource source = source(List.of("R1"), true);

        handover.catchUp(source).subscribe();

        assertThat(source.replayCallCount).isZero();
        assertThat(source.markCaughtUpCallCount).isZero();
        assertThat(delivered).containsExactly("L1");
    }

    @Test
    void a_payload_already_delivered_by_the_replay_is_not_delivered_again_whether_buffered_or_live() {
        List<String> delivered = new ArrayList<>();
        ReactiveHandover<String> handover = handover(delivered);

        // Buffered before the replay runs, but shares the replay's dedup id. Not yet subscribed to a pipeline, so
        // its ack only resolves once catchUp below drains it - just fire it and move on.
        handover.accept("1").subscribe();
        handover.catchUp(source(List.of("1"), false)).subscribe();

        assertThat(delivered).containsExactly("1");

        // A second live copy of the same id, arriving after the engine has gone live, is skipped too, but its ack
        // still completes normally.
        StepVerifier.create(handover.accept("1")).verifyComplete();
        assertThat(delivered).containsExactly("1");
    }

    @Test
    void exceeding_the_max_buffered_events_cap_fails_loud_with_the_documented_message() {
        List<String> delivered = new ArrayList<>();
        ReactiveHandover<String> handover = ReactiveHandover.create(
                payload -> Mono.fromRunnable(() -> delivered.add(payload)), payload -> payload,
                new CatchupThenLiveOptions(CatchupThenLiveOptions.DEFAULT_DEDUP_CACHE_SIZE, 1));

        handover.accept("L1").subscribe();

        StepVerifier.create(handover.accept("L2"))
                .verifyErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageStartingWith(HandoverMessages.bufferOverflow(1))
                        .hasMessageContaining("(cap 1)")
                        .hasMessageContaining("Emit result:"));
    }

    @Test
    void a_failed_catch_up_fails_pending_acks_and_later_accept_calls_with_the_original_failure() {
        ReactiveHandover<String> handover = handover(new ArrayList<>());

        RuntimeException replayFailure = new RuntimeException("replay boom");
        FakeSource source = source(List.of(), false);
        source.replayFailure = replayFailure;

        // Buffered before the catch-up runs, so it is a pending ack when the replay fails.
        List<Throwable> pendingAckErrors = new ArrayList<>();
        handover.accept("L1").subscribe(v -> {
        }, pendingAckErrors::add);

        StepVerifier.create(handover.catchUp(source))
                .verifyErrorMessage("replay boom");

        assertThat(pendingAckErrors).hasSize(1);
        assertThat(pendingAckErrors.get(0)).isSameAs(replayFailure);

        StepVerifier.create(handover.accept("L2"))
                .verifyErrorSatisfies(error -> assertThat(error).isSameAs(replayFailure));
    }

    @Test
    void accept_and_catch_up_reject_null_arguments_eagerly() {
        ReactiveHandover<String> handover = handover(new ArrayList<>());

        assertThatThrownBy(() -> handover.accept(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("payload cannot be null");
        assertThatThrownBy(() -> handover.catchUp(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("source cannot be null");
    }

    @Test
    void a_live_payloads_accept_mono_completes_only_after_its_fold_has_run() {
        List<String> log = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String> handover = ReactiveHandover.create(
                payload -> Mono.fromRunnable(() -> log.add("fold:" + payload)), payload -> payload, CatchupThenLiveOptions.defaults());

        handover.catchUp(source(List.of(), true)).subscribe();
        handover.accept("L1").subscribe(v -> {
        }, e -> {
        }, () -> log.add("ack:L1"));

        assertThat(log).containsExactly("fold:L1", "ack:L1");
    }

    @Test
    void the_marker_is_recorded_only_after_every_replayed_payload_has_actually_been_folded() throws Exception {
        List<String> log = Collections.synchronizedList(new ArrayList<>());
        // Holds a replayed fold open until the test releases it, so the assertion is about ordering rather than
        // about which thread happens to win. The fold is asynchronous, like the reactor projection DSL's boundedElastic
        // bridge to a blocking repository. A synchronous fold cannot show the defect, because concatMap's inner
        // completes before the replay Flux can signal onComplete.
        // Gates the LAST replayed payload. That is where the defect lives: the replay Flux signals onComplete once its
        // final item is emitted, so concat can advance to the marker while concatMap still has that item to fold.
        CompletableFuture<Void> lastFoldGate = new CompletableFuture<>();
        CountDownLatch lastFoldStarted = new CountDownLatch(1);
        Function<String, Mono<Void>> gatedFold = payload -> {
            Mono<Void> fold = "R2".equals(payload)
                    ? Mono.<Void>fromRunnable(lastFoldStarted::countDown)
                    .then(Mono.fromFuture(lastFoldGate))
                    .then(Mono.<Void>fromRunnable(() -> log.add("folded:R2")))
                    : Mono.<Void>fromRunnable(() -> log.add("folded:" + payload));
            return fold.subscribeOn(Schedulers.boundedElastic());
        };
        ReactiveHandover<String> handover = ReactiveHandover.create(gatedFold, payload -> payload, CatchupThenLiveOptions.defaults());

        FakeSource source = source(List.of("R1", "R2"), false);
        source.onMarkCaughtUp = () -> log.add("marker");

        CountDownLatch caughtUp = new CountDownLatch(1);
        handover.catchUp(source).subscribe(ignored -> {
        }, error -> caughtUp.countDown(), caughtUp::countDown);

        assertThat(lastFoldStarted.await(5, TimeUnit.SECONDS)).isTrue();
        lastFoldGate.complete(null);
        assertThat(caughtUp.await(5, TimeUnit.SECONDS)).isTrue();

        // The marker means "catch-up done", so a restart skips the replay. Recording it while a replayed payload is
        // still unfolded loses that payload for good, with no error anywhere.
        assertThat(log).containsExactly("folded:R1", "folded:R2", "marker");
    }

    @Test
    void a_fold_error_is_routed_to_that_payloads_ack_without_killing_the_pipeline_so_a_later_payload_is_still_delivered() {
        List<String> delivered = new ArrayList<>();
        Function<String, Mono<Void>> deliver = payload -> "boom".equals(payload)
                ? Mono.error(new RuntimeException("fold failed"))
                : Mono.fromRunnable(() -> delivered.add(payload));
        ReactiveHandover<String> handover = ReactiveHandover.create(deliver, payload -> payload, CatchupThenLiveOptions.defaults());

        handover.catchUp(source(List.of(), true)).subscribe();

        StepVerifier.create(handover.accept("boom")).verifyErrorMessage("fold failed");
        StepVerifier.create(handover.accept("L2")).verifyComplete();

        assertThat(delivered).containsExactly("L2");
    }

    // --- helpers ---

    private static ReactiveHandover<String> handover(List<String> delivered) {
        return ReactiveHandover.create(
                payload -> Mono.fromRunnable(() -> delivered.add(payload)), payload -> payload, CatchupThenLiveOptions.defaults());
    }

    private static FakeSource source(List<String> history, boolean alreadyCaughtUp) {
        return new FakeSource(history, alreadyCaughtUp);
    }

    private static final class FakeSource implements ReactiveHandover.Source<String> {
        private final List<String> history;
        private final boolean alreadyCaughtUp;
        private RuntimeException replayFailure;
        private Runnable onMarkCaughtUp;
        private int replayCallCount = 0;
        private int markCaughtUpCallCount = 0;

        private FakeSource(List<String> history, boolean alreadyCaughtUp) {
            this.history = history;
            this.alreadyCaughtUp = alreadyCaughtUp;
        }

        @Override
        public Mono<Boolean> isAlreadyCaughtUp() {
            return Mono.just(alreadyCaughtUp);
        }

        @Override
        public Flux<String> replay() {
            replayCallCount++;
            if (replayFailure != null) {
                return Flux.error(replayFailure);
            }
            return Flux.fromIterable(history);
        }

        @Override
        public Mono<Void> markCaughtUp() {
            markCaughtUpCallCount++;
            return Mono.fromRunnable(() -> {
                if (onMarkCaughtUp != null) {
                    onMarkCaughtUp.run();
                }
            });
        }
    }
}
