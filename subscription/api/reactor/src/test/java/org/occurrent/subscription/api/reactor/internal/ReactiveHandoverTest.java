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
import org.junit.jupiter.api.Timeout;
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.internal.HandoverMessages;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class ReactiveHandoverTest {

    @Test
    void live_payloads_accepted_before_catch_up_are_buffered_and_delivered_after_the_replay_in_order() throws Exception {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String> handover = handover(delivered);

        // The replay now runs off-thread (subscribeOn(boundedElastic) in catchUp), so subscribing here only queues
        // these as buffered live payloads. Each one's own Mono is what completes when it has been folded, so
        // capture those before catchUp starts.
        CompletableFuture<Void> l1 = handover.accept("L1").toFuture();
        CompletableFuture<Void> l2 = handover.accept("L2").toFuture();

        handover.catchUp(source(List.of("R1", "R2"), false)).block(Duration.ofSeconds(5));
        // catchUp's Mono completes once the marker is recorded, before the buffered live payloads are folded (see
        // the class javadoc), so wait for L1/L2's own acks rather than assuming they are already delivered.
        l1.get(5, TimeUnit.SECONDS);
        l2.get(5, TimeUnit.SECONDS);

        assertThat(delivered).containsExactly("R1", "R2", "L1", "L2");

        handover.accept("L3").block(Duration.ofSeconds(5));
        assertThat(delivered).containsExactly("R1", "R2", "L1", "L2", "L3");
    }

    @Test
    void the_returned_mono_completes_and_the_marker_is_persisted_before_the_buffered_live_payloads_are_folded() throws Exception {
        List<String> log = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String> handover = ReactiveHandover.create(
                payload -> Mono.fromRunnable(() -> log.add(payload)), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");

        // Captured before catchUp starts, so this registers as a buffered live payload. Its own Mono is what
        // completes when it has been folded.
        CompletableFuture<Void> l1 = handover.accept("L1").toFuture();
        FakeSource source = source(List.of("R1"), false);
        source.onMarkCaughtUp = () -> log.add("marker");

        handover.catchUp(source).block(Duration.ofSeconds(5));
        // The returned Mono completing only proves R1 was folded and the marker recorded - it completes *before* the
        // buffered live payload is folded (see the class javadoc), so wait for L1's own ack before asserting the
        // full order below.
        l1.get(5, TimeUnit.SECONDS);

        // Load-bearing order for the reactor engine, the mirror image of the blocking one: replay, then the marker,
        // then the buffered live payload.
        assertThat(log).containsExactly("R1", "marker", "L1");
    }

    @Test
    void when_already_caught_up_the_replay_is_skipped_the_marker_is_not_recorded_again_but_buffered_live_payloads_are_still_delivered() throws Exception {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String> handover = handover(delivered);

        // Captured before catchUp starts, so this registers as a buffered live payload. Its own Mono is what
        // completes when it has been folded.
        CompletableFuture<Void> l1 = handover.accept("L1").toFuture();
        FakeSource source = source(List.of("R1"), true);

        handover.catchUp(source).block(Duration.ofSeconds(5));
        // replayCallCount/markCaughtUpCallCount are set before catchUp's Mono completes, so block() above already
        // makes them safe to read. The buffered live payload, however, is only folded after that Mono completes, so
        // wait for L1's own ack before asserting it was delivered.
        l1.get(5, TimeUnit.SECONDS);

        assertThat(source.replayCallCount).isZero();
        assertThat(source.markCaughtUpCallCount).isZero();
        assertThat(delivered).containsExactly("L1");
    }

    @Test
    void a_payload_already_delivered_by_the_replay_is_not_delivered_again_whether_buffered_or_live() {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String> handover = handover(delivered);

        // Buffered before the replay runs, but shares the replay's dedup id. Not yet subscribed to a pipeline, so
        // its ack only resolves once catchUp below drains it - just fire it and move on.
        handover.accept("1").subscribe();
        // "1" is added to `delivered` by the replay phase itself, which is guaranteed to have run by the time the
        // returned Mono completes (replay, then marker, then catchupDone) - block() is enough here, unlike the
        // buffered-live-payload cases above.
        handover.catchUp(source(List.of("1"), false)).block(Duration.ofSeconds(5));

        assertThat(delivered).containsExactly("1");

        // A second live copy of the same id, arriving after the engine has gone live, is skipped too, but its ack
        // still completes normally.
        StepVerifier.create(handover.accept("1")).verifyComplete();
        assertThat(delivered).containsExactly("1");
    }

    // The test above only repeats an id the replay already delivered, so nothing covered a repeat that was only ever
    // live. That case is the common one in production, because a push sink acknowledges after the fold, so the broker
    // sends the event again whenever a fold throws. Below, A sent twice in a row is folded once. A, B, C, A folds A
    // twice, because the cache only holds two ids here and B and C pushed A out of it.
    @Test
    void a_live_payload_sent_twice_is_folded_once_until_the_cache_forgets_it() {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String> handover = ReactiveHandover.create(
                payload -> Mono.fromRunnable(() -> delivered.add(payload)), payload -> payload,
                new CatchupThenLiveOptions(2, CatchupThenLiveOptions.DEFAULT_MAX_BUFFERED_EVENTS), "test payload");
        handover.catchUp(source(List.of(), false)).block(Duration.ofSeconds(5));

        // Each accept's Mono completes once its fold has run, so blocking on it waits for exactly that.
        handover.accept("A").block(Duration.ofSeconds(5));
        handover.accept("A").block(Duration.ofSeconds(5));
        assertThat(delivered).containsExactly("A");

        handover.accept("B").block(Duration.ofSeconds(5));
        handover.accept("C").block(Duration.ofSeconds(5));
        handover.accept("A").block(Duration.ofSeconds(5));
        assertThat(delivered).containsExactly("A", "B", "C", "A");
    }

    @Test
    void exceeding_the_max_buffered_events_cap_fails_loud_with_the_documented_message() {
        List<String> delivered = new ArrayList<>();
        ReactiveHandover<String> handover = ReactiveHandover.create(
                payload -> Mono.fromRunnable(() -> delivered.add(payload)), payload -> payload,
                new CatchupThenLiveOptions(CatchupThenLiveOptions.DEFAULT_DEDUP_CACHE_SIZE, 1), "test payload");

        handover.accept("L1").subscribe();

        // Refused before anything is offered to the sink, since the cap counts every payload taken in and not yet
        // delivered, so there is no emit result to report.
        StepVerifier.create(handover.accept("L2"))
                .verifyErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessage(HandoverMessages.bufferOverflow(1))
                        .hasMessageContaining("(cap 1)"));
    }

    /**
     * The live sink comes from the safe spec, so it rejects a second producer offering at the same time rather
     * than corrupting its queue. That rejection used to be reported as a buffer overflow, telling an operator to
     * rebuild a read model offline for what is a moment of contention.
     */
    @Test
    void concurrent_producers_are_never_told_the_buffer_overflowed() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        ReactiveHandover<String> handover = ReactiveHandover.create(
                payload -> Mono.fromRunnable(() -> delivered.add(payload)), payload -> payload,
                CatchupThenLiveOptions.defaults(), "test payload");
        StepVerifier.create(handover.catchUp(source(List.of(), false))).expectNext(true).verifyComplete();

        int producers = 8;
        int perProducer = 40;
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(producers);
        for (int producer = 0; producer < producers; producer++) {
            int id = producer;
            Thread.ofVirtual().start(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perProducer; i++) {
                        handover.accept(id + ":" + i).subscribe(ignored -> {
                        }, failures::add);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();

        assertThat(failures).as("no producer was refused, and none was told the buffer overflowed").isEmpty();

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (delivered.size() < producers * perProducer && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(delivered).as("every payload every producer offered was delivered")
                .hasSize(producers * perProducer);
    }

    /**
     * A producer that loses the serialization race waits on a scheduler rather than on its own thread. The winner
     * drains the sink inline, so its own offer runs the handler and takes as long as the handler does. A loser
     * that retried on its own thread would be held for that whole time too, on a carrier or event-loop thread
     * that has other work.
     */
    @Test
    void a_producer_that_loses_the_serialization_race_does_not_wait_on_its_own_thread() throws Exception {
        CountDownLatch handlerEntered = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        List<String> delivered = new CopyOnWriteArrayList<>();
        ReactiveHandover<String> handover = ReactiveHandover.create(
                payload -> Mono.fromRunnable(() -> {
                    if (payload.equals("winner")) {
                        handlerEntered.countDown();
                        awaitLatchQuietly(releaseHandler);
                    }
                    delivered.add(payload);
                }), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        StepVerifier.create(handover.catchUp(source(List.of(), false))).expectNext(true).verifyComplete();

        Thread winner = Thread.ofVirtual().start(() -> handover.accept("winner").subscribe(ignored -> {
        }, ignored -> {
        }));
        assertThat(handlerEntered.await(5, TimeUnit.SECONDS))
                .as("the first producer is inside the handler, draining the sink on its own thread")
                .isTrue();

        long before = System.nanoTime();
        handover.accept("loser").subscribe(ignored -> {
        }, ignored -> {
        });
        long offerNanos = System.nanoTime() - before;

        releaseHandler.countDown();
        winner.join();

        assertThat(TimeUnit.NANOSECONDS.toMillis(offerNanos))
                .as("the losing offer returned rather than waiting out the winner's handler")
                .isLessThan(500L);

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!delivered.contains("loser") && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(delivered).as("both payloads were delivered").containsExactlyInAnyOrder("winner", "loser");
    }

    /**
     * One caller offering two events in order gets them delivered in that order, even when the first one loses
     * the race for the sink and has to be offered again. Retries that ran independently could reach the sink in
     * either order, which for a caller feeding a position-ordered append means the second event can be applied
     * before the first.
     */
    @Test
    void two_events_from_one_caller_are_delivered_in_the_order_they_were_offered() throws Exception {
        CountDownLatch handlerEntered = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        List<String> delivered = new CopyOnWriteArrayList<>();
        ReactiveHandover<String> handover = ReactiveHandover.create(
                payload -> Mono.fromRunnable(() -> {
                    if (payload.equals("blocker")) {
                        handlerEntered.countDown();
                        awaitLatchQuietly(releaseHandler);
                    }
                    delivered.add(payload);
                }), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        StepVerifier.create(handover.catchUp(source(List.of(), false))).expectNext(true).verifyComplete();

        // Another thread takes the sink and stays in its handler, so both offers below are contended.
        Thread blocker = Thread.ofVirtual().start(() -> handover.accept("blocker").subscribe(ignored -> {
        }, ignored -> {
        }));
        assertThat(handlerEntered.await(5, TimeUnit.SECONDS)).isTrue();

        handover.accept("first").subscribe(ignored -> {
        }, ignored -> {
        });
        handover.accept("second").subscribe(ignored -> {
        }, ignored -> {
        });

        releaseHandler.countDown();
        blocker.join();

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (delivered.size() < 3 && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(delivered)
                .as("the two offers this caller made in order reach the handler in that order")
                .containsExactly("blocker", "first", "second");
    }

    /**
     * A payload offered after a stop lands on a sink whose pipeline has ended. That is the dropped answer, the
     * same one the stop check gives, and it completes false rather than erroring with an overflow it did not have.
     */
    @Test
    void a_payload_offered_once_the_pipeline_has_ended_is_dropped_rather_than_called_an_overflow() {
        List<String> delivered = new CopyOnWriteArrayList<>();
        FakeSource stopped = source(List.of("H1", "H2"), false);
        stopped.stopAfter(0);
        ReactiveHandover<String> handover = ReactiveHandover.create(
                payload -> Mono.fromRunnable(() -> delivered.add(payload)), payload -> payload,
                CatchupThenLiveOptions.defaults(), "test payload");

        StepVerifier.create(handover.catchUp(stopped)).expectNext(false).verifyComplete();

        StepVerifier.create(handover.acceptReportingDelivery("L1"))
                .as("nothing is draining a buffer for it to wait in, so it is dropped rather than refused")
                .expectNext(false)
                .verifyComplete();
        assertThat(delivered).isEmpty();
    }

    /**
     * A live handler that fails is not the engine failing. Its error reaches the caller that offered that payload,
     * through that payload's own acknowledgement, and the engine goes on accepting the next one. Only a catch-up
     * that fails makes the engine refuse for good, which the test below covers.
     */
    @Test
    void a_live_handler_that_fails_does_not_make_the_engine_refuse_permanently() {
        RuntimeException liveFailure = new IllegalStateException("live boom");
        ReactiveHandover<String> handover = ReactiveHandover.create(
                payload -> payload.equals("L1") ? Mono.error(liveFailure) : Mono.empty(), payload -> payload,
                CatchupThenLiveOptions.defaults(), "test payload");

        StepVerifier.create(handover.catchUp(source(List.of(), false))).expectNext(true).verifyComplete();
        assertThat(handover.refusesPermanently()).as("a healthy engine refuses nothing").isFalse();

        // The fold's own error is reported through this payload's own acknowledgement, not through the catch-up.
        StepVerifier.create(handover.acceptReportingDelivery("L1"))
                .verifyErrorSatisfies(error -> assertThat(error).isSameAs(liveFailure));

        assertThat(handover.refusesPermanently())
                .as("a handler that failed is not the engine failing, so it still accepts the next payload")
                .isFalse();
        StepVerifier.create(handover.acceptReportingDelivery("L2")).expectNext(true).verifyComplete();
    }

    /**
     * A catch-up that fails does make the engine refuse permanently, and every later payload is refused with the
     * catch-up-failed message rather than with whatever the fold threw.
     */
    @Test
    void a_failed_catch_up_makes_the_engine_refuse_permanently() {
        FakeSource failing = source(List.of("H1"), false);
        failing.replayFailure = new IllegalStateException("replay boom");
        ReactiveHandover<String> handover = ReactiveHandover.create(
                payload -> Mono.empty(), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");

        StepVerifier.create(handover.catchUp(failing))
                .verifyErrorSatisfies(error -> assertThat(error).hasMessage("replay boom"));

        assertThat(handover.refusesPermanently()).isTrue();
        StepVerifier.create(handover.acceptReportingDelivery("L1"))
                .verifyErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("Catch-up failed"));
    }

    /**
     * Every acknowledgement completes even when offers keep arriving while a drain is running. An offer that
     * arrives then sees a drain already in progress and returns without doing anything itself, so the drain has to
     * look at the queue again after it releases. If it does not, that offer's acknowledgement waits for a caller
     * that is never coming.
     * <p>
     * This covers acknowledgement under contention, not that re-check on its own. Removing the re-check leaves
     * this green, because the window it closes is the few instructions between the drain's last look at an empty
     * queue and it letting go, which no amount of offers reaches reliably.
     */
    @Test
    void an_offer_that_arrives_while_a_drain_is_running_still_gets_its_acknowledgement() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        ReactiveHandover<String> handover = ReactiveHandover.create(
                payload -> Mono.fromRunnable(() -> delivered.add(payload)), payload -> payload,
                CatchupThenLiveOptions.defaults(), "test payload");
        StepVerifier.create(handover.catchUp(source(List.of(), false))).expectNext(true).verifyComplete();

        // Many producers offering at once is what puts offers in the queue while somebody else is draining it,
        // which is the window the acknowledgement can be lost in.
        int producers = 6;
        int perProducer = 200;
        CountDownLatch acknowledged = new CountDownLatch(producers * perProducer);
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        CountDownLatch start = new CountDownLatch(1);
        for (int producer = 0; producer < producers; producer++) {
            int id = producer;
            Thread.ofVirtual().start(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < perProducer; i++) {
                    handover.accept(id + ":" + i).subscribe(ignored -> {
                    }, error -> {
                        failures.add(error);
                        acknowledged.countDown();
                    }, acknowledged::countDown);
                }
            });
        }
        start.countDown();

        assertThat(acknowledged.await(30, TimeUnit.SECONDS))
                .as("every offer was acknowledged, so none was left on the queue with nobody to take it")
                .isTrue();
        assertThat(failures).isEmpty();
        assertThat(delivered).hasSize(producers * perProducer);
    }

    /**
     * A handler that takes its time holds the drain, so every offer that arrives meanwhile waits in the queue in
     * front of the sink rather than in the sink's own queue. The cap counts both, so callers cannot pile up behind
     * a slow handler without limit, and nothing already taken in is lost when a later one is refused.
     */
    @Test
    void offers_waiting_in_front_of_the_sink_count_towards_the_cap() throws Exception {
        int cap = 4;
        CountDownLatch handlerEntered = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        List<String> delivered = new CopyOnWriteArrayList<>();
        ReactiveHandover<String> handover = ReactiveHandover.create(
                payload -> Mono.fromRunnable(() -> {
                    if (payload.equals("slow")) {
                        handlerEntered.countDown();
                        awaitLatchQuietly(releaseHandler);
                    }
                    delivered.add(payload);
                }), payload -> payload,
                new CatchupThenLiveOptions(CatchupThenLiveOptions.DEFAULT_DEDUP_CACHE_SIZE, cap), "test payload");
        StepVerifier.create(handover.catchUp(source(List.of(), false))).expectNext(true).verifyComplete();

        // Holds the drain, so nothing offered below reaches a handler until it is released.
        Thread slow = Thread.ofVirtual().start(() -> handover.accept("slow").subscribe(ignored -> {
        }, ignored -> {
        }));
        assertThat(handlerEntered.await(5, TimeUnit.SECONDS)).isTrue();

        // "slow" already holds one of the cap's places, so three more fit and the fourth is refused.
        List<Throwable> refusals = new CopyOnWriteArrayList<>();
        for (int i = 0; i < cap; i++) {
            handover.accept("queued-" + i).subscribe(ignored -> {
            }, refusals::add);
        }

        assertThat(refusals)
                .as("the cap counts what is waiting in front of the sink as well as what is in it")
                .hasSize(1);
        assertThat(refusals.get(0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(HandoverMessages.bufferOverflow(cap));

        releaseHandler.countDown();
        slow.join();

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (delivered.size() < cap && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(delivered)
                .as("nothing that was taken in was lost by refusing the one that did not fit")
                .containsExactly("slow", "queued-0", "queued-1", "queued-2");

        // The places are given back as the payloads are delivered, so the engine takes offers again.
        StepVerifier.create(handover.acceptReportingDelivery("after")).expectNext(true).verifyComplete();
    }

    /**
     * The drain is over when the payloads taken in while the history was being read have all been handled, and
     * payloads taken in afterwards are live delivery however early they arrive. Counting deliveries alone could
     * not tell the two apart, so a payload taken in after the boundary ended the drain in place of one taken in
     * before it, and a source that frees the subscription on that signal did so with a buffered payload still
     * waiting.
     */
    @Test
    void a_payload_taken_in_after_the_history_was_read_does_not_end_the_drain() throws Exception {
        CountDownLatch firstBufferedEntered = new CountDownLatch(1);
        CountDownLatch releaseFirstBuffered = new CountDownLatch(1);
        List<String> delivered = new CopyOnWriteArrayList<>();
        ReactiveHandover<String> handover = ReactiveHandover.create(
                payload -> Mono.fromRunnable(() -> {
                    if (payload.equals("buffered-1")) {
                        firstBufferedEntered.countDown();
                        awaitLatchQuietly(releaseFirstBuffered);
                    }
                    delivered.add(payload);
                }), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");

        // Two payloads arrive while the history is still being read, so both belong to the drain.
        handover.accept("buffered-1").subscribe(ignored -> {
        }, ignored -> {
        });
        handover.accept("buffered-2").subscribe(ignored -> {
        }, ignored -> {
        });

        List<String> signals = new CopyOnWriteArrayList<>();
        FakeSource source = source(List.of(), false);
        source.onHistoryDone = () -> signals.add("historyDone");
        source.onLiveDrained = () -> signals.add("liveDrained");
        StepVerifier.create(handover.catchUp(source)).expectNext(true).verifyComplete();

        assertThat(firstBufferedEntered.await(5, TimeUnit.SECONDS))
                .as("the drain has started and is handling the first of the two buffered payloads")
                .isTrue();
        assertThat(signals).containsExactly("historyDone");

        // Taken in after the history was read, so this one is live delivery and must not end the drain.
        handover.accept("after-the-boundary").subscribe(ignored -> {
        }, ignored -> {
        });
        Thread.sleep(200);
        assertThat(signals)
                .as("a payload taken in after the boundary cannot end a drain it was never part of")
                .containsExactly("historyDone");

        releaseFirstBuffered.countDown();

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!signals.contains("liveDrained") && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(signals).as("the drain ends once both buffered payloads have been handled")
                .containsExactly("historyDone", "liveDrained");
        assertThat(delivered).startsWith("buffered-1", "buffered-2");
    }

    private static void awaitLatchQuietly(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for the latch");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void a_failed_catch_up_fails_pending_acks_and_later_accept_calls_with_the_cause_attached() {
        ReactiveHandover<String> handover = handover(new ArrayList<>());

        RuntimeException replayFailure = new RuntimeException("replay boom");
        FakeSource source = source(List.of(), false);
        source.replayFailure = replayFailure;

        // Buffered before the catch-up runs, so it is a pending ack when the replay fails. Captured as a future
        // rather than a callback-populated list: the worker thread fails catchupDone and then, as a separate step,
        // fails pendingLiveAcks, so a test thread woken by the former could otherwise read the list before the
        // latter has run. Waiting on L1's own future avoids that race.
        CompletableFuture<Void> l1 = handover.accept("L1").toFuture();

        // The catch-up signal still carries the raw cause: that caller asked about the catch-up itself.
        StepVerifier.create(handover.catchUp(source))
                .verifyErrorMessage("replay boom");

        // The acks do not. They are wrapped in the terminal-refusal message, the same one the blocking engine uses,
        // because a caller feeding live payloads needs to be told this is terminal and what the recovery is, not just
        // what threw during a replay it never saw. Both sides of the failure read the same way.
        assertThatThrownBy(() -> l1.get(5, TimeUnit.SECONDS))
                .cause().isInstanceOf(IllegalStateException.class).hasMessageContaining("Catch-up failed")
                .cause().isSameAs(replayFailure);

        StepVerifier.create(handover.accept("L2"))
                .verifyErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("Catch-up failed")
                        .hasCauseReference(replayFailure));
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
                payload -> Mono.fromRunnable(() -> log.add("fold:" + payload)), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");

        handover.catchUp(source(List.of(), true)).block(Duration.ofSeconds(5));
        // Blocking is the assertion here: accept()'s contract is that its Mono completes only once the fold has run,
        // so waiting for it before logging "ack:L1" is what proves the ordering rather than racing a callback against
        // the test thread.
        handover.accept("L1").block(Duration.ofSeconds(5));
        log.add("ack:L1");

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
        ReactiveHandover<String> handover = ReactiveHandover.create(gatedFold, payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");

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
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        Function<String, Mono<Void>> deliver = payload -> "boom".equals(payload)
                ? Mono.error(new RuntimeException("fold failed"))
                : Mono.fromRunnable(() -> delivered.add(payload));
        ReactiveHandover<String> handover = ReactiveHandover.create(deliver, payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");

        handover.catchUp(source(List.of(), true)).block(Duration.ofSeconds(5));

        StepVerifier.create(handover.accept("boom")).verifyErrorMessage("fold failed");
        StepVerifier.create(handover.accept("L2")).verifyComplete();

        assertThat(delivered).containsExactly("L2");
    }

    @Test
    void a_null_de_dup_key_fails_loud_on_both_the_replay_and_the_live_path() {
        List<String> delivered = new ArrayList<>();
        ReactiveHandover<String> handover = ReactiveHandover.create(
                payload -> Mono.fromRunnable(() -> delivered.add(payload)), payload -> null, CatchupThenLiveOptions.defaults(), "test payload");

        // Without the guard this reaches BoundedIdCache, whose eviction queue rejects a null element, so it surfaces as
        // a bare NullPointerException from inside the cache rather than naming the cause.
        StepVerifier.create(handover.catchUp(source(List.of("R1"), false)))
                .verifyErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessage(HandoverMessages.dedupKeyRequired()));

        ReactiveHandover<String> live = ReactiveHandover.create(
                payload -> Mono.fromRunnable(() -> delivered.add(payload)), payload -> null, CatchupThenLiveOptions.defaults(), "test payload");
        live.catchUp(source(List.of(), true)).block();
        StepVerifier.create(live.accept("L1"))
                .verifyErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessage(HandoverMessages.dedupKeyRequired()));
    }

    // acceptIfLive(..): a caller that can redeliver, unlike accept(..)/acceptReportingDelivery(..), which keep
    // buffering when not live, proved unchanged by acceptReportingDelivery_still_buffers_a_payload_offered_before_catch_up
    // below and by live_payloads_accepted_before_catch_up_are_buffered_and_delivered_after_the_replay_in_order above,
    // still exercised through accept(..) itself, the write path's only entry point.

    @Test
    void acceptReportingDelivery_still_buffers_a_payload_offered_before_catch_up() throws Exception {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String> handover = handover(delivered);

        // Not yet subscribed to a pipeline, so this only proves it was accepted rather than refused; the ack itself
        // resolves once catchUp below drains it.
        CompletableFuture<Boolean> l1 = handover.acceptReportingDelivery("L1").toFuture();

        handover.catchUp(source(List.of("R1"), false)).block(Duration.ofSeconds(5));
        assertThat(l1.get(5, TimeUnit.SECONDS)).as("buffered then genuinely delivered, not dropped").isTrue();

        assertThat(delivered).containsExactly("R1", "L1");
    }

    @Test
    void acceptIfLive_refuses_without_buffering_when_not_live() {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String> handover = handover(delivered);

        StepVerifier.create(handover.acceptIfLive("L1")).expectNext(false).verifyComplete();
        assertThat(delivered).as("refused outright, never buffered").isEmpty();

        // Proof it was truly refused rather than silently buffered. A catch-up that reaches live delivers only the
        // replay's own history, never the refused payload.
        handover.catchUp(source(List.of("R1"), false)).block(Duration.ofSeconds(5));
        assertThat(delivered).containsExactly("R1");
    }

    @Test
    void acceptIfLive_delivers_when_live() {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String> handover = handover(delivered);
        handover.catchUp(source(List.of(), true)).block(Duration.ofSeconds(5));

        StepVerifier.create(handover.acceptIfLive("L1")).expectNext(true).verifyComplete();

        assertThat(delivered).containsExactly("L1");
    }

    @Test
    void acceptIfLive_reports_true_for_a_key_an_earlier_attempt_already_delivered() {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String> handover = handover(delivered);
        handover.catchUp(source(List.of(), true)).block(Duration.ofSeconds(5));

        StepVerifier.create(handover.acceptIfLive("L1")).expectNext(true).verifyComplete();
        StepVerifier.create(handover.acceptIfLive("L1"))
                .as("already delivered, so a redelivery still lands true")
                .expectNext(true).verifyComplete();

        assertThat(delivered).as("folded once, not twice").containsExactly("L1");
    }

    /**
     * The regression guard for the same bug class {@code BlockingHandover.acceptIfLive}'s test of the same name
     * guards. Reading the terminal failure after the live check would let a permanently failed catch-up complete
     * {@code false} (redeliver forever) instead of erroring, turning a real failure into an unbounded
     * bypass-of-every-delivery-failure-policy loop. The terminal failure must be checked, and errored on, before the
     * live check, exactly as {@link ReactiveHandover#acceptReportingDelivery(Object)} already orders it.
     */
    @Test
    void acceptIfLive_errors_rather_than_defers_after_a_catch_up_failure() {
        ReactiveHandover<String> handover = handover(new ArrayList<>());
        RuntimeException replayFailure = new RuntimeException("replay boom");
        FakeSource failingSource = source(List.of(), false);
        failingSource.replayFailure = replayFailure;
        StepVerifier.create(handover.catchUp(failingSource)).verifyErrorMessage("replay boom");

        StepVerifier.create(handover.acceptIfLive("L1"))
                .verifyErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("Catch-up failed")
                        .hasCauseReference(replayFailure));
    }

    // --- helpers ---

    private static ReactiveHandover<String> handover(List<String> delivered) {
        return ReactiveHandover.create(
                payload -> Mono.fromRunnable(() -> delivered.add(payload)), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
    }

    private static FakeSource source(List<String> history, boolean alreadyCaughtUp) {
        return new FakeSource(history, alreadyCaughtUp);
    }

    @Test
    void a_stopped_replay_emits_false_and_records_no_marker() {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String> handover = handover(delivered);
        FakeSource source = source(List.of("R1", "R2", "R3"), false);
        source.stopAfter(2);

        StepVerifier.create(handover.catchUp(source)).expectNext(false).verifyComplete();

        assertThat(delivered).containsExactly("R1", "R2");
        // Recording completion here would make the next catch-up skip a history it never finished folding.
        assertThat(source.markCaughtUpCallCount()).isZero();
    }

    @Test
    void a_stopped_replay_leaves_the_handover_usable_and_completes_live_acks_rather_than_failing_them() {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String> handover = handover(delivered);
        FakeSource stopped = source(List.of("R1", "R2"), false);
        stopped.stopAfter(1);

        StepVerifier.create(handover.catchUp(stopped)).expectNext(false).verifyComplete();

        // A failed catch-up errors this ack. A stopped one completes it: the payload was dropped, not rejected, which
        // is what lets a shared feed keep serving its other projections.
        StepVerifier.create(handover.accept("L1")).verifyComplete();
        assertThat(delivered).containsExactly("R1");
    }

    @Test
    void a_later_catch_up_revives_a_handover_a_previous_one_stopped() {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String> handover = handover(delivered);
        FakeSource stopped = source(List.of("R1", "R2"), false);
        stopped.stopAfter(1);
        StepVerifier.create(handover.catchUp(stopped)).expectNext(false).verifyComplete();

        FakeSource retried = source(List.of("R1", "R2"), false);
        StepVerifier.create(handover.catchUp(retried)).expectNext(true).verifyComplete();

        assertThat(retried.markCaughtUpCallCount()).isEqualTo(1);
        StepVerifier.create(handover.accept("L1")).verifyComplete();
        assertThat(delivered).containsExactly("R1", "R1", "R2", "L1");
    }

    @Test
    void replay_lifecycle_is_started_then_completed_before_the_marker() throws Exception {
        List<String> log = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String> handover = ReactiveHandover.create(
                payload -> Mono.fromRunnable(() -> log.add(payload)), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        FakeSource source = source(List.of("R1"), false);
        source.onReplayStarted = () -> log.add("started");
        source.onReplayCompleted = () -> log.add("completed");
        source.onMarkCaughtUp = () -> log.add("marker");

        handover.catchUp(source).block(Duration.ofSeconds(5));

        assertThat(log).containsExactly("started", "R1", "completed", "marker");
        assertThat(source.replayAbandonedCallCount).isZero();
    }

    @Test
    void replay_lifecycle_methods_are_never_called_when_already_caught_up() throws Exception {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String> handover = handover(delivered);
        FakeSource source = source(List.of("R1"), true);

        handover.catchUp(source).block(Duration.ofSeconds(5));

        assertThat(source.replayStartedCallCount).isZero();
        assertThat(source.replayCompletedCallCount).isZero();
        assertThat(source.replayAbandonedCallCount).isZero();
    }

    @Test
    void a_stopped_replay_calls_replay_abandoned_instead_of_replay_completed() {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String> handover = handover(delivered);
        FakeSource source = source(List.of("R1", "R2", "R3"), false);
        source.stopAfter(2);

        StepVerifier.create(handover.catchUp(source)).expectNext(false).verifyComplete();

        assertThat(source.replayStartedCallCount).isEqualTo(1);
        assertThat(source.replayCompletedCallCount).isZero();
        assertThat(source.replayAbandonedCallCount).isEqualTo(1);
    }

    @Test
    void a_failed_replay_calls_replay_abandoned_before_the_failure_propagates() {
        ReactiveHandover<String> handover = handover(new ArrayList<>());
        RuntimeException replayFailure = new RuntimeException("replay boom");
        FakeSource source = source(List.of(), false);
        source.replayFailure = replayFailure;

        StepVerifier.create(handover.catchUp(source)).verifyErrorMessage("replay boom");

        assertThat(source.replayAbandonedCallCount).isEqualTo(1);
        assertThat(source.replayCompletedCallCount).isZero();
    }

    // A source's replayAbandoned() erroring must not replace the failure that made the engine call it.
    @Test
    void a_replay_abandoned_that_itself_errors_does_not_mask_the_failure_that_triggered_it() {
        ReactiveHandover<String> handover = handover(new ArrayList<>());
        RuntimeException replayFailure = new RuntimeException("replay boom");
        FakeSource source = source(List.of(), false);
        source.replayFailure = replayFailure;
        source.onReplayAbandoned = () -> {
            throw new IllegalStateException("replayAbandoned boom");
        };

        StepVerifier.create(handover.catchUp(source)).verifyErrorMessage("replay boom");
    }

    private static final class FakeSource implements ReactiveHandover.Source<String> {
        private final List<String> history;
        private final boolean alreadyCaughtUp;
        private RuntimeException replayFailure;
        private Runnable onMarkCaughtUp;
        private Runnable onReplayStarted;
        private Runnable onReplayCompleted;
        private Runnable onReplayAbandoned;
        private Runnable onHistoryDone;
        private Runnable onLiveDrained;
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
        public void historyDone() {
            if (onHistoryDone != null) {
                onHistoryDone.run();
            }
        }

        @Override
        public void liveDrained() {
            if (onLiveDrained != null) {
                onLiveDrained.run();
            }
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

        @Override
        public void replayStarted() {
            replayStartedCallCount++;
            if (onReplayStarted != null) {
                onReplayStarted.run();
            }
        }

        @Override
        public Mono<Void> replayCompleted() {
            replayCompletedCallCount++;
            return Mono.fromRunnable(() -> {
                if (onReplayCompleted != null) {
                    onReplayCompleted.run();
                }
            });
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
