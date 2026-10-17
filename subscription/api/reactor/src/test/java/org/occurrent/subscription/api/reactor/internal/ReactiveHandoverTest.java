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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.internal.HandoverMessages;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.Signal;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class ReactiveHandoverTest {

    @Test
    void live_payloads_accepted_before_catch_up_are_buffered_and_delivered_after_the_replay_in_order() throws Exception {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String, String> handover = handover(delivered);

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
        ReactiveHandover<String, String> handover = ReactiveHandover.create(
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
        ReactiveHandover<String, String> handover = handover(delivered);

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
        ReactiveHandover<String, String> handover = handover(delivered);

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

    // The replay applies an event and the live copy of it is suppressed, so on the recording paths neither delivery
    // would write down the append it came from. The suppression tells the source instead, once per suppressed copy,
    // and for both timings, the copy that buffered during the replay and the one that arrived after the drain.
    @Test
    void a_payload_the_replay_delivered_reaches_the_source_when_its_live_copy_is_suppressed() throws Exception {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String, String> handover = handover(delivered);
        FakeSource source = source(List.of("1"), false);

        CompletableFuture<Void> buffered = handover.accept("1").toFuture();
        handover.catchUp(source).block(Duration.ofSeconds(5));
        // The buffered copy is suppressed after the catch-up Mono completes, so its own ack is what says the
        // suppression has run.
        buffered.get(5, TimeUnit.SECONDS);

        assertThat(delivered).containsExactly("1");
        assertThat(source.alreadyDeliveredByReplay).containsExactly("1");

        StepVerifier.create(handover.accept("1")).verifyComplete();

        assertThat(delivered).containsExactly("1");
        assertThat(source.alreadyDeliveredByReplay).containsExactly("1", "1");
    }

    // The negative half, and the one that would still pass with a single cache. A repeat the replay never delivered
    // was already delivered live, and that delivery wrote down whatever it owed, so telling the source again would
    // have it write the same thing twice for one event.
    @Test
    void a_payload_an_earlier_live_delivery_handled_reaches_the_source_again_for_nothing() {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String, String> handover = handover(delivered);
        FakeSource source = source(List.of(), false);
        handover.catchUp(source).block(Duration.ofSeconds(5));

        StepVerifier.create(handover.accept("A")).verifyComplete();
        StepVerifier.create(handover.accept("A")).verifyComplete();

        assertThat(delivered).containsExactly("A");
        assertThat(source.alreadyDeliveredByReplay).isEmpty();
    }

    // The test above only repeats an id the replay already delivered, so nothing covered a repeat that was only ever
    // live. That case is the common one in production, because a push sink acknowledges after the fold, so the broker
    // sends the event again whenever a fold throws. Below, A sent twice in a row is folded once. A, B, C, A folds A
    // twice, because the cache only holds two ids here and B and C pushed A out of it.
    @Test
    void a_live_payload_sent_twice_is_folded_once_until_the_cache_forgets_it() {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String, String> handover = ReactiveHandover.create(
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
        ReactiveHandover<String, String> handover = ReactiveHandover.create(
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
        ReactiveHandover<String, String> handover = ReactiveHandover.create(
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
        ReactiveHandover<String, String> handover = ReactiveHandover.create(
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
        ReactiveHandover<String, String> handover = ReactiveHandover.create(
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
        ReactiveHandover<String, String> handover = ReactiveHandover.create(
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
        ReactiveHandover<String, String> handover = ReactiveHandover.create(
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
        ReactiveHandover<String, String> handover = ReactiveHandover.create(
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
        ReactiveHandover<String, String> handover = ReactiveHandover.create(
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
        ReactiveHandover<String, String> handover = ReactiveHandover.create(
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
        ReactiveHandover<String, String> handover = ReactiveHandover.create(
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
        ReactiveHandover<String, String> handover = handover(new ArrayList<>());

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
        ReactiveHandover<String, String> handover = handover(new ArrayList<>());

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
        ReactiveHandover<String, String> handover = ReactiveHandover.create(
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
        ReactiveHandover<String, String> handover = ReactiveHandover.create(gatedFold, payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");

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
        ReactiveHandover<String, String> handover = ReactiveHandover.create(deliver, payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");

        handover.catchUp(source(List.of(), true)).block(Duration.ofSeconds(5));

        StepVerifier.create(handover.accept("boom")).verifyErrorMessage("fold failed");
        StepVerifier.create(handover.accept("L2")).verifyComplete();

        assertThat(delivered).containsExactly("L2");
    }

    @Test
    void a_null_de_dup_key_fails_loud_on_both_the_replay_and_the_live_path() {
        List<String> delivered = new ArrayList<>();
        ReactiveHandover<String, String> handover = ReactiveHandover.create(
                payload -> Mono.fromRunnable(() -> delivered.add(payload)), payload -> null, CatchupThenLiveOptions.defaults(), "test payload");

        // Without the guard this reaches BoundedIdCache, whose eviction queue rejects a null element, so it surfaces as
        // a bare NullPointerException from inside the cache rather than naming the cause.
        StepVerifier.create(handover.catchUp(source(List.of("R1"), false)))
                .verifyErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessage(HandoverMessages.dedupKeyRequired()));

        ReactiveHandover<String, String> live = ReactiveHandover.create(
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
        ReactiveHandover<String, String> handover = handover(delivered);

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
        ReactiveHandover<String, String> handover = handover(delivered);

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
        ReactiveHandover<String, String> handover = handover(delivered);
        handover.catchUp(source(List.of(), true)).block(Duration.ofSeconds(5));

        StepVerifier.create(handover.acceptIfLive("L1")).expectNext(true).verifyComplete();

        assertThat(delivered).containsExactly("L1");
    }

    @Test
    void acceptIfLive_reports_true_for_a_key_an_earlier_attempt_already_delivered() {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String, String> handover = handover(delivered);
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
        ReactiveHandover<String, String> handover = handover(new ArrayList<>());
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

    // A fold written in Kotlin can throw a checked exception without declaring it. It errors the replay pipeline like
    // any other failure, and the pipeline's error handler records it.
    @Test
    void a_checked_exception_from_a_replayed_fold_is_recorded_and_a_later_live_payload_is_refused() {
        assertThatAFoldFailureIsRecordedAndALaterLivePayloadRefused(new IOException("the view is down"));
    }

    @Test
    void a_runtime_exception_from_a_replayed_fold_is_recorded_and_a_later_live_payload_is_refused() {
        assertThatAFoldFailureIsRecordedAndALaterLivePayloadRefused(new IllegalStateException("the view is down"));
    }

    // The source's own replayAbandoned() throwing must neither replace the failure that made the engine call it nor
    // stop the rest of the failure handling, which is what tells the pending payloads and the caller about it.
    @Test
    void a_replay_abandoned_that_throws_a_checked_exception_does_not_keep_the_failure_from_reaching_the_caller() {
        assertThatAThrowingReplayAbandonedStillFailsTheCatchUp(new IOException("replayAbandoned boom"));
    }

    @Test
    void a_replay_abandoned_that_throws_a_runtime_exception_does_not_keep_the_failure_from_reaching_the_caller() {
        assertThatAThrowingReplayAbandonedStillFailsTheCatchUp(new IllegalStateException("replayAbandoned boom"));
    }

    private static void assertThatAFoldFailureIsRecordedAndALaterLivePayloadRefused(Exception foldFailure) {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> {
            if (payload.equals("R2")) {
                sneakyThrow(foldFailure);
            }
            delivered.add(payload);
        }), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");

        StepVerifier.create(handover.catchUp(source(List.of("R1", "R2"), false)))
                .verifyErrorSatisfies(error -> assertThat(error).isSameAs(foldFailure));

        StepVerifier.create(handover.accept("L1"))
                .verifyErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("Catch-up failed")
                        .hasCauseReference(foldFailure));
        assertThat(handover.refusesPermanently()).isTrue();
        assertThat(delivered).containsExactly("R1");
    }

    private static void assertThatAThrowingReplayAbandonedStillFailsTheCatchUp(Exception abandonFailure) {
        ReactiveHandover<String, String> handover = handover(new ArrayList<>());
        RuntimeException replayFailure = new RuntimeException("replay boom");
        FakeSource source = source(List.of(), false);
        source.replayFailure = replayFailure;
        source.onReplayAbandoned = () -> sneakyThrow(abandonFailure);

        // Bounded rather than left to the class timeout, so a failure handler that never finishes shows up as this
        // expectation going unmet.
        StepVerifier.create(handover.catchUp(source))
                .expectErrorSatisfies(error -> assertThat(error).isSameAs(replayFailure))
                .verify(Duration.ofSeconds(5));

        StepVerifier.create(handover.accept("L1"))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("Catch-up failed")
                        .hasCauseReference(replayFailure))
                .verify(Duration.ofSeconds(5));
        assertThat(handover.refusesPermanently()).isTrue();
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(Throwable failure) throws T {
        throw (T) failure;
    }

    // --- helpers ---

    private static ReactiveHandover<String, String> handover(List<String> delivered) {
        return ReactiveHandover.create(
                payload -> Mono.fromRunnable(() -> delivered.add(payload)), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
    }

    /**
     * Two replays folding into the same view at once is the loss the hold before a replay exists to prevent. The
     * first to finish takes the handover live, and from then on live payloads reach the view next to the second
     * replay, which throws them away with its batch if it stops.
     */
    @Test
    void a_replay_waits_for_a_replay_already_running() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch foldingR1 = new CountDownLatch(1);
        CountDownLatch releaseR1 = new CountDownLatch(1);
        CountDownLatch secondReplayStarted = new CountDownLatch(1);
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> {
            log.add(payload);
            if (payload.equals("R1")) {
                foldingR1.countDown();
                try {
                    releaseR1.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        FakeSource second = source(List.of("R2"), false);
        second.onReplayStarted = secondReplayStarted::countDown;

        Mono<Boolean> firstCatchUp = handover.catchUp(source(List.of("R1"), false));
        assertThat(foldingR1.await(5, TimeUnit.SECONDS)).isTrue();
        Mono<Boolean> secondCatchUp = handover.catchUp(second);
        assertThat(secondReplayStarted.await(300, TimeUnit.MILLISECONDS))
                .as("the second replay waits for the first").isFalse();

        releaseR1.countDown();

        StepVerifier.create(firstCatchUp).expectNext(true).verifyComplete();
        StepVerifier.create(secondCatchUp).expectNext(true).verifyComplete();
        assertThat(secondReplayStarted.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(log).containsExactly("R1", "R2");
    }

    /**
     * The payloads a drain delivers were checked against the keys of the replay before them and are reported to that
     * replay's source. A replay starting before that drain ends clears those keys and takes over the source, so a
     * copy still queued is delivered a second time or reported to the wrong replay.
     */
    @Test
    void a_replay_waits_for_the_drain_of_the_catch_up_ahead_of_it() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        AtomicReference<ReactiveHandover<String, String>> self = new AtomicReference<>();
        AtomicReference<CompletableFuture<Boolean>> liveAck = new AtomicReference<>();
        CountDownLatch foldingR1 = new CountDownLatch(1);
        CountDownLatch releaseR1 = new CountDownLatch(1);
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> {
            log.add(payload);
            if (payload.equals("R1")) {
                liveAck.set(offeredFromOutside(() -> self.get().acceptReportingDelivery("L1")));
                foldingR1.countDown();
                try {
                    releaseR1.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        self.set(handover);
        FakeSource second = source(List.of("R2"), false);
        second.onReplayStarted = () -> log.add("second started");

        Mono<Boolean> firstCatchUp = handover.catchUp(source(List.of("R1"), false));
        assertThat(foldingR1.await(5, TimeUnit.SECONDS)).isTrue();
        Mono<Boolean> secondCatchUp = handover.catchUp(second);
        Mono.delay(Duration.ofMillis(300)).block();

        releaseR1.countDown();

        StepVerifier.create(firstCatchUp).expectNext(true).verifyComplete();
        StepVerifier.create(secondCatchUp).expectNext(true).verifyComplete();
        assertThat(liveAck.get().get(5, TimeUnit.SECONDS)).isTrue();
        assertThat(log).as("the first catch-up's drain ends before the second replay starts")
                .containsExactly("R1", "L1", "second started", "R2");
    }

    /**
     * The last delivery of a drain tells the source and gives the replay turn back. A source whose callback throws
     * must not keep the turn, since that runs where no error handler hears about it and every later replay would wait.
     */
    @Test
    void a_drained_callback_that_throws_still_lets_the_next_replay_start() throws Exception {
        AtomicReference<ReactiveHandover<String, String>> self = new AtomicReference<>();
        AtomicReference<CompletableFuture<Boolean>> liveAck = new AtomicReference<>();
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> {
            if (payload.equals("R1")) {
                liveAck.set(offeredFromOutside(() -> self.get().acceptReportingDelivery("L1")));
            }
        }), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        self.set(handover);
        FakeSource first = source(List.of("R1"), false);
        first.onLiveDrained = () -> {
            throw new IllegalStateException("drained callback failed");
        };

        StepVerifier.create(handover.catchUp(first)).expectNext(true).verifyComplete();
        assertThat(liveAck.get().get(5, TimeUnit.SECONDS)).isTrue();

        StepVerifier.create(handover.catchUp(source(List.of("R2"), false)))
                .expectNext(true).expectComplete().verify(Duration.ofSeconds(5));
    }

    /**
     * A catch-up that fails before it replays owns no drain. If its failure took the drains of earlier catch-ups with
     * it, their sources would never hear that their buffer drained and their replay turns would be given back while
     * those payloads are still being delivered.
     */
    @Test
    void a_catch_up_failing_before_its_replay_leaves_an_earlier_drain_to_finish() throws Exception {
        AtomicReference<ReactiveHandover<String, String>> self = new AtomicReference<>();
        CountDownLatch deliveringL1 = new CountDownLatch(1);
        CountDownLatch releaseL1 = new CountDownLatch(1);
        CountDownLatch firstDrained = new CountDownLatch(1);
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> {
            if (payload.equals("R1")) {
                offeredFromOutside(() -> self.get().acceptReportingDelivery("L1"));
            }
            if (payload.equals("L1")) {
                deliveringL1.countDown();
                try {
                    releaseL1.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        self.set(handover);
        FakeSource first = source(List.of("R1"), false);
        first.onLiveDrained = firstDrained::countDown;
        ReactiveHandover.Source<String> failingLookup = new ReactiveHandover.Source<>() {
            @Override
            public Mono<Boolean> isAlreadyCaughtUp() {
                return Mono.error(new IllegalStateException("marker lookup failed"));
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

        StepVerifier.create(handover.catchUp(first)).expectNext(true).verifyComplete();
        assertThat(deliveringL1.await(5, TimeUnit.SECONDS)).isTrue();
        StepVerifier.create(handover.catchUp(failingLookup)).expectError(IllegalStateException.class).verify(Duration.ofSeconds(5));
        releaseL1.countDown();

        assertThat(firstDrained.await(5, TimeUnit.SECONDS)).as("the earlier drain still reached its source").isTrue();
    }

    /**
     * The same as the blocking engine. A catch-up that failed leaves the handover refusing everything, so a replay
     * holding a turn taken behind that failure is refused rather than folded into a view nobody is using any more.
     */
    @Test
    void a_replay_queued_behind_a_failed_catch_up_does_not_start() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch foldingR1 = new CountDownLatch(1);
        CountDownLatch releaseR1 = new CountDownLatch(1);
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> {
            log.add(payload);
            if (payload.equals("R1")) {
                foldingR1.countDown();
                try {
                    releaseR1.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (payload.equals("R2")) {
                throw new IllegalStateException("fold failed");
            }
        }), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        FakeSource queued = source(List.of("R3"), false);

        Mono<Boolean> failing = handover.catchUp(source(List.of("R1", "R2"), false));
        assertThat(foldingR1.await(5, TimeUnit.SECONDS)).isTrue();
        Mono<Boolean> queuedCatchUp = handover.catchUp(queued);
        releaseR1.countDown();

        StepVerifier.create(failing).expectError(IllegalStateException.class).verify(Duration.ofSeconds(5));
        StepVerifier.create(queuedCatchUp).expectError(ReactiveHandover.PreDispatchRefusalException.class).verify(Duration.ofSeconds(5));
        assertThat(log).as("the queued replay never folded its history").containsExactly("R1", "R2");
    }

    /**
     * Giving a replay turn back resumes the catch-up waiting for it on the thread that gives it back, so a failure has
     * to be published first. Otherwise that catch-up reads no failure, replays in full on a handover that refuses
     * every live payload from then on, and tells its caller the catch-up succeeded.
     */
    @Test
    void a_catch_up_woken_by_a_failure_giving_back_its_turn_sees_that_failure() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch foldingR1 = new CountDownLatch(1);
        CountDownLatch releaseR1 = new CountDownLatch(1);
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> {
            log.add(payload);
            if (payload.equals("R1")) {
                foldingR1.countDown();
                try {
                    releaseR1.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        // Fails after its drain is registered, so the failure handling gives that drain's turn back.
        FakeSource failingMarker = source(List.of("R1"), false);
        failingMarker.onMarkCaughtUp = () -> {
            throw new IllegalStateException("marker failed");
        };
        FakeSource queued = source(List.of("R2"), false);

        Mono<Boolean> failing = handover.catchUp(failingMarker);
        assertThat(foldingR1.await(5, TimeUnit.SECONDS)).isTrue();
        Mono<Boolean> queuedCatchUp = handover.catchUp(queued);
        Mono.delay(Duration.ofMillis(300)).block();
        releaseR1.countDown();

        StepVerifier.create(failing).expectError(IllegalStateException.class).verify(Duration.ofSeconds(5));
        StepVerifier.create(queuedCatchUp).expectError(ReactiveHandover.PreDispatchRefusalException.class).verify(Duration.ofSeconds(5));
        assertThat(log).as("the queued replay never folded its history").containsExactly("R1");
    }

    /**
     * The same as the blocking engine. A catch-up waiting for its turn revives the handover again when it gets the
     * turn, since the catch-up it waited for can have stopped in between and a handover left stopped answers the
     * payloads arriving during this replay as dropped rather than buffering them.
     */
    @Test
    void a_replay_that_waited_for_a_stopped_catch_up_takes_in_the_payloads_that_arrive_during_it() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        AtomicReference<ReactiveHandover<String, String>> self = new AtomicReference<>();
        AtomicReference<CompletableFuture<Boolean>> liveAck = new AtomicReference<>();
        CountDownLatch foldingR1 = new CountDownLatch(1);
        CountDownLatch releaseR1 = new CountDownLatch(1);
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> {
            log.add(payload);
            if (payload.equals("R1")) {
                foldingR1.countDown();
                try {
                    releaseR1.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (payload.equals("R3")) {
                liveAck.set(offeredFromOutside(() -> self.get().acceptReportingDelivery("L1")));
            }
        }), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        self.set(handover);
        FakeSource stopping = source(List.of("R1", "R2"), false);
        stopping.stopAfter(1);

        Mono<Boolean> stopped = handover.catchUp(stopping);
        assertThat(foldingR1.await(5, TimeUnit.SECONDS)).isTrue();
        Mono<Boolean> queued = handover.catchUp(source(List.of("R3"), false));
        Mono.delay(Duration.ofMillis(300)).block();
        releaseR1.countDown();

        StepVerifier.create(stopped).expectNext(false).verifyComplete();
        StepVerifier.create(queued).expectNext(true).verifyComplete();
        assertThat(liveAck.get().get(5, TimeUnit.SECONDS)).as("the payload was delivered rather than dropped").isTrue();
        assertThat(log).containsExactly("R1", "R3", "L1");
    }

    private static FakeSource source(List<String> history, boolean alreadyCaughtUp) {
        return new FakeSource(history, alreadyCaughtUp);
    }

    @Test
    void a_stopped_replay_emits_false_and_records_no_marker() {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String, String> handover = handover(delivered);
        FakeSource source = source(List.of("R1", "R2", "R3"), false);
        source.stopAfter(2);

        StepVerifier.create(handover.catchUp(source)).expectNext(false).verifyComplete();

        assertThat(delivered).containsExactly("R1", "R2");
        // Recording completion here would make the next catch-up skip a history it never finished folding.
        assertThat(source.markCaughtUpCallCount()).isZero();
    }

    @Test
    void a_stopped_replay_leaves_the_handover_usable_and_errors_live_acks_as_not_applied() {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String, String> handover = handover(delivered);
        FakeSource stopped = source(List.of("R1", "R2"), false);
        stopped.stopAfter(1);

        StepVerifier.create(handover.catchUp(stopped)).expectNext(false).verifyComplete();

        // Not folded, so the ack must not complete as if it were. Errored as not applied rather than as a failed
        // catch-up, and the handover does not refuse for good, which is what lets a shared feed keep serving its
        // other projections.
        StepVerifier.create(handover.acceptReportingDelivery("L1")).expectNext(false).verifyComplete();
        StepVerifier.create(handover.accept("L1"))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                        .hasMessage(HandoverMessages.stoppedBeforeApplied("test payload")))
                .verify();
        assertThat(handover.refusesPermanently()).isFalse();
        assertThat(delivered).containsExactly("R1");
    }

    @Test
    void accept_during_a_replay_completes_only_once_the_drain_has_folded_the_payload() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch replaying = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        ReactiveHandover<String, String> handover = handoverHoldingItsReplayAt("R1", delivered, replaying, releaseReplay, null);
        try {
            CompletableFuture<Boolean> catchUp = handover.catchUp(source(List.of("R1"), false)).subscribeOn(Schedulers.boundedElastic()).toFuture();
            awaitLatchQuietly(replaying);

            CompletableFuture<List<String>> foldedWhenAcceptCompleted = handover.accept("L1")
                    .then(Mono.fromCallable(() -> List.copyOf(delivered)))
                    .toFuture();
            releaseReplay.countDown();

            assertThat(catchUp.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(foldedWhenAcceptCompleted.get(5, TimeUnit.SECONDS)).as("what was folded when accept(..) completed")
                    .containsExactly("R1", "L1");
        } finally {
            releaseReplay.countDown();
        }
    }

    @Test
    void a_replay_stopped_while_accept_waits_errors_accept_and_folds_nothing() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch replaying = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        ReactiveHandover<String, String> handover = handoverHoldingItsReplayAt("R1", delivered, replaying, releaseReplay, null);
        FakeSource stopping = source(List.of("R1", "R2"), false);
        stopping.stopAfter(1);
        try {
            CompletableFuture<Boolean> catchUp = handover.catchUp(stopping).subscribeOn(Schedulers.boundedElastic()).toFuture();
            awaitLatchQuietly(replaying);

            CompletableFuture<Void> accepted = handover.accept("L1").toFuture();
            releaseReplay.countDown();

            assertThat(catchUp.get(5, TimeUnit.SECONDS)).isFalse();
            assertThatThrownBy(() -> accepted.get(5, TimeUnit.SECONDS)).as("what accept(..) errored with once the replay stopped")
                    .cause()
                    .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                    .hasMessage(HandoverMessages.stoppedBeforeApplied("test payload"));
            assertThat(delivered).containsExactly("R1");
            assertThat(handover.refusesPermanently()).isFalse();
        } finally {
            releaseReplay.countDown();
        }
    }

    @Test
    void a_catch_up_failing_while_accept_waits_errors_accept_with_the_failure() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch replaying = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        IllegalStateException foldFailure = new IllegalStateException("fold failed");
        ReactiveHandover<String, String> handover = handoverHoldingItsReplayAt("R1", delivered, replaying, releaseReplay, foldFailure);
        try {
            CompletableFuture<Boolean> catchUp = handover.catchUp(source(List.of("R1"), false)).subscribeOn(Schedulers.boundedElastic()).toFuture();
            awaitLatchQuietly(replaying);

            CompletableFuture<Void> accepted = handover.accept("L1").toFuture();
            releaseReplay.countDown();

            assertThatThrownBy(() -> catchUp.get(5, TimeUnit.SECONDS)).hasCause(foldFailure);
            assertThatThrownBy(() -> accepted.get(5, TimeUnit.SECONDS)).as("what accept(..) errored with once the catch-up failed")
                    .cause()
                    .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                    .hasMessage(HandoverMessages.catchUpFailed("test payload"))
                    .hasCauseReference(foldFailure);
            assertThat(delivered).isEmpty();
        } finally {
            releaseReplay.countDown();
        }
    }

    @Test
    void stopping_a_handover_no_catch_up_has_started_errors_a_waiting_accept_and_every_payload_after_it() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handover(delivered);
        CompletableFuture<Void> accepted = handover.accept("L1").toFuture();

        handover.stopIfNotCatchingUp();

        assertThatThrownBy(() -> accepted.get(5, TimeUnit.SECONDS)).as("what accept(..) errored with once the handover stopped")
                .cause()
                .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                .hasMessage(HandoverMessages.stoppedBeforeApplied("test payload"));
        StepVerifier.create(handover.acceptReportingDelivery("L2")).expectNext(false).verifyComplete();
        StepVerifier.create(handover.accept("L3"))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                        .hasMessage(HandoverMessages.stoppedBeforeApplied("test payload")))
                .verify(Duration.ofSeconds(5));
        assertThat(handover.refusesPermanently()).isFalse();
        assertThat(delivered).isEmpty();
    }

    @Test
    void a_catch_up_after_a_stop_no_catch_up_preceded_takes_the_handover_live() {
        List<String> delivered = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handover(delivered);
        handover.stopIfNotCatchingUp();

        StepVerifier.create(handover.catchUp(source(List.of("R1"), false))).expectNext(true).verifyComplete();

        StepVerifier.create(handover.accept("L1")).verifyComplete();
        assertThat(delivered).containsExactly("R1", "L1");
    }

    @Test
    void stopping_a_handover_while_its_catch_up_replays_leaves_a_waiting_accept_to_the_drain() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch replaying = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        ReactiveHandover<String, String> handover = handoverHoldingItsReplayAt("R1", delivered, replaying, releaseReplay, null);
        try {
            CompletableFuture<Boolean> catchUp = handover.catchUp(source(List.of("R1"), false)).toFuture();
            awaitLatchQuietly(replaying);
            CompletableFuture<Void> accepted = handover.accept("L1").toFuture();

            handover.stopIfNotCatchingUp();
            releaseReplay.countDown();

            assertThat(catchUp.get(5, TimeUnit.SECONDS)).isTrue();
            accepted.get(5, TimeUnit.SECONDS);
            assertThat(delivered).containsExactly("R1", "L1");
        } finally {
            releaseReplay.countDown();
        }
    }

    // The marker read is still outstanding, so the catch-up has not taken the replay turn yet, and only the count of
    // running catch-ups tells the stop that one was called.
    @Test
    void stopping_a_handover_while_its_catch_up_reads_the_marker_leaves_a_waiting_accept_to_that_catch_up() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handover(delivered);
        CompletableFuture<Boolean> markerRead = new CompletableFuture<>();
        ReactiveHandover.Source<String> source = new ReactiveHandover.Source<>() {
            @Override
            public Mono<Boolean> isAlreadyCaughtUp() {
                return Mono.fromFuture(markerRead);
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
        CompletableFuture<Boolean> catchUp = handover.catchUp(source).toFuture();
        CompletableFuture<Void> accepted = handover.accept("L1").toFuture();

        handover.stopIfNotCatchingUp();
        markerRead.complete(true);

        assertThat(catchUp.get(5, TimeUnit.SECONDS)).isTrue();
        accepted.get(5, TimeUnit.SECONDS);
        assertThat(delivered).containsExactly("L1");
    }

    // The source is asked on the thread that called catchUp(..), before any pipeline exists, and asking it can take
    // as long as the source likes.
    @Test
    void stopping_a_handover_while_its_catch_up_asks_the_source_whether_it_is_caught_up_leaves_a_waiting_accept_to_that_catch_up() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handover(delivered);
        CountDownLatch asking = new CountDownLatch(1);
        CountDownLatch answer = new CountDownLatch(1);
        ReactiveHandover.Source<String> source = new ReactiveHandover.Source<>() {
            @Override
            public Mono<Boolean> isAlreadyCaughtUp() {
                asking.countDown();
                awaitLatchQuietly(answer);
                return Mono.just(true);
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
        CompletableFuture<Void> accepted = handover.accept("L1").toFuture();
        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            java.util.concurrent.Future<CompletableFuture<Boolean>> catchingUp = executor.submit(() -> handover.catchUp(source).toFuture());
            awaitLatchQuietly(asking);

            handover.stopIfNotCatchingUp();
            answer.countDown();

            assertThat(catchingUp.get(5, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(catchThrowable(() -> accepted.get(5, TimeUnit.SECONDS))).as("what accept(..) failed with").isNull();
            assertThat(delivered).containsExactly("L1");
        } finally {
            answer.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void a_stop_after_a_catch_up_whose_source_threw_when_asked_whether_it_is_caught_up_errors_a_waiting_accept() {
        List<String> delivered = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handover(delivered);
        ReactiveHandover.Source<String> source = new ReactiveHandover.Source<>() {
            @Override
            public Mono<Boolean> isAlreadyCaughtUp() {
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
        CompletableFuture<Void> accepted = handover.accept("L1").toFuture();
        assertThatThrownBy(() -> handover.catchUp(source)).hasMessage("marker unreadable");

        handover.stopIfNotCatchingUp();

        assertThatThrownBy(() -> accepted.get(5, TimeUnit.SECONDS)).as("what accept(..) errored with once the handover stopped")
                .cause()
                .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                .hasMessage(HandoverMessages.stoppedBeforeApplied("test payload"));
        assertThat(delivered).isEmpty();
    }

    // The catch-up cleared the stop when it was called, and threw before anything could answer a payload fed after it.
    @Test
    void a_payload_fed_after_a_stop_and_a_catch_up_whose_source_threw_errors() {
        List<String> delivered = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handover(delivered);
        handover.stopIfNotCatchingUp();
        ReactiveHandover.Source<String> source = new ReactiveHandover.Source<>() {
            @Override
            public Mono<Boolean> isAlreadyCaughtUp() {
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
        assertThatThrownBy(() -> handover.catchUp(source)).hasMessage("marker unreadable");

        CompletableFuture<Void> accepted = handover.accept("L1").toFuture();

        assertThat(catchThrowable(() -> accepted.get(5, TimeUnit.SECONDS))).as("what accept(..) failed with after the stop")
                .isInstanceOf(java.util.concurrent.ExecutionException.class)
                .cause()
                .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                .hasMessage(HandoverMessages.stoppedBeforeApplied("test payload"));
        assertThat(delivered).isEmpty();
    }

    // The stop found the catch-up running and left the waiting payload to it, and the catch-up then threw before
    // anything could answer that payload.
    @Test
    void a_stop_during_a_catch_up_whose_source_then_threw_errors_a_waiting_accept() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handover(delivered);
        CountDownLatch asking = new CountDownLatch(1);
        CountDownLatch answer = new CountDownLatch(1);
        ReactiveHandover.Source<String> source = new ReactiveHandover.Source<>() {
            @Override
            public Mono<Boolean> isAlreadyCaughtUp() {
                asking.countDown();
                awaitLatchQuietly(answer);
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
        CompletableFuture<Void> accepted = handover.accept("L1").toFuture();
        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            java.util.concurrent.Future<?> catchingUp = executor.submit(() -> handover.catchUp(source));
            awaitLatchQuietly(asking);

            handover.stopIfNotCatchingUp();
            answer.countDown();

            assertThatThrownBy(() -> catchingUp.get(5, TimeUnit.SECONDS)).cause().hasMessage("marker unreadable");
            assertThat(catchThrowable(() -> accepted.get(5, TimeUnit.SECONDS))).as("what accept(..) failed with")
                    .isInstanceOf(java.util.concurrent.ExecutionException.class)
                    .cause()
                    .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                    .hasMessage(HandoverMessages.stoppedBeforeApplied("test payload"));
            assertThat(delivered).isEmpty();
        } finally {
            answer.countDown();
            executor.shutdownNow();
        }
    }

    // The stop finds the first catch-up running, and the second one starts after the stop. Both sources throw, the
    // first one first, so the first finds the second still running and the second finds no stop since it started.
    @Test
    void a_stop_between_two_catch_ups_whose_sources_threw_errors_a_waiting_accept() {
        assertThat(acceptAfterTwoCatchUpsThrewAroundAStop(true)).as("what accept(L1) ended with after a stop and two catch-ups that threw")
                .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                .hasMessage(HandoverMessages.stoppedBeforeApplied("test payload"));
    }

    @Test
    void a_stop_between_two_catch_ups_whose_sources_threw_errors_a_waiting_accept_when_the_later_one_throws_first() {
        assertThat(acceptAfterTwoCatchUpsThrewAroundAStop(false)).as("what accept(L1) ended with after a stop and two catch-ups that threw")
                .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                .hasMessage(HandoverMessages.stoppedBeforeApplied("test payload"));
    }

    // Returns what the Mono of a payload fed before both catch-ups ended with, a TimeoutException when it never ended.
    private static Throwable acceptAfterTwoCatchUpsThrewAroundAStop(boolean firstThrowsFirst) {
        List<String> delivered = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handover(delivered);
        CountDownLatch askingFirst = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch askingSecond = new CountDownLatch(1);
        CountDownLatch releaseSecond = new CountDownLatch(1);
        CompletableFuture<Void> accepted = handover.accept("L1").toFuture();
        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(2);
        try {
            java.util.concurrent.Future<?> first = executor.submit(() -> handover.catchUp(sourceThatThrowsWhenAsked(askingFirst, releaseFirst)));
            awaitLatchQuietly(askingFirst);
            handover.stopIfNotCatchingUp();
            java.util.concurrent.Future<?> second = executor.submit(() -> handover.catchUp(sourceThatThrowsWhenAsked(askingSecond, releaseSecond)));
            awaitLatchQuietly(askingSecond);

            if (firstThrowsFirst) {
                releaseFirst.countDown();
                assertThatThrownBy(() -> first.get(5, TimeUnit.SECONDS)).cause().hasMessage("marker unreadable");
                releaseSecond.countDown();
                assertThatThrownBy(() -> second.get(5, TimeUnit.SECONDS)).cause().hasMessage("marker unreadable");
            } else {
                releaseSecond.countDown();
                assertThatThrownBy(() -> second.get(5, TimeUnit.SECONDS)).cause().hasMessage("marker unreadable");
                releaseFirst.countDown();
                assertThatThrownBy(() -> first.get(5, TimeUnit.SECONDS)).cause().hasMessage("marker unreadable");
            }

            assertThat(delivered).isEmpty();
            try {
                accepted.get(5, TimeUnit.SECONDS);
                return null;
            } catch (java.util.concurrent.ExecutionException e) {
                return e.getCause();
            } catch (java.util.concurrent.TimeoutException | InterruptedException e) {
                return e;
            }
        } finally {
            releaseFirst.countDown();
            releaseSecond.countDown();
            executor.shutdownNow();
        }
    }

    private static ReactiveHandover.Source<String> sourceThatThrowsWhenAsked(CountDownLatch asking, CountDownLatch release) {
        return new ReactiveHandover.Source<>() {
            @Override
            public Mono<Boolean> isAlreadyCaughtUp() {
                asking.countDown();
                awaitLatchQuietly(release);
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

    // The refusal of a payload the full buffer has no room for runs its caller's code, here a stop, while another
    // thread is stopping the handover too.
    @Test
    void a_stop_from_the_refusal_of_a_payload_the_buffer_has_no_room_for_does_not_deadlock_with_another_stop() throws Exception {
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.empty(), payload -> payload,
                new CatchupThenLiveOptions(CatchupThenLiveOptions.DEFAULT_DEDUP_CACHE_SIZE, 1), "test payload");
        handover.accept("L1").subscribe(ignored -> {
        }, ignored -> {
        });
        CountDownLatch refused = new CountDownLatch(1);
        CountDownLatch refusingDone = new CountDownLatch(1);
        CountDownLatch stoppingDone = new CountDownLatch(1);
        Thread stopping = new Thread(() -> {
            awaitLatchQuietly(refused);
            handover.stopIfNotCatchingUp();
            stoppingDone.countDown();
        }, "stopping");
        Thread refusing = new Thread(() -> {
            handover.accept("L2").subscribe(ignored -> {
            }, error -> {
                refused.countDown();
                await().atMost(Duration.ofSeconds(5)).until(() -> stopping.getState() == Thread.State.BLOCKED
                        || stopping.getState() == Thread.State.TERMINATED);
                handover.stopIfNotCatchingUp();
            });
            refusingDone.countDown();
        }, "refusing");
        stopping.setDaemon(true);
        refusing.setDaemon(true);
        stopping.start();
        refusing.start();

        boolean finished = refusingDone.await(5, TimeUnit.SECONDS) && stoppingDone.await(5, TimeUnit.SECONDS);

        assertThat(java.lang.management.ManagementFactory.getThreadMXBean().findDeadlockedThreads()).as("deadlocked threads").isNull();
        assertThat(finished).as("both threads finished").isTrue();
    }

    // No catch-up runs, so the stop alone has to answer every payload, including one that joins the waiting payloads
    // while the stop is answering them.
    @Test
    void an_accept_racing_a_stop_with_no_catch_up_errors() throws Exception {
        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(2);
        try {
            for (int round = 0; round < 2000; round++) {
                List<String> delivered = new CopyOnWriteArrayList<>();
                ReactiveHandover<String, String> handover = handover(delivered);
                CountDownLatch start = new CountDownLatch(1);
                java.util.concurrent.Future<CompletableFuture<Void>> accepting = executor.submit(() -> {
                    start.await();
                    return handover.accept("L1").toFuture();
                });
                java.util.concurrent.Future<?> stopping = executor.submit(() -> {
                    start.await();
                    handover.stopIfNotCatchingUp();
                    return null;
                });
                start.countDown();

                stopping.get(5, TimeUnit.SECONDS);
                CompletableFuture<Void> accepted = accepting.get(5, TimeUnit.SECONDS);
                assertThat(catchThrowable(() -> accepted.get(5, TimeUnit.SECONDS))).as("what accept(..) failed with in round %d", round)
                        .isInstanceOf(java.util.concurrent.ExecutionException.class)
                        .cause()
                        .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                        .hasMessage(HandoverMessages.stoppedBeforeApplied("test payload"));
                assertThat(delivered).isEmpty();
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void stopping_a_live_handover_leaves_it_delivering() {
        List<String> delivered = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handover(delivered);
        StepVerifier.create(handover.catchUp(source(List.of("R1"), false))).expectNext(true).verifyComplete();

        handover.stopIfNotCatchingUp();

        StepVerifier.create(handover.accept("L1")).verifyComplete();
        assertThat(delivered).containsExactly("R1", "L1");
    }

    // Whichever order the three calls run in, the payload's Mono completes once the payload is delivered, or errors
    // and the payload is never delivered. It never stays pending after both the stop and the catch-up have returned.
    @Test
    void an_accept_racing_a_stop_and_a_catch_up_completes_delivered_or_errors_undelivered() throws Exception {
        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(3);
        try {
            for (int round = 0; round < 500; round++) {
                List<String> delivered = new CopyOnWriteArrayList<>();
                ReactiveHandover<String, String> handover = handover(delivered);
                CountDownLatch start = new CountDownLatch(1);
                java.util.concurrent.Future<CompletableFuture<Void>> accepting = executor.submit(() -> {
                    start.await();
                    return handover.accept("L1").toFuture();
                });
                java.util.concurrent.Future<?> stopping = executor.submit(() -> {
                    start.await();
                    handover.stopIfNotCatchingUp();
                    return null;
                });
                java.util.concurrent.Future<CompletableFuture<Boolean>> catchingUp = executor.submit(() -> {
                    start.await();
                    return handover.catchUp(source(List.of("R1"), false)).toFuture();
                });
                start.countDown();

                stopping.get(5, TimeUnit.SECONDS);
                assertThat(catchingUp.get(5, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS)).as("catch-up in round %d", round).isTrue();
                CompletableFuture<Void> accepted = accepting.get(5, TimeUnit.SECONDS);
                Throwable failure = catchThrowable(() -> accepted.get(5, TimeUnit.SECONDS));
                if (failure == null) {
                    assertThat(delivered).as("delivered in round %d, where accept(..) completed", round).containsExactly("R1", "L1");
                } else {
                    assertThat(failure).as("what accept(..) errored with in round %d", round)
                            .isInstanceOf(java.util.concurrent.ExecutionException.class)
                            .cause()
                            .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                            .hasMessage(HandoverMessages.stoppedBeforeApplied("test payload"));
                    assertThat(delivered).as("delivered in round %d, where accept(..) errored", round).containsExactly("R1");
                }
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private static ReactiveHandover<String, String> handoverHoldingItsReplayAt(String held, List<String> delivered, CountDownLatch reached,
                                                                               CountDownLatch release, RuntimeException failure) {
        return ReactiveHandover.create(payload -> Mono.fromRunnable(() -> {
            if (payload.equals(held)) {
                reached.countDown();
                awaitLatchQuietly(release);
                if (failure != null) {
                    throw failure;
                }
            }
            delivered.add(payload);
        }), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
    }

    @Test
    void a_later_catch_up_revives_a_handover_a_previous_one_stopped() {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String, String> handover = handover(delivered);
        FakeSource stopped = source(List.of("R1", "R2"), false);
        stopped.stopAfter(1);
        StepVerifier.create(handover.catchUp(stopped)).expectNext(false).verifyComplete();

        FakeSource retried = source(List.of("R1", "R2"), false);
        StepVerifier.create(handover.catchUp(retried)).expectNext(true).verifyComplete();

        assertThat(retried.markCaughtUpCallCount()).isEqualTo(1);
        StepVerifier.create(handover.accept("L1")).verifyComplete();
        assertThat(delivered).containsExactly("R1", "R1", "R2", "L1");
    }

    // A view that buffers during a replay discards that buffer when the replay stops, so a key the stopped replay left
    // behind would suppress the only copy of an event the read model never got. The key is forgotten instead, and a
    // view that wrote the event through receives it twice, which at-least-once delivery allows.
    @Test
    void a_payload_a_stopped_replay_delivered_is_delivered_again_once_the_handover_goes_live() {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String, String> handover = handover(delivered);
        FakeSource stopped = source(List.of("R1", "R2"), false);
        stopped.stopAfter(1);
        StepVerifier.create(handover.catchUp(stopped)).expectNext(false).verifyComplete();

        FakeSource goLive = source(List.of(), true);
        StepVerifier.create(handover.catchUp(goLive)).expectNext(true).verifyComplete();
        StepVerifier.create(handover.accept("R1")).verifyComplete();

        assertThat(delivered).containsExactly("R1", "R1");
        assertThat(stopped.alreadyDeliveredByReplay).isEmpty();
        assertThat(goLive.alreadyDeliveredByReplay).isEmpty();
    }

    // The live sink accepts one subscriber ever, so a catch-up on a handover that is already live must not subscribe it
    // again. The sink's refusal would be recorded as a failed catch-up and every later payload refused. It would
    // arrive after the second catch-up's own signal, hence the pause.
    @Test
    void a_catch_up_on_a_handover_that_is_already_live_leaves_it_accepting_payloads() {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String, String> handover = handover(delivered);
        StepVerifier.create(handover.catchUp(source(List.of("R1"), false))).expectNext(true).verifyComplete();

        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();
        Mono.delay(Duration.ofMillis(500)).block();

        assertThat(handover.refusesPermanently()).isFalse();
        StepVerifier.create(handover.accept("L1")).verifyComplete();
        assertThat(delivered).containsExactly("R1", "L1");
    }

    // A catch-up that replays nothing leaves a live copy of a payload the earlier, finished replay delivered reaching
    // the source that replayed it, since that source is the one that can record it.
    @Test
    void a_catch_up_that_replays_nothing_leaves_the_earlier_replays_payloads_reaching_the_source_that_replayed_them() {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String, String> handover = handover(delivered);
        FakeSource replayed = source(List.of("1"), false);
        StepVerifier.create(handover.catchUp(replayed)).expectNext(true).verifyComplete();

        FakeSource goLive = source(List.of(), true);
        StepVerifier.create(handover.catchUp(goLive)).expectNext(true).verifyComplete();
        StepVerifier.create(handover.accept("1")).verifyComplete();

        assertThat(delivered).containsExactly("1");
        assertThat(replayed.alreadyDeliveredByReplay).containsExactly("1");
        assertThat(goLive.alreadyDeliveredByReplay).isEmpty();
    }

    // The blocking engine has the same test. The live pipeline delivers on its own thread here, so the replay pauses
    // after R1 for long enough that a live payload delivered mid-replay would reach the log before the abandon does.
    @Test
    void a_live_payload_accepted_while_a_replay_runs_on_a_live_handover_is_delivered_after_that_replay_is_stopped() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        List<CompletableFuture<Boolean>> liveAcks = new CopyOnWriteArrayList<>();
        AtomicReference<ReactiveHandover<String, String>> self = new AtomicReference<>();
        AtomicBoolean offered = new AtomicBoolean();
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.defer(() -> {
            log.add(payload);
            if (payload.equals("R1") && offered.compareAndSet(false, true)) {
                liveAcks.add(offeredFromOutside(() -> self.get().acceptReportingDelivery("R1")));
                liveAcks.add(offeredFromOutside(() -> self.get().acceptReportingDelivery("L1")));
                return Mono.delay(Duration.ofMillis(300)).then();
            }
            return Mono.empty();
        }), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        self.set(handover);
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();
        FakeSource replaying = source(List.of("R1", "R2"), false);
        replaying.stopAfter(1);
        replaying.onReplayAbandoned = () -> log.add("abandoned");

        StepVerifier.create(handover.catchUp(replaying)).expectNext(false).verifyComplete();
        for (CompletableFuture<Boolean> ack : liveAcks) {
            assertThat(ack.get(5, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(log).containsExactly("R1", "abandoned", "R1", "L1");
    }

    // A stop ends the replay, not the live delivery the handover already had, so a payload fed after it is delivered,
    // the same as on the blocking engine.
    @Test
    void a_live_handover_keeps_delivering_after_a_replay_on_it_is_stopped() {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String, String> handover = handover(delivered);
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();
        FakeSource replaying = source(List.of("R1", "R2"), false);
        replaying.stopAfter(1);
        StepVerifier.create(handover.catchUp(replaying)).expectNext(false).verifyComplete();

        StepVerifier.create(handover.acceptReportingDelivery("L1")).expectNext(true).verifyComplete();
        assertThat(delivered).containsExactly("R1", "L1");
    }

    // A replay owns the keys it delivered. A second replay starts from none, so a live copy of an event only the first
    // replay delivered is delivered, rather than suppressed and reported to the second replay's source, which never
    // saw it.
    @Test
    void a_second_replay_starts_without_the_keys_the_first_replay_delivered() {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String, String> handover = handover(delivered);
        FakeSource first = source(List.of("1"), false);
        StepVerifier.create(handover.catchUp(first)).expectNext(true).verifyComplete();
        FakeSource second = source(List.of(), false);
        StepVerifier.create(handover.catchUp(second)).expectNext(true).verifyComplete();

        StepVerifier.create(handover.accept("1")).verifyComplete();

        assertThat(delivered).containsExactly("1", "1");
        assertThat(first.alreadyDeliveredByReplay).isEmpty();
        assertThat(second.alreadyDeliveredByReplay).isEmpty();
    }

    // When a replay on a live handover fails, the acknowledgements of the live payloads held back during it fail with
    // the catch-up failure, so their callers offer them again. None of them may reach the view here as well. Two of
    // them, since the failure opens the pause, so only the first ever waits at it and the second arrives after.
    // The pause gives a delivery that would wrongly follow the failure the time to show up in the log.
    @Test
    void live_payloads_held_back_while_a_replay_on_a_live_handover_fails_are_not_delivered_after_their_acks_failed() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        List<CompletableFuture<Boolean>> liveAcks = new CopyOnWriteArrayList<>();
        AtomicReference<ReactiveHandover<String, String>> self = new AtomicReference<>();
        AtomicBoolean offered = new AtomicBoolean();
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.defer(() -> {
            if (payload.equals("R2")) {
                return Mono.error(new IllegalStateException("replay boom"));
            }
            log.add(payload);
            if (payload.equals("R1") && offered.compareAndSet(false, true)) {
                liveAcks.add(offeredFromOutside(() -> self.get().acceptReportingDelivery("L1")));
                liveAcks.add(offeredFromOutside(() -> self.get().acceptReportingDelivery("L2")));
            }
            return Mono.empty();
        }), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        self.set(handover);
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();

        StepVerifier.create(handover.catchUp(source(List.of("R1", "R2"), false))).verifyErrorMessage("replay boom");
        for (CompletableFuture<Boolean> ack : liveAcks) {
            assertThat(catchThrowable(() -> ack.get(5, TimeUnit.SECONDS))).hasCauseInstanceOf(ReactiveHandover.PreDispatchRefusalException.class);
        }
        Mono.delay(Duration.ofMillis(300)).block();

        assertThat(log).containsExactly("R1");
    }

    // A stop answers the payloads it drops before it reports itself stopped, so a caller that goes on to call goLive()
    // finds them answered rather than answered while that call is running.
    @Test
    void a_stopped_replay_answers_the_payloads_it_drops_before_it_reports_the_stop() throws Exception {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String, String> handover = handover(delivered);
        CompletableFuture<Boolean> buffered = handover.acceptReportingDelivery("L1").toFuture();
        FakeSource stopping = source(List.of("R1", "R2"), false);
        stopping.stopAfter(1);

        // Read inside the signal itself, which the stop emits on its own thread, so this is the state a caller reacting
        // to that signal sees rather than whatever the two threads happen to reach first.
        AtomicBoolean answeredWhenTheStopWasReported = new AtomicBoolean();
        StepVerifier.create(handover.catchUp(stopping).doOnNext(ignored -> answeredWhenTheStopWasReported.set(buffered.isDone())))
                .expectNext(false)
                .verifyComplete();

        assertThat(answeredWhenTheStopWasReported).isTrue();
        assertThat(buffered.get(5, TimeUnit.SECONDS)).isFalse();
    }

    // The caller of a payload answered false offers it again, so the next catch-up delivering the copy the stop
    // answered as well would apply it twice. L2 is queued behind that copy, so its fold is the point to check at.
    @Test
    void a_payload_a_stopped_replay_answered_is_not_delivered_by_the_next_catch_up() throws Exception {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String, String> handover = handover(delivered);
        CompletableFuture<Boolean> buffered = handover.acceptReportingDelivery("L1").toFuture();
        FakeSource stopping = source(List.of("R1", "R2"), false);
        stopping.stopAfter(1);
        StepVerifier.create(handover.catchUp(stopping)).expectNext(false).verifyComplete();
        assertThat(buffered.get(5, TimeUnit.SECONDS)).isFalse();

        StepVerifier.create(handover.catchUp(source(List.of("R1", "R2"), false))).expectNext(true).verifyComplete();
        StepVerifier.create(handover.acceptReportingDelivery("L2")).expectNext(true).verifyComplete();

        assertThat(delivered).containsExactly("R1", "R1", "R2", "L2");
    }

    // The buffer holds one payload, and the stop answered the one it held, so a payload arriving during the next
    // replay has that place to itself.
    @Test
    void a_payload_a_stopped_replay_answered_no_longer_takes_a_place_in_the_live_buffer() throws Exception {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<ReactiveHandover<String, String>> self = new AtomicReference<>();
        AtomicReference<CompletableFuture<Boolean>> duringNextReplay = new AtomicReference<>();
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> {
            delivered.add(payload);
            if (payload.equals("B1")) {
                duringNextReplay.set(offeredFromOutside(() -> self.get().acceptReportingDelivery("L2")));
            }
        }), payload -> payload, new CatchupThenLiveOptions(CatchupThenLiveOptions.DEFAULT_DEDUP_CACHE_SIZE, 1), "test payload");
        self.set(handover);
        CompletableFuture<Boolean> buffered = handover.acceptReportingDelivery("L1").toFuture();
        FakeSource stopping = source(List.of("A1", "A2"), false);
        stopping.stopAfter(1);
        StepVerifier.create(handover.catchUp(stopping)).expectNext(false).verifyComplete();
        assertThat(buffered.get(5, TimeUnit.SECONDS)).isFalse();

        StepVerifier.create(handover.catchUp(source(List.of("B1"), false))).expectNext(true).verifyComplete();

        assertThat(duringNextReplay.get().get(5, TimeUnit.SECONDS)).isTrue();
        assertThat(delivered).containsExactly("A1", "B1", "L2");
    }

    // A catch-up that arrives while an earlier one's buffered payloads are still being delivered counts its own
    // payloads and tells its own source. The earlier catch-up is still told when its own set is exhausted, which one
    // shared set of counters could not do, since the later catch-up took them over.
    @Test
    void a_catch_up_arriving_during_an_earlier_drain_leaves_that_drain_its_own_source() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch firstDeliveryReached = new CountDownLatch(1);
        CountDownLatch releaseFirstDelivery = new CountDownLatch(1);
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> {
            if (payload.equals("L1")) {
                firstDeliveryReached.countDown();
                try {
                    releaseFirstDelivery.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            log.add(payload);
        }), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        handover.accept("L1").subscribe();
        handover.accept("L2").subscribe();
        FakeSource first = source(List.of(), false);
        first.onLiveDrained = () -> log.add("first drained");
        FakeSource second = source(List.of(), true);
        second.onLiveDrained = () -> log.add("second drained");

        StepVerifier.create(handover.catchUp(first)).expectNext(true).verifyComplete();
        assertThat(firstDeliveryReached.await(5, TimeUnit.SECONDS)).isTrue();
        StepVerifier.create(handover.catchUp(second)).expectNext(true).verifyComplete();
        releaseFirstDelivery.countDown();
        Mono.delay(Duration.ofMillis(500)).block();

        assertThat(log).contains("L1", "L2", "first drained", "second drained");
    }

    // A catch-up with nothing to replay runs while another catch-up is replaying, which is what a feed's goLive()
    // racing its catchUp() does. It must not let the live payloads through, since the replay it would release them
    // into can still discard what a view buffered from them. Nor does it complete before that replay ends, since
    // acceptIfLive(..) refuses until then.
    @Test
    void a_catch_up_with_nothing_to_replay_does_not_release_a_running_replays_hold_on_live_delivery() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        List<CompletableFuture<Boolean>> liveAcks = new CopyOnWriteArrayList<>();
        AtomicReference<ReactiveHandover<String, String>> self = new AtomicReference<>();
        AtomicBoolean offered = new AtomicBoolean();
        CountDownLatch replayReached = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> {
            log.add(payload);
            if (payload.equals("R1") && offered.compareAndSet(false, true)) {
                liveAcks.add(offeredFromOutside(() -> self.get().acceptReportingDelivery("L1")));
                replayReached.countDown();
                try {
                    releaseReplay.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        self.set(handover);
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();
        Mono<Boolean> replaying = handover.catchUp(source(List.of("R1"), false));
        assertThat(replayReached.await(5, TimeUnit.SECONDS)).isTrue();

        CompletableFuture<Boolean> nothingToReplay = waitingBehindTheRunningReplay(handover);

        assertThat(nothingToReplay).as("the catch-up with nothing to replay while R1 is held").isNotDone();
        assertThat(log).containsExactly("R1");
        releaseReplay.countDown();
        StepVerifier.create(replaying).expectNext(true).verifyComplete();
        assertThat(nothingToReplay.get(5, TimeUnit.SECONDS)).isTrue();
        assertThat(liveAcks.get(0).get(5, TimeUnit.SECONDS)).isTrue();
        assertThat(log).containsExactly("R1", "L1");
    }

    // A feed's goLive() called while its first catchUp() replays. Reporting live before that replay ends left
    // acceptIfLive(..) refusing after the caller was told the feed is live.
    @Test
    void a_catch_up_with_nothing_to_replay_reports_live_only_once_the_running_replay_has_ended() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch replayReached = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        ReactiveHandover<String, String> handover = ReactiveHandover.create(
                holdingAt("R1", log, replayReached, releaseReplay, null), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        try {
            Mono<Boolean> replaying = handover.catchUp(source(List.of("R1"), false));
            assertThat(replayReached.await(5, TimeUnit.SECONDS)).isTrue();

            CompletableFuture<Boolean> nothingToReplay = waitingBehindTheRunningReplay(handover);

            assertThat(nothingToReplay).as("the catch-up with nothing to replay while R1 is held").isNotDone();
            releaseReplay.countDown();
            StepVerifier.create(replaying).expectNext(true).verifyComplete();
            assertThat(nothingToReplay.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(handover.acceptIfLive("L1").block(Duration.ofSeconds(5))).isTrue();
            assertThat(log).containsExactly("R1", "L1");
        } finally {
            releaseReplay.countDown();
        }
    }

    // When the replay it waited for fails, the handover refuses everything, so the catch-up waiting for it errors with
    // that failure rather than reporting a handover that is live.
    @Test
    void a_catch_up_with_nothing_to_replay_fails_when_the_running_replay_it_waited_for_fails() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch replayReached = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        IllegalStateException foldFailure = new IllegalStateException("fold boom");
        ReactiveHandover<String, String> handover = ReactiveHandover.create(
                holdingAt("R1", log, replayReached, releaseReplay, foldFailure), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        try {
            Mono<Boolean> replaying = handover.catchUp(source(List.of("R1"), false));
            assertThat(replayReached.await(5, TimeUnit.SECONDS)).isTrue();

            CompletableFuture<Boolean> nothingToReplay = waitingBehindTheRunningReplay(handover);

            assertThat(nothingToReplay).as("the catch-up with nothing to replay while R1 is held").isNotDone();
            releaseReplay.countDown();
            StepVerifier.create(replaying).verifyErrorMessage("fold boom");
            assertThatThrownBy(() -> nothingToReplay.get(5, TimeUnit.SECONDS)).cause()
                    .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                    .hasCauseReference(foldFailure);
            assertThat(handover.refusesPermanently()).isTrue();
        } finally {
            releaseReplay.countDown();
        }
    }

    // The catch-up with nothing to replay found no replay holding live delivery back, and R1 took its hold before that
    // catch-up went live. Going live releases only the hold the catch-up took itself, so R1's stays in place.
    @Test
    void a_catch_up_with_nothing_to_replay_leaves_a_hold_taken_after_it_checked_for_one_in_place() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch replayReached = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        CountDownLatch historyDone = new CountDownLatch(1);
        CountDownLatch releaseHistoryDone = new CountDownLatch(1);
        ReactiveHandover<String, String> handover = ReactiveHandover.create(
                holdingAt("R1", log, replayReached, releaseReplay, null), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        FakeSource nothingToReplay = source(List.of(), true);
        nothingToReplay.onHistoryDone = () -> {
            historyDone.countDown();
            try {
                releaseHistoryDone.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
        try {
            Mono<Boolean> goingLive = handover.catchUp(nothingToReplay);
            assertThat(historyDone.await(5, TimeUnit.SECONDS)).isTrue();
            Mono<Boolean> replaying = handover.catchUp(source(List.of("R1"), false));
            assertThat(replayReached.await(5, TimeUnit.SECONDS)).isTrue();
            releaseHistoryDone.countDown();
            StepVerifier.create(goingLive).expectNext(true).verifyComplete();

            assertThat(handover.acceptIfLive("L1").block(Duration.ofSeconds(5))).as("while R1 is held").isFalse();
            releaseReplay.countDown();
            StepVerifier.create(replaying).expectNext(true).verifyComplete();
            assertThat(handover.acceptIfLive("L2").block(Duration.ofSeconds(5))).isTrue();
            assertThat(log).containsExactly("R1", "L2");
        } finally {
            releaseHistoryDone.countDown();
            releaseReplay.countDown();
        }
    }

    // A catch-up with nothing to replay waits for the hold R1 has in place when it is called, and not for R2, which
    // takes the turn once R1 ends.
    @Test
    void a_catch_up_with_nothing_to_replay_does_not_wait_for_a_replay_that_takes_the_turn_after_the_one_it_waited_for() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch foldingR1 = new CountDownLatch(1);
        CountDownLatch releaseR1 = new CountDownLatch(1);
        CountDownLatch foldingR2 = new CountDownLatch(1);
        CountDownLatch releaseR2 = new CountDownLatch(1);
        Function<String, Mono<Void>> heldAtR1 = holdingAt("R1", log, foldingR1, releaseR1, null);
        Function<String, Mono<Void>> heldAtR2 = holdingAt("R2", log, foldingR2, releaseR2, null);
        ReactiveHandover<String, String> handover = ReactiveHandover.create(
                payload -> payload.equals("R2") ? heldAtR2.apply(payload) : heldAtR1.apply(payload),
                payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        try {
            CompletableFuture<Boolean> firstReplay = handover.catchUp(source(List.of("R1"), false)).toFuture();
            assertThat(foldingR1.await(5, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<Boolean> secondReplay = handover.catchUp(source(List.of("R2"), false)).toFuture();
            CompletableFuture<Boolean> nothingToReplay = waitingBehindTheRunningReplay(handover);
            assertThat(nothingToReplay).as("the catch-up with nothing to replay while R1 is held").isNotDone();

            releaseR1.countDown();

            assertThat(nothingToReplay.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(foldingR2.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(handover.acceptIfLive("L1").block(Duration.ofSeconds(5))).as("while R2 is replaying").isFalse();
            releaseR2.countDown();
            assertThat(firstReplay.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(secondReplay.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(handover.acceptIfLive("L2").block(Duration.ofSeconds(5))).isTrue();
            assertThat(log).containsExactly("R1", "R2", "L2");
        } finally {
            releaseR1.countDown();
            releaseR2.countDown();
        }
    }

    // A fold that blocks on a catch-up with nothing to replay would wait for a replay that cannot end until the fold
    // returns, so the catch-up answers without waiting, the same as on the blocking engine.
    @Test
    void a_catch_up_with_nothing_to_replay_the_running_replays_fold_blocks_on_answers_without_waiting() {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Boolean> answers = new CopyOnWriteArrayList<>();
        AtomicReference<ReactiveHandover<String, String>> self = new AtomicReference<>();
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> {
            if (payload.equals("R1")) {
                answers.add(self.get().catchUp(source(List.of(), true)).block(Duration.ofSeconds(5)));
            }
            log.add(payload);
        }), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        self.set(handover);

        StepVerifier.create(handover.catchUp(source(List.of("R1"), false))).expectNext(true).expectComplete().verify(Duration.ofSeconds(10));

        assertThat(answers).containsExactly(true);
        assertThat(handover.acceptIfLive("L1").block(Duration.ofSeconds(5))).isTrue();
        assertThat(log).containsExactly("R1", "L1");
    }

    // The fold switches threads before it asks, so only the returned Mono being part of the fold's own tells the
    // handover where the call came from.
    @Test
    void a_catch_up_with_nothing_to_replay_composed_into_the_running_replays_fold_answers_without_waiting() {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Boolean> answers = new CopyOnWriteArrayList<>();
        AtomicReference<ReactiveHandover<String, String>> self = new AtomicReference<>();
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> {
            Mono<Void> folded = Mono.fromRunnable(() -> log.add(payload));
            if (!payload.equals("R1")) {
                return folded;
            }
            return Mono.delay(Duration.ofMillis(1))
                    .then(Mono.defer(() -> self.get().catchUp(source(List.of(), true))))
                    .doOnNext(answers::add)
                    .then(folded);
        }, payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        self.set(handover);

        StepVerifier.create(handover.catchUp(source(List.of("R1"), false))).expectNext(true).expectComplete().verify(Duration.ofSeconds(10));

        assertThat(answers).containsExactly(true);
        assertThat(handover.acceptIfLive("L1").block(Duration.ofSeconds(5))).isTrue();
        assertThat(log).containsExactly("R1", "L1");
    }

    // R2 waits for the live fold before it replays, so a live fold that blocks on a catch-up with nothing to replay
    // would wait for R2's hold while R2 waits for the fold. The catch-up answers without waiting instead.
    @Test
    void a_catch_up_with_nothing_to_replay_a_live_fold_blocks_on_while_a_replay_starts_answers_without_waiting() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Boolean> answers = new CopyOnWriteArrayList<>();
        CountDownLatch foldingL1 = new CountDownLatch(1);
        CountDownLatch releaseL1 = new CountDownLatch(1);
        AtomicReference<ReactiveHandover<String, String>> self = new AtomicReference<>();
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> {
            if (payload.equals("L1")) {
                foldingL1.countDown();
                awaitLatchQuietly(releaseL1);
                answers.add(self.get().catchUp(source(List.of(), true)).block(Duration.ofSeconds(5)));
            }
            log.add(payload);
        }), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        self.set(handover);
        try {
            assertThatALiveFoldGoesLiveWhileAReplayStarts(handover, foldingL1, releaseL1);

            assertThat(answers).containsExactly(true);
            assertThat(log).containsExactly("L1", "R2");
        } finally {
            releaseL1.countDown();
        }
    }

    // The fold switches threads before it asks, so only the returned Mono being part of the fold's own tells the
    // handover where the call came from.
    @Test
    void a_catch_up_with_nothing_to_replay_composed_into_a_live_fold_while_a_replay_starts_answers_without_waiting() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Boolean> answers = new CopyOnWriteArrayList<>();
        CountDownLatch foldingL1 = new CountDownLatch(1);
        CountDownLatch releaseL1 = new CountDownLatch(1);
        AtomicReference<ReactiveHandover<String, String>> self = new AtomicReference<>();
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> {
            Mono<Void> folded = Mono.fromRunnable(() -> log.add(payload));
            if (!payload.equals("L1")) {
                return folded;
            }
            return Mono.fromRunnable(() -> {
                        foldingL1.countDown();
                        awaitLatchQuietly(releaseL1);
                    })
                    .then(Mono.delay(Duration.ofMillis(1)))
                    .then(Mono.defer(() -> self.get().catchUp(source(List.of(), true))))
                    .doOnNext(answers::add)
                    .then(folded);
        }, payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        self.set(handover);
        try {
            assertThatALiveFoldGoesLiveWhileAReplayStarts(handover, foldingL1, releaseL1);

            assertThat(answers).containsExactly(true);
            assertThat(log).containsExactly("L1", "R2");
        } finally {
            releaseL1.countDown();
        }
    }

    // L1's fold completes on the thread that offered L1, so the accept caller's continuation runs there right after it.
    // That continuation is the caller's code, not this handover's, so the catch-up it makes waits for R2 to end.
    @Test
    void a_catch_up_the_accept_callers_continuation_makes_after_a_live_fold_waits_for_the_replay_that_started() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch foldingL1 = new CountDownLatch(1);
        CountDownLatch releaseL1 = new CountDownLatch(1);
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> {
            if (payload.equals("L1")) {
                foldingL1.countDown();
                awaitLatchQuietly(releaseL1);
            }
            log.add(payload);
        }), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        try {
            StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();
            CompletableFuture<Boolean> continued = handover.acceptIfLive("L1")
                    .flatMap(ignored -> handover.catchUp(source(List.of(), true)))
                    .flatMap(ignored -> handover.acceptIfLive("L2"))
                    .subscribeOn(Schedulers.boundedElastic())
                    .toFuture();
            assertThat(foldingL1.await(5, TimeUnit.SECONDS)).isTrue();
            CountDownLatch r2Holding = new CountDownLatch(1);
            FakeSource r2 = source(List.of("R2"), false);
            r2.onCaughtUpChecked = r2Holding::countDown;
            CompletableFuture<Boolean> secondReplay = handover.catchUp(r2).toFuture();
            assertThat(r2Holding.await(5, TimeUnit.SECONDS)).isTrue();

            releaseL1.countDown();

            assertThat(secondReplay.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(continued.get(5, TimeUnit.SECONDS)).as("whether L2 was accepted after the caller's catch-up").isTrue();
            assertThat(log).containsExactly("L1", "R2", "L2");
        } finally {
            releaseL1.countDown();
        }
    }

    // A live fold that blocks on a catch-up that replays would wait for a replay that cannot start before the fold
    // returns, so the catch-up answers true without waiting. L2 arrives while the replay runs and is delivered after it.
    // The fold waits a while after the answer, so a replay started alongside the fold is seen starting.
    @Test
    void a_catch_up_that_replays_a_live_fold_blocks_on_answers_true_and_replays_after_that_fold() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Boolean> answers = new CopyOnWriteArrayList<>();
        CountDownLatch replaying = new CountDownLatch(1);
        AtomicBoolean replayStartedDuringFold = new AtomicBoolean();
        FakeSource history = source(List.of("R1", "R2"), false);
        history.onReplayStarted = replaying::countDown;
        AtomicReference<ReactiveHandover<String, String>> self = new AtomicReference<>();
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> {
            if (payload.equals("L1")) {
                answers.add(self.get().catchUp(history).block(Duration.ofSeconds(5)));
                replayStartedDuringFold.set(countsDownWithin(replaying, Duration.ofMillis(200)));
            }
            log.add(payload);
        }), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        self.set(handover);
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();

        handover.accept("L1").subscribeOn(Schedulers.boundedElastic()).toFuture().get(10, TimeUnit.SECONDS);

        assertThat(answers).containsExactly(true);
        assertThat(replayStartedDuringFold).isFalse();
        assertThat(replaying.await(5, TimeUnit.SECONDS)).isTrue();
        StepVerifier.create(handover.accept("L2")).expectComplete().verify(Duration.ofSeconds(5));
        assertThat(log).containsExactly("L1", "R1", "R2", "L2");
    }

    // The fold switches threads before it asks, so only the returned Mono being part of the fold's own tells the
    // handover where the call came from.
    @Test
    void a_catch_up_that_replays_composed_into_a_live_fold_answers_true_and_replays_after_that_fold() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Boolean> answers = new CopyOnWriteArrayList<>();
        CountDownLatch replaying = new CountDownLatch(1);
        AtomicBoolean replayStartedDuringFold = new AtomicBoolean();
        FakeSource history = source(List.of("R1", "R2"), false);
        history.onReplayStarted = replaying::countDown;
        AtomicReference<ReactiveHandover<String, String>> self = new AtomicReference<>();
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> {
            Mono<Void> folded = Mono.fromRunnable(() -> log.add(payload));
            if (!payload.equals("L1")) {
                return folded;
            }
            return Mono.delay(Duration.ofMillis(1))
                    .then(Mono.defer(() -> self.get().catchUp(history)))
                    .doOnNext(answers::add)
                    .then(Mono.fromRunnable(() -> replayStartedDuringFold.set(countsDownWithin(replaying, Duration.ofMillis(200)))))
                    .then(folded);
        }, payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        self.set(handover);
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();

        StepVerifier.create(handover.accept("L1")).expectComplete().verify(Duration.ofSeconds(5));

        assertThat(answers).containsExactly(true);
        assertThat(replayStartedDuringFold).isFalse();
        assertThat(replaying.await(5, TimeUnit.SECONDS)).isTrue();
        StepVerifier.create(handover.accept("L2")).expectComplete().verify(Duration.ofSeconds(5));
        assertThat(log).containsExactly("L1", "R1", "R2", "L2");
    }

    // R1's fold blocks on a catch-up whose replay needs the turn R1's own catch-up holds until it has gone live.
    @Test
    void a_catch_up_that_replays_a_replayed_fold_blocks_on_answers_true_and_replays_after_the_running_catch_up() {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Boolean> answers = new CopyOnWriteArrayList<>();
        AtomicReference<ReactiveHandover<String, String>> self = new AtomicReference<>();
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> {
            if (payload.equals("R1")) {
                answers.add(self.get().catchUp(source(List.of("S1"), false)).block(Duration.ofSeconds(5)));
            }
            log.add(payload);
        }), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        self.set(handover);

        StepVerifier.create(handover.catchUp(source(List.of("R1"), false))).expectNext(true).expectComplete().verify(Duration.ofSeconds(10));

        assertThat(answers).containsExactly(true);
        awaitSize(log, 2);
        StepVerifier.create(handover.accept("L1")).expectComplete().verify(Duration.ofSeconds(5));
        assertThat(log).containsExactly("R1", "S1", "L1");
    }

    // The fold got true before the replay ran, so nothing waits for the replay's outcome. Its failure starts the
    // handover failing like any failed catch-up, so both markers are forgotten and a later payload is refused.
    @Test
    void a_catch_up_a_live_fold_got_true_from_starts_the_handover_failing_when_its_replay_fails() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Boolean> answers = new CopyOnWriteArrayList<>();
        FakeSource live = source(List.of(), true);
        FakeSource failingReplay = source(List.of("R1"), false);
        failingReplay.replayFailure = new IllegalStateException("replay boom");
        AtomicReference<ReactiveHandover<String, String>> self = new AtomicReference<>();
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> {
            if (payload.equals("L1")) {
                answers.add(self.get().catchUp(failingReplay).block(Duration.ofSeconds(5)));
            }
            log.add(payload);
        }), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        self.set(handover);
        StepVerifier.create(handover.catchUp(live)).expectNext(true).verifyComplete();

        handover.accept("L1").subscribeOn(Schedulers.boundedElastic()).toFuture().get(10, TimeUnit.SECONDS);

        assertThat(answers).containsExactly(true);
        await().atMost(Duration.ofSeconds(5)).until(handover::refusesPermanently);
        assertThat(failingReplay.forgetCaughtUpCallCount()).isEqualTo(1);
        assertThat(live.forgetCaughtUpCallCount()).isEqualTo(1);
        StepVerifier.create(handover.accept("L2"))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                        .hasRootCauseMessage("replay boom"))
                .verify(Duration.ofSeconds(5));
        assertThat(log).containsExactly("L1");
    }

    // The outer replay's replayStarted() blocks on a catch-up whose replay needs the turn the outer one holds until it
    // has gone live.
    @Test
    void a_catch_up_that_replays_a_source_callback_blocks_on_answers_true_and_replays_after_the_running_catch_up() {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Boolean> answers = new CopyOnWriteArrayList<>();
        AtomicReference<ReactiveHandover<String, String>> self = new AtomicReference<>();
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> log.add(payload)),
                payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        self.set(handover);
        FakeSource outer = source(List.of("R1"), false);
        outer.onReplayStarted = () -> answers.add(self.get().catchUp(source(List.of("S1"), false)).block(Duration.ofSeconds(5)));

        StepVerifier.create(handover.catchUp(outer)).expectNext(true).expectComplete().verify(Duration.ofSeconds(10));

        assertThat(answers).containsExactly(true);
        await().atMost(Duration.ofSeconds(5)).until(() -> log.size() == 2);
        assertThat(log).containsExactly("R1", "S1");
    }

    // The fold got true before the replay ran, so a stop of that replay is not reported to it. The handover was live
    // before, so it goes on delivering once the stopped replay has let go of live delivery.
    @Test
    void a_catch_up_a_live_fold_got_true_from_that_is_stopped_leaves_the_handover_live() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Boolean> answers = new CopyOnWriteArrayList<>();
        FakeSource history = source(List.of("R1", "R2"), false);
        history.stopAfter(1);
        AtomicReference<ReactiveHandover<String, String>> self = new AtomicReference<>();
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> {
            if (payload.equals("L1")) {
                answers.add(self.get().catchUp(history).block(Duration.ofSeconds(5)));
            }
            log.add(payload);
        }), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        self.set(handover);
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();

        handover.accept("L1").subscribeOn(Schedulers.boundedElastic()).toFuture().get(10, TimeUnit.SECONDS);

        assertThat(answers).containsExactly(true);
        await().atMost(Duration.ofSeconds(5)).until(() -> history.replayAbandonedCallCount == 1);
        StepVerifier.create(handover.accept("L2")).expectComplete().verify(Duration.ofSeconds(5));
        assertThat(history.markCaughtUpCallCount()).isZero();
        assertThat(handover.refusesPermanently()).isFalse();
        assertThat(log).containsExactly("L1", "R1", "L2");
    }

    // historyDone() blocks on a catch-up whose replay needs the turn the outer catch-up holds until its drain ends.
    @Test
    void a_catch_up_that_replays_history_done_blocks_on_answers_true_and_replays_after_the_running_catch_up() {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Boolean> answers = new CopyOnWriteArrayList<>();
        AtomicReference<ReactiveHandover<String, String>> self = new AtomicReference<>();
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> log.add(payload)),
                payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        self.set(handover);
        FakeSource outer = source(List.of("R1"), false);
        outer.onHistoryDone = () -> answers.add(self.get().catchUp(source(List.of("S1"), false)).block(Duration.ofSeconds(5)));

        StepVerifier.create(handover.catchUp(outer)).expectNext(true).expectComplete().verify(Duration.ofSeconds(10));

        assertThat(answers).containsExactly(true);
        await().atMost(Duration.ofSeconds(5)).until(() -> log.size() == 2);
        StepVerifier.create(handover.accept("L1")).expectComplete().verify(Duration.ofSeconds(5));
        assertThat(log).containsExactly("R1", "S1", "L1");
    }

    // Nothing was buffered, so liveDrained() runs right after the marker is written, before the outer catch-up gives
    // back the turn the blocked-on catch-up needs.
    @Test
    void a_catch_up_that_replays_live_drained_blocks_on_with_nothing_buffered_answers_true_and_replays_after_the_running_catch_up() {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Boolean> answers = new CopyOnWriteArrayList<>();
        AtomicReference<ReactiveHandover<String, String>> self = new AtomicReference<>();
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> log.add(payload)),
                payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        self.set(handover);
        FakeSource outer = source(List.of("R1"), false);
        outer.onLiveDrained = () -> answers.add(self.get().catchUp(source(List.of("S1"), false)).block(Duration.ofSeconds(5)));

        StepVerifier.create(handover.catchUp(outer)).expectNext(true).expectComplete().verify(Duration.ofSeconds(10));

        assertThat(answers).containsExactly(true);
        await().atMost(Duration.ofSeconds(5)).until(() -> log.size() == 2);
        StepVerifier.create(handover.accept("L1")).expectComplete().verify(Duration.ofSeconds(5));
        assertThat(log).containsExactly("R1", "S1", "L1");
    }

    // L1 arrives during the replay, so liveDrained() runs after L1's delivery, before the outer catch-up gives back
    // the turn the blocked-on catch-up needs.
    @Test
    void a_catch_up_that_replays_live_drained_blocks_on_after_a_buffered_payload_answers_true_and_replays_after_the_running_catch_up() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Boolean> answers = new CopyOnWriteArrayList<>();
        AtomicReference<ReactiveHandover<String, String>> self = new AtomicReference<>();
        AtomicReference<CompletableFuture<Boolean>> liveAck = new AtomicReference<>();
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> {
            if (payload.equals("R1")) {
                liveAck.set(offeredFromOutside(() -> self.get().acceptReportingDelivery("L1")));
            }
            log.add(payload);
        }), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        self.set(handover);
        FakeSource outer = source(List.of("R1"), false);
        outer.onLiveDrained = () -> answers.add(self.get().catchUp(source(List.of("S1"), false)).block(Duration.ofSeconds(5)));

        StepVerifier.create(handover.catchUp(outer)).expectNext(true).expectComplete().verify(Duration.ofSeconds(10));

        assertThat(liveAck.get().get(10, TimeUnit.SECONDS)).isTrue();
        await().atMost(Duration.ofSeconds(10)).until(() -> log.size() == 3);
        assertThat(answers).containsExactly(true);
        assertThat(log).containsExactly("R1", "L1", "S1");
    }

    // L1's fold asks twice before the replay it asked for can start, from two sources the way a feed builds one per
    // call, so the second call adds nothing the first replay does not already deliver.
    @Test
    void two_catch_ups_a_live_fold_asks_for_before_the_replay_starts_replay_once() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Boolean> answers = new CopyOnWriteArrayList<>();
        FakeSource first = source(List.of("R1", "R2"), false);
        FakeSource second = source(List.of("R1", "R2"), false);
        AtomicReference<ReactiveHandover<String, String>> self = new AtomicReference<>();
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> {
            if (payload.equals("L1")) {
                answers.add(self.get().catchUp(first).block(Duration.ofSeconds(5)));
                answers.add(self.get().catchUp(second).block(Duration.ofSeconds(5)));
            }
            log.add(payload);
        }), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        self.set(handover);
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();

        handover.accept("L1").subscribeOn(Schedulers.boundedElastic()).toFuture().get(10, TimeUnit.SECONDS);

        assertThat(answers).containsExactly(true, true);
        await().atMost(Duration.ofSeconds(5)).until(() -> log.size() >= 3);
        StepVerifier.create(handover.accept("L2")).expectComplete().verify(Duration.ofSeconds(5));
        assertThat(log).containsExactly("L1", "R1", "R2", "L2");
        assertThat(first.replayCallCount + second.replayCallCount).isEqualTo(1);
    }

    // L1's fold asks for N1 from a timer thread, so only the Mono being part of the fold's own tells the handover
    // where the call came from. N1 is delivered after L1, and the fold does not wait for that.
    @Test
    void a_payload_a_live_fold_feeds_back_after_switching_threads_is_delivered_after_that_fold_without_waiting_for_it() {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Boolean> answers = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of("L1", self -> Mono.delay(Duration.ofMillis(1))
                .then(Mono.defer(() -> self.acceptReportingDelivery("N1")))
                .doOnNext(answers::add)));
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();

        StepVerifier.create(handover.accept("L1")).expectComplete().verify(Duration.ofSeconds(5));

        assertThat(answers).containsExactly(true);
        awaitSize(log, 2);
        assertThat(log).containsExactly("L1", "N1");
    }

    @Test
    void a_payload_a_live_fold_blocks_on_feeding_back_is_delivered_after_that_fold() {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Boolean> answers = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of("L1", self ->
                Mono.fromRunnable(() -> answers.add(self.acceptReportingDelivery("N1").block(Duration.ofSeconds(5))))));
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();

        StepVerifier.create(handover.accept("L1")).expectComplete().verify(Duration.ofSeconds(10));

        assertThat(answers).containsExactly(true);
        awaitSize(log, 2);
        assertThat(log).containsExactly("L1", "N1");
    }

    @Test
    void a_payload_a_replayed_fold_feeds_back_is_delivered_after_the_replay() {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Boolean> answers = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of("R1", self -> Mono.delay(Duration.ofMillis(1))
                .then(Mono.defer(() -> self.acceptReportingDelivery("N1")))
                .doOnNext(answers::add)));

        StepVerifier.create(handover.catchUp(source(List.of("R1", "R2"), false))).expectNext(true).expectComplete().verify(Duration.ofSeconds(5));

        assertThat(answers).containsExactly(true);
        awaitSize(log, 3);
        assertThat(log).containsExactly("R1", "R2", "N1");
    }

    // N1 is fed while N2 is still to come from L1, and N3 is fed from N1's own fold, so it goes behind N2.
    @Test
    void payloads_fed_back_from_folds_are_delivered_in_the_order_they_were_fed() {
        List<String> log = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of(
                "L1", self -> self.acceptReportingDelivery("N1").then(self.acceptReportingDelivery("N2")),
                "N1", self -> self.acceptReportingDelivery("N3")));
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();

        StepVerifier.create(handover.accept("L1")).expectComplete().verify(Duration.ofSeconds(5));

        awaitSize(log, 4);
        assertThat(log).containsExactly("L1", "N1", "N2", "N3");
    }

    @Test
    void accept_from_a_live_fold_completes_once_queued_and_its_payload_is_delivered_after_that_fold() {
        List<String> log = new CopyOnWriteArrayList<>();
        List<String> answers = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of("L1", self ->
                self.accept("N1").doOnSuccess(ignored -> answers.add("N1 queued"))));
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();

        StepVerifier.create(handover.accept("L1")).expectComplete().verify(Duration.ofSeconds(5));

        assertThat(answers).containsExactly("N1 queued");
        awaitSize(log, 2);
        assertThat(log).containsExactly("L1", "N1");
    }

    // The 0.33.0 shape of a fold feeding its own handover, subscribed and not waited for.
    @Test
    void accept_a_live_fold_subscribes_and_leaves_has_its_payload_delivered_after_that_fold() {
        List<String> log = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of("L1", self ->
                Mono.fromRunnable(() -> self.accept("N1").subscribe())));
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();

        StepVerifier.create(handover.accept("L1")).expectComplete().verify(Duration.ofSeconds(5));

        awaitSize(log, 2);
        assertThat(log).containsExactly("L1", "N1");
    }

    @Test
    void accept_from_a_replayed_fold_completes_once_queued_and_its_payload_is_delivered_after_the_replay() {
        List<String> log = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of("R1", self -> self.accept("N1")));

        StepVerifier.create(handover.catchUp(source(List.of("R1", "R2"), false))).expectNext(true).expectComplete().verify(Duration.ofSeconds(5));

        awaitSize(log, 3);
        assertThat(log).containsExactly("R1", "R2", "N1");
    }

    // N1 folds on another thread after a delay, so L1 is only answered after it when the answer waits for N1.
    @Test
    void the_answer_to_a_payload_whose_fold_composes_in_a_fed_payload_comes_once_that_payload_is_folded() {
        List<String> log = new CopyOnWriteArrayList<>();
        AtomicReference<List<String>> loggedWhenAnswered = new AtomicReference<>();
        ReactiveHandover<String, String> handover = handoverFolding(CatchupThenLiveOptions.defaults(), (self, payload) -> switch (payload) {
            case "L1" -> self.accept("N1").then(Mono.fromRunnable(() -> log.add("L1")));
            case "N1" -> Mono.delay(Duration.ofMillis(100)).then(Mono.fromRunnable(() -> log.add("N1")));
            default -> Mono.fromRunnable(() -> log.add(payload));
        });
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();

        StepVerifier.create(handover.accept("L1").doOnSuccess(ignored -> loggedWhenAnswered.set(List.copyOf(log))))
                .expectComplete().verify(Duration.ofSeconds(5));

        assertThat(loggedWhenAnswered.get()).containsExactly("L1", "N1");
    }

    @Test
    void the_answer_to_a_payload_whose_fold_subscribes_a_fed_payload_comes_once_that_payload_is_folded() {
        List<String> log = new CopyOnWriteArrayList<>();
        AtomicReference<List<String>> loggedWhenAnswered = new AtomicReference<>();
        ReactiveHandover<String, String> handover = handoverFolding(CatchupThenLiveOptions.defaults(), (self, payload) -> switch (payload) {
            case "L1" -> Mono.fromRunnable(() -> self.accept("N1").subscribe()).then(Mono.fromRunnable(() -> log.add("L1")));
            case "N1" -> Mono.delay(Duration.ofMillis(100)).then(Mono.fromRunnable(() -> log.add("N1")));
            default -> Mono.fromRunnable(() -> log.add(payload));
        });
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();

        StepVerifier.create(handover.accept("L1").doOnSuccess(ignored -> loggedWhenAnswered.set(List.copyOf(log))))
                .expectComplete().verify(Duration.ofSeconds(5));

        assertThat(loggedWhenAnswered.get()).containsExactly("L1", "N1");
    }

    // L1 is not recorded as delivered while N1 failed, so its redelivery folds it again, and the fixed N1 with it.
    @Test
    void a_fed_payload_whose_fold_fails_fails_the_answer_of_the_payload_that_fed_it_and_that_payload_is_folded_again_when_redelivered() {
        List<String> log = new CopyOnWriteArrayList<>();
        AtomicBoolean n1Fails = new AtomicBoolean(true);
        ReactiveHandover<String, String> handover = handoverFolding(CatchupThenLiveOptions.defaults(), (self, payload) -> switch (payload) {
            case "L1" -> self.accept("N1").then(Mono.fromRunnable(() -> log.add("L1")));
            case "N1" -> Mono.delay(Duration.ofMillis(20)).then(Mono.defer(() -> n1Fails.get()
                    ? Mono.<Void>error(new IllegalStateException("fold failed for N1"))
                    : Mono.fromRunnable(() -> log.add("N1"))));
            default -> Mono.fromRunnable(() -> log.add(payload));
        });
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();

        StepVerifier.create(handover.accept("L1"))
                .expectErrorSatisfies(error -> assertThat(error).isInstanceOf(IllegalStateException.class).hasMessage("fold failed for N1"))
                .verify(Duration.ofSeconds(5));
        assertThat(handover.refusesPermanently()).isFalse();
        assertThat(log).containsExactly("L1");

        n1Fails.set(false);
        StepVerifier.create(handover.accept("L1")).expectComplete().verify(Duration.ofSeconds(5));
        assertThat(log).containsExactly("L1", "L1", "N1");

        StepVerifier.create(handover.accept("L1")).expectComplete().verify(Duration.ofSeconds(5));
        assertThat(log).containsExactly("L1", "L1", "N1");
        assertThat(handover.refusesPermanently()).isFalse();
    }

    // The fold swallows the refusal of N2, which the cap leaves no room for while L1 and N1 are still counted, so it
    // is the handover that fails the answer to L1, after N1 is folded.
    @Test
    void a_fed_payload_refused_for_a_full_buffer_fails_the_answer_of_the_payload_that_fed_it() {
        List<String> log = new CopyOnWriteArrayList<>();
        AtomicReference<List<String>> loggedWhenAnswered = new AtomicReference<>();
        ReactiveHandover<String, String> handover = handoverFolding(new CatchupThenLiveOptions(10, 2), (self, payload) -> switch (payload) {
            case "L1" -> Flux.just("N1", "N2").concatMap(next -> self.accept(next).onErrorComplete())
                    .then(Mono.fromRunnable(() -> log.add("L1")));
            default -> Mono.fromRunnable(() -> log.add(payload));
        });
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();

        StepVerifier.create(handover.accept("L1").doOnError(ignored -> loggedWhenAnswered.set(List.copyOf(log))))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                        .hasMessage(HandoverMessages.bufferOverflow(2)))
                .verify(Duration.ofSeconds(5));

        assertThat(loggedWhenAnswered.get()).containsExactly("L1", "N1");
        assertThat(handover.refusesPermanently()).isFalse();
    }

    // L1 has a copy of itself offered from outside while its fold runs, so the copy is taken up while N1 is still to be
    // folded.
    @Test
    void a_copy_of_a_payload_offered_from_outside_is_answered_once_that_payload_and_what_it_fed_are_folded() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        AtomicReference<CompletableFuture<Void>> copy = new AtomicReference<>();
        AtomicReference<List<String>> loggedWhenCopyAnswered = new AtomicReference<>();
        ReactiveHandover<String, String> handover = handoverFolding(CatchupThenLiveOptions.defaults(), (self, payload) -> switch (payload) {
            case "L1" -> Mono.defer(() -> {
                copy.set(offeredFromOutside(() -> self.accept("L1").doOnSuccess(ignored -> loggedWhenCopyAnswered.set(List.copyOf(log)))));
                return self.accept("N1");
            }).then(Mono.fromRunnable(() -> log.add("L1")));
            case "N1" -> Mono.delay(Duration.ofMillis(100)).then(Mono.fromRunnable(() -> log.add("N1")));
            default -> Mono.fromRunnable(() -> log.add(payload));
        });
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();

        StepVerifier.create(handover.accept("L1")).expectComplete().verify(Duration.ofSeconds(5));
        copy.get().get(5, TimeUnit.SECONDS);

        assertThat(loggedWhenCopyAnswered.get()).containsExactly("L1", "N1");
        assertThat(log).containsExactly("L1", "N1");
    }

    @Test
    void a_copy_of_a_payload_offered_from_outside_fails_with_the_payload_when_what_it_fed_fails() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        AtomicReference<CompletableFuture<Void>> copy = new AtomicReference<>();
        ReactiveHandover<String, String> handover = handoverFolding(CatchupThenLiveOptions.defaults(), (self, payload) -> switch (payload) {
            case "L1" -> Mono.defer(() -> {
                copy.set(offeredFromOutside(() -> self.accept("L1")));
                return self.accept("N1");
            }).then(Mono.fromRunnable(() -> log.add("L1")));
            case "N1" -> Mono.delay(Duration.ofMillis(100)).then(Mono.error(new IllegalStateException("fold failed for N1")));
            default -> Mono.fromRunnable(() -> log.add(payload));
        });
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();

        StepVerifier.create(handover.accept("L1"))
                .expectErrorSatisfies(error -> assertThat(error).isInstanceOf(IllegalStateException.class).hasMessage("fold failed for N1"))
                .verify(Duration.ofSeconds(5));

        assertThat(catchThrowable(() -> copy.get().get(5, TimeUnit.SECONDS)))
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("fold failed for N1");
        assertThat(log).containsExactly("L1");
        assertThat(handover.refusesPermanently()).isFalse();
    }

    // Each payload in the chain holds back the answer of the one before it, so a chain the cap does not stop would
    // leave the answer to L1 waiting for good. The chain ends at N100 so that a handover without the cap fails the
    // assertion instead of running out of memory.
    @Test
    void a_fold_that_feeds_new_payloads_without_end_fails_the_answer_to_the_payload_the_chain_started_from() {
        ReactiveHandover<String, String> handover = handoverFolding(new CatchupThenLiveOptions(10, 3), (self, payload) -> {
            int next = payload.equals("L1") ? 1 : Integer.parseInt(payload.substring(1)) + 1;
            return next > 100 ? Mono.empty() : self.accept("N" + next);
        });
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();

        StepVerifier.create(handover.accept("L1"))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                        .hasMessage(HandoverMessages.fedChainTooLong("test payload", 3)))
                .verify(Duration.ofSeconds(5));

        assertThat(handover.refusesPermanently()).isFalse();
    }

    @Test
    void a_fold_that_feeds_its_own_payload_again_is_answered_and_folds_it_once() {
        List<String> log = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverFolding(CatchupThenLiveOptions.defaults(), (self, payload) ->
                self.accept("L1").then(Mono.fromRunnable(() -> log.add(payload))));
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();

        StepVerifier.create(handover.accept("L1")).expectComplete().verify(Duration.ofSeconds(5));

        assertThat(log).containsExactly("L1");
        StepVerifier.create(handover.accept("L1")).expectComplete().verify(Duration.ofSeconds(5));
        assertThat(log).containsExactly("L1");
    }

    // Nobody waits for a payload liveDrained() feeds, so its failure is not handed to a caller and the handover starts
    // failing.
    @Test
    void a_payload_a_source_callback_feeds_that_fails_makes_the_handover_start_failing() {
        List<String> log = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of());
        FakeSource source = source(List.of("R1"), false);
        source.onLiveDrained = () -> handover.accept("bad1").subscribe();

        StepVerifier.create(handover.catchUp(source)).expectNext(true).verifyComplete();

        await().atMost(Duration.ofSeconds(5)).until(handover::refusesPermanently);
        StepVerifier.create(handover.accept("L2"))
                .expectErrorSatisfies(error -> assertThat(error)
                        .hasMessage(HandoverMessages.queuedEventFailed("test payload"))
                        .hasRootCauseMessage("fold failed for bad1"))
                .verify(Duration.ofSeconds(5));
        assertThat(source.forgetCaughtUpCallCount()).isEqualTo(1);
    }

    // A catch-up that replays nothing runs historyDone() beside live delivery, so what it feeds goes to the live sink.
    @Test
    void a_payload_history_done_feeds_when_nothing_is_replayed_and_that_fails_makes_the_handover_start_failing() {
        List<String> log = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of());
        FakeSource source = source(List.of(), true);
        source.onHistoryDone = () -> handover.accept("bad1").subscribe();

        StepVerifier.create(handover.catchUp(source)).expectNext(true).verifyComplete();

        await().atMost(Duration.ofSeconds(5)).until(handover::refusesPermanently);
        StepVerifier.create(handover.accept("L2"))
                .expectErrorSatisfies(error -> assertThat(error)
                        .hasMessage(HandoverMessages.queuedEventFailed("test payload"))
                        .hasRootCauseMessage("fold failed for bad1"))
                .verify(Duration.ofSeconds(5));
        assertThat(source.forgetCaughtUpCallCount()).isEqualTo(1);
    }

    @Test
    void accept_if_live_from_a_live_fold_is_refused_and_its_payload_is_never_delivered() {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Throwable> refusals = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of("L1", self ->
                self.acceptIfLive("N1").doOnError(refusals::add).onErrorComplete()));
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();

        StepVerifier.create(handover.accept("L1")).expectComplete().verify(Duration.ofSeconds(5));
        StepVerifier.create(handover.accept("L2")).expectComplete().verify(Duration.ofSeconds(5));

        assertThat(refusals).singleElement(InstanceOfAssertFactories.THROWABLE)
                .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                .hasMessage(HandoverMessages.acceptedOrDeferredFromOwnDelivery("test payload"));
        assertThat(log).containsExactly("L1", "L2");
    }

    // The handover is not live while it replays, and a fold still gets the refusal rather than an answer for either.
    @Test
    void accept_if_live_from_a_replayed_fold_is_refused_and_its_payload_is_never_delivered() {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Throwable> refusals = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of("R1", self ->
                self.acceptIfLive("N1").doOnError(refusals::add).onErrorComplete()));

        StepVerifier.create(handover.catchUp(source(List.of("R1"), false))).expectNext(true).expectComplete().verify(Duration.ofSeconds(5));
        StepVerifier.create(handover.accept("L1")).expectComplete().verify(Duration.ofSeconds(5));

        assertThat(refusals).singleElement(InstanceOfAssertFactories.THROWABLE)
                .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                .hasMessage(HandoverMessages.acceptedOrDeferredFromOwnDelivery("test payload"));
        assertThat(log).containsExactly("R1", "L1");
    }

    @Test
    void a_refused_accept_if_live_from_a_live_fold_leaves_the_handover_serving_and_a_later_call_from_outside_answers_true_once_delivered() {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Throwable> refusals = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of("L1", self ->
                self.acceptIfLive("N1").doOnError(refusals::add).onErrorComplete()));
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();

        StepVerifier.create(handover.accept("L1")).expectComplete().verify(Duration.ofSeconds(5));
        StepVerifier.create(handover.acceptIfLive("L2")).expectNext(true).verifyComplete();

        assertThat(refusals).hasSize(1);
        assertThat(handover.refusesPermanently()).isFalse();
        assertThat(log).as("L2 is delivered by the time it is answered, N1 never is").containsExactly("L1", "L2");
    }

    @Test
    void a_refused_accept_if_live_from_a_replayed_fold_leaves_the_handover_serving_and_a_later_call_from_outside_answers_true_once_delivered() {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Throwable> refusals = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of("R1", self ->
                self.acceptIfLive("N1").doOnError(refusals::add).onErrorComplete()));
        StepVerifier.create(handover.catchUp(source(List.of("R1"), false))).expectNext(true).expectComplete().verify(Duration.ofSeconds(5));

        StepVerifier.create(handover.acceptIfLive("L1")).expectNext(true).verifyComplete();

        assertThat(refusals).hasSize(1);
        assertThat(handover.refusesPermanently()).isFalse();
        assertThat(log).as("L1 is delivered by the time it is answered, N1 never is").containsExactly("R1", "L1");
    }

    // bad1 and Y were both answered once queued, so Y is delivered although bad1 failed, and only then does the
    // handover fail for good.
    @Test
    void a_payload_queued_behind_one_whose_delivery_failed_is_delivered_before_the_handover_fails_for_good() {
        List<String> log = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of());
        FakeSource source = source(List.of("R1"), false);
        source.onLiveDrained = () -> {
            handover.accept("bad1").subscribe();
            handover.accept("Y").subscribe();
        };
        StepVerifier.create(handover.catchUp(source)).expectNext(true).verifyComplete();

        awaitSize(log, 2);
        assertThat(log).containsExactly("R1", "Y");
        assertThatALaterPayloadIsRefusedFor(handover, "bad1");
        assertThat(source.forgetCaughtUpCallCount()).isEqualTo(1);
    }

    @Test
    void a_payload_a_delivery_feeds_back_while_the_handover_is_failing_is_delivered_too() {
        List<String> log = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of("Y", self -> self.accept("Z")));
        FakeSource source = source(List.of("R1"), false);
        source.onLiveDrained = () -> {
            handover.accept("bad1").subscribe();
            handover.accept("Y").subscribe();
        };
        StepVerifier.create(handover.catchUp(source)).expectNext(true).verifyComplete();

        awaitSize(log, 3);
        assertThat(log).containsExactly("R1", "Y", "Z");
        assertThatALaterPayloadIsRefusedFor(handover, "bad1");
    }

    @Test
    void a_second_failure_while_the_handover_is_failing_leaves_the_payloads_behind_it_delivered() {
        List<String> log = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of());
        FakeSource source = source(List.of("R1"), false);
        source.onLiveDrained = () -> {
            handover.accept("bad1").subscribe();
            handover.accept("bad2").subscribe();
            handover.accept("Y").subscribe();
        };
        StepVerifier.create(handover.catchUp(source)).expectNext(true).verifyComplete();

        awaitSize(log, 2);
        assertThat(log).containsExactly("R1", "Y");
        assertThatALaterPayloadIsRefusedFor(handover, "bad1");
    }

    // Y's fold is held while the handover is failing, so E arrives from outside with Y still to be delivered. The
    // marker is forgotten before E is refused, so a caller that reacts to the refusal finds it gone.
    @Test
    void a_payload_from_outside_is_refused_with_the_failure_while_the_handover_delivers_what_it_took_in() throws InterruptedException {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch deliveringY = new CountDownLatch(1);
        CountDownLatch releaseY = new CountDownLatch(1);
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of("Y", self -> Mono.fromRunnable(() -> {
            deliveringY.countDown();
            awaitQuietly(releaseY);
        }).subscribeOn(Schedulers.boundedElastic())));
        FakeSource source = source(List.of("R1"), false);
        source.onLiveDrained = () -> {
            handover.accept("bad1").subscribe();
            handover.accept("Y").subscribe();
        };
        StepVerifier.create(handover.catchUp(source)).expectNext(true).verifyComplete();
        assertThat(deliveringY.await(5, TimeUnit.SECONDS)).isTrue();

        CompletableFuture<Void> fromOutside = handover.accept("E").toFuture();

        assertThat(fromOutside).isCompletedExceptionally();
        assertThat(source.forgetCaughtUpCallCount()).isEqualTo(1);
        releaseY.countDown();
        assertThat(catchThrowable(() -> fromOutside.get(5, TimeUnit.SECONDS)))
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("fold failed for bad1");
        awaitSize(log, 2);
        assertThat(log).containsExactly("R1", "Y");
    }

    // Whether a call comes from this handover's own code is asked when its Mono is subscribed, not when it is built.
    // The fold blocks on a Mono the test built, on the thread the handover called the fold on.
    @Test
    void accept_built_outside_a_fold_and_blocked_on_inside_it_completes_once_queued() {
        List<String> log = new CopyOnWriteArrayList<>();
        AtomicReference<Mono<Void>> builtOutside = new AtomicReference<>();
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of("L1", self ->
                Mono.fromRunnable(() -> builtOutside.get().block(Duration.ofSeconds(2)))));
        builtOutside.set(handover.accept("N1"));
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();

        StepVerifier.create(handover.accept("L1")).expectComplete().verify(Duration.ofSeconds(5));

        awaitSize(log, 2);
        assertThat(log).containsExactly("L1", "N1");
    }

    // N1 is already reported queued when the replay stops, so the stop keeps it rather than dropping it, and the next
    // catch-up that goes live delivers it.
    @Test
    void a_payload_a_replayed_fold_fed_back_survives_a_stop_and_is_delivered_once_a_later_catch_up_goes_live() {
        List<String> log = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of("R1", self -> self.acceptReportingDelivery("N1")));
        FakeSource stoppedAfterR1 = source(List.of("R1", "R2"), false);
        stoppedAfterR1.stopAfter(1);

        StepVerifier.create(handover.catchUp(stoppedAfterR1)).expectNext(false).expectComplete().verify(Duration.ofSeconds(5));
        assertThat(log).containsExactly("R1");

        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).expectComplete().verify(Duration.ofSeconds(5));
        awaitSize(log, 2);
        assertThat(log).containsExactly("R1", "N1");
    }

    // E is offered from outside while the source callback that fed bad1 runs, so it is queued behind bad1 and its caller is still waiting when
    // bad1 fails. A failed queued payload does not throw away the live view, so E is delivered and its caller told.
    @Test
    void a_payload_from_outside_queued_before_a_queued_payload_failed_is_still_delivered() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        AtomicReference<CompletableFuture<Void>> fromOutside = new AtomicReference<>();
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of());
        FakeSource source = source(List.of("R1"), false);
        source.onLiveDrained = () -> {
            handover.accept("bad1").subscribe();
            fromOutside.set(offeredFromOutside(() -> handover.accept("E")));
        };
        StepVerifier.create(handover.catchUp(source)).expectNext(true).verifyComplete();

        fromOutside.get().get(5, TimeUnit.SECONDS);
        assertThat(log).containsExactly("R1", "E");
        assertThatALaterPayloadIsRefusedFor(handover, "bad1");
    }

    // N1 is answered true once queued during the replay, before any catch-up took the live sink, so the failed replay
    // is what has to deliver it before the handover fails for good.
    @Test
    void a_payload_a_replayed_fold_fed_back_is_delivered_when_a_later_replayed_payload_fails() {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Boolean> answers = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of("R1", self ->
                self.acceptReportingDelivery("N1").doOnNext(answers::add)));
        FakeSource source = source(List.of("R1", "bad2"), false);

        StepVerifier.create(handover.catchUp(source)).expectErrorMessage("fold failed for bad2").verify(Duration.ofSeconds(5));

        awaitSize(log, 2);
        assertThat(answers).containsExactly(true);
        assertThat(log).containsExactly("R1", "N1");
        assertThat(source.forgetCaughtUpCallCount()).isEqualTo(1);
        assertThatALaterPayloadIsRefusedFor(handover, "bad2");
    }

    // L1's fold feeds N1 and is held until the second replay waits for it, so N1 is still queued when that replay fails.
    @Test
    void a_payload_a_live_fold_fed_back_is_delivered_when_a_replay_waiting_for_that_fold_fails() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Boolean> answers = new CopyOnWriteArrayList<>();
        CountDownLatch foldingL1 = new CountDownLatch(1);
        CountDownLatch releaseL1 = new CountDownLatch(1);
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of("L1", self ->
                self.acceptReportingDelivery("N1").doOnNext(answers::add).then(Mono.fromRunnable(() -> {
                    foldingL1.countDown();
                    awaitQuietly(releaseL1);
                }))));
        FakeSource live = source(List.of(), true);
        StepVerifier.create(handover.catchUp(live)).expectNext(true).verifyComplete();
        CompletableFuture<Void> l1 = handover.accept("L1").subscribeOn(Schedulers.boundedElastic()).toFuture();
        assertThat(foldingL1.await(5, TimeUnit.SECONDS)).isTrue();
        CountDownLatch replayWaiting = new CountDownLatch(1);
        FakeSource failing = source(List.of("bad3"), false);
        failing.onCaughtUpChecked = replayWaiting::countDown;
        CompletableFuture<Boolean> replay = handover.catchUp(failing).toFuture();
        assertThat(replayWaiting.await(5, TimeUnit.SECONDS)).isTrue();

        releaseL1.countDown();

        assertThat(catchThrowable(() -> replay.get(5, TimeUnit.SECONDS))).hasRootCauseMessage("fold failed for bad3");
        awaitSize(log, 2);
        assertThat(answers).containsExactly(true);
        assertThat(log).containsExactly("L1", "N1");
        l1.get(5, TimeUnit.SECONDS);
        assertThat(failing.forgetCaughtUpCallCount()).isEqualTo(1);
        assertThat(live.forgetCaughtUpCallCount()).isEqualTo(1);
        assertThatALaterPayloadIsRefusedFor(handover, "bad3");
    }

    // The marker read fails while L1's fold, which fed N1, is still running, so N1 is queued when the handover starts
    // failing. The marker is forgotten, so the catch-up of whatever replaces the handover replays the history.
    @Test
    void a_payload_a_live_fold_fed_back_is_delivered_when_a_catch_up_fails_to_read_the_marker() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Boolean> answers = new CopyOnWriteArrayList<>();
        CountDownLatch foldingL1 = new CountDownLatch(1);
        CountDownLatch releaseL1 = new CountDownLatch(1);
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of("L1", self ->
                self.acceptReportingDelivery("N1").doOnNext(answers::add).then(Mono.fromRunnable(() -> {
                    foldingL1.countDown();
                    awaitQuietly(releaseL1);
                }))));
        FakeSource live = source(List.of(), true);
        StepVerifier.create(handover.catchUp(live)).expectNext(true).verifyComplete();
        CompletableFuture<Void> l1 = handover.accept("L1").subscribeOn(Schedulers.boundedElastic()).toFuture();
        assertThat(foldingL1.await(5, TimeUnit.SECONDS)).isTrue();
        FakeSource unreadable = source(List.of(), true);
        unreadable.caughtUpFailure = new IllegalStateException("marker read failed");

        StepVerifier.create(handover.catchUp(unreadable)).expectErrorMessage("marker read failed").verify(Duration.ofSeconds(5));
        releaseL1.countDown();

        awaitSize(log, 2);
        assertThat(answers).containsExactly(true);
        assertThat(log).containsExactly("L1", "N1");
        l1.get(5, TimeUnit.SECONDS);
        assertThat(unreadable.forgetCaughtUpCallCount()).isEqualTo(1);
        assertThat(live.forgetCaughtUpCallCount()).isEqualTo(1);
        StepVerifier.create(handover.accept("L2"))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                        .hasRootCauseMessage("marker read failed"))
                .verify(Duration.ofSeconds(5));
    }

    // Y's fold is still running, so the handover is failing and has not failed for good yet. A catch-up asked for then
    // would replay, write back the marker the failure deleted, and report live a handover that refuses every event.
    @Test
    void a_catch_up_asked_for_while_the_handover_is_failing_is_refused_and_writes_no_marker() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch deliveringY = new CountDownLatch(1);
        CountDownLatch releaseY = new CountDownLatch(1);
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of("Y", self -> Mono.fromRunnable(() -> {
            deliveringY.countDown();
            awaitQuietly(releaseY);
        }).subscribeOn(Schedulers.boundedElastic())));
        FakeSource first = source(List.of("R1"), false);
        first.onLiveDrained = () -> {
            handover.accept("bad1").subscribe();
            handover.accept("Y").subscribe();
        };
        StepVerifier.create(handover.catchUp(first)).expectNext(true).verifyComplete();
        assertThat(deliveringY.await(5, TimeUnit.SECONDS)).isTrue();
        FakeSource another = source(List.of("R9"), false);

        CompletableFuture<Boolean> catchUp = handover.catchUp(another).toFuture();
        releaseY.countDown();

        assertThat(catchThrowable(() -> catchUp.get(5, TimeUnit.SECONDS)))
                .hasCauseInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                .hasRootCauseMessage("fold failed for bad1");
        awaitSize(log, 2);
        assertThat(another.markCaughtUpCallCount()).isZero();
        assertThat(another.replayCallCount).isZero();
    }

    // The failure starts while B writes its marker, so the failure's own forget runs before that write ends. B forgets
    // its marker again rather than leave it in place.
    @Test
    void a_marker_written_while_the_handover_starts_failing_is_forgotten_again() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of());
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();
        FakeSource unreadable = source(List.of(), true);
        unreadable.caughtUpFailure = new IllegalStateException("marker read failed");
        FakeSource writing = source(List.of("R1"), false);
        writing.onMarkCaughtUp = () -> assertThat(catchThrowable(() ->
                CompletableFuture.supplyAsync(() -> handover.catchUp(unreadable).toFuture()).join().get(5, TimeUnit.SECONDS)))
                .hasRootCauseMessage("marker read failed");

        StepVerifier.create(handover.catchUp(writing))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                        .hasRootCauseMessage("marker read failed"))
                .verify(Duration.ofSeconds(5));

        assertThat(writing.markCaughtUpCallCount()).isEqualTo(1);
        assertThat(writing.forgetCaughtUpCallCount()).isEqualTo(1);
    }

    // The failure starts while B writes its marker, and B's write then errors although the marker was stored. B's
    // catch-up is not the first failure, and it still forgets the marker it may have written.
    @Test
    void a_marker_write_that_errors_after_the_handover_started_failing_is_forgotten() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of());
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();
        FakeSource unreadable = source(List.of(), true);
        unreadable.caughtUpFailure = new IllegalStateException("marker read failed");
        FakeSource writing = source(List.of("R1"), false);
        writing.onMarkCaughtUp = () -> {
            assertThat(catchThrowable(() ->
                    CompletableFuture.supplyAsync(() -> handover.catchUp(unreadable).toFuture()).join().get(5, TimeUnit.SECONDS)))
                    .hasRootCauseMessage("marker read failed");
            throw new IllegalStateException("marker write failed");
        };

        StepVerifier.create(handover.catchUp(writing)).expectErrorMessage("marker write failed").verify(Duration.ofSeconds(5));

        assertThat(writing.markCaughtUpCallCount()).isEqualTo(1);
        assertThat(writing.forgetCaughtUpCallCount()).isEqualTo(1);
    }

    @Test
    void a_marker_that_cannot_be_forgotten_is_retried_and_the_handover_still_delivers_what_it_took_in() {
        List<String> log = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of());
        FakeSource source = source(List.of("R1"), false);
        source.onLiveDrained = () -> {
            handover.accept("bad1").subscribe();
            handover.accept("Y").subscribe();
        };
        source.forgetFailure = new IllegalStateException("marker delete failed");
        StepVerifier.create(handover.catchUp(source)).expectNext(true).verifyComplete();

        awaitSize(log, 2);
        assertThat(log).containsExactly("R1", "Y");
        assertThatALaterPayloadIsRefusedFor(handover, "bad1");
        assertThat(source.forgetCaughtUpCallCount()).isEqualTo(4);
    }

    // Neither marker store ever answers the delete. Each attempt gives up after the timeout, so the catch-up is
    // answered and the handover delivers what its fold fed it and fails for good, as it does when the delete errors.
    @Test
    void a_forget_that_never_completes_still_answers_the_catch_up_and_the_handover_fails_for_good() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch foldingL1 = new CountDownLatch(1);
        CountDownLatch releaseL1 = new CountDownLatch(1);
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of("L1", self ->
                self.acceptReportingDelivery("N1").then(Mono.fromRunnable(() -> {
                    foldingL1.countDown();
                    awaitQuietly(releaseL1);
                }))));
        handover.forgetAttemptTimeout(Duration.ofMillis(50));
        FakeSource live = source(List.of(), true);
        live.forgetNeverCompletes = true;
        StepVerifier.create(handover.catchUp(live)).expectNext(true).verifyComplete();
        CompletableFuture<Void> l1 = handover.accept("L1").subscribeOn(Schedulers.boundedElastic()).toFuture();
        assertThat(foldingL1.await(5, TimeUnit.SECONDS)).isTrue();
        FakeSource unreadable = source(List.of(), true);
        unreadable.caughtUpFailure = new IllegalStateException("marker read failed");
        unreadable.forgetNeverCompletes = true;

        StepVerifier.create(handover.catchUp(unreadable)).expectErrorMessage("marker read failed").verify(Duration.ofSeconds(10));
        releaseL1.countDown();

        l1.get(5, TimeUnit.SECONDS);
        awaitSize(log, 2);
        assertThat(log).containsExactly("L1", "N1");
        assertThat(unreadable.forgetCaughtUpCallCount()).isEqualTo(4);
        assertThat(live.forgetCaughtUpCallCount()).isEqualTo(4);
        StepVerifier.create(handover.accept("L2"))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                        .hasRootCauseMessage("marker read failed"))
                .verify(Duration.ofSeconds(5));
    }

    // B's catch-up lost the race to fail first, so it forgets the marker it may have written before it answers, and
    // a store that never answers that delete cannot keep it from answering.
    @Test
    void a_catch_up_that_is_not_the_first_failure_is_answered_when_its_forget_never_completes() {
        List<String> log = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of());
        handover.forgetAttemptTimeout(Duration.ofMillis(50));
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();
        FakeSource unreadable = source(List.of(), true);
        unreadable.caughtUpFailure = new IllegalStateException("marker read failed");
        FakeSource writing = source(List.of("R1"), false);
        writing.forgetNeverCompletes = true;
        writing.onMarkCaughtUp = () -> {
            assertThat(catchThrowable(() ->
                    CompletableFuture.supplyAsync(() -> handover.catchUp(unreadable).toFuture()).join().get(5, TimeUnit.SECONDS)))
                    .hasRootCauseMessage("marker read failed");
            throw new IllegalStateException("marker write failed");
        };

        StepVerifier.create(handover.catchUp(writing)).expectErrorMessage("marker write failed").verify(Duration.ofSeconds(10));

        assertThat(writing.forgetCaughtUpCallCount()).isEqualTo(4);
    }

    // No catch-up failed, so the refusal says what did.
    @Test
    void a_payload_refused_after_a_queued_payload_failed_names_that_failure_rather_than_a_catch_up() {
        List<String> log = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of());
        FakeSource source = source(List.of("R1"), false);
        source.onLiveDrained = () -> handover.accept("bad1").subscribe();
        StepVerifier.create(handover.catchUp(source)).expectNext(true).verifyComplete();

        await().atMost(Duration.ofSeconds(5)).until(handover::refusesPermanently);

        StepVerifier.create(handover.accept("L2"))
                .expectErrorSatisfies(error -> assertThat(error)
                        .hasMessage(HandoverMessages.queuedEventFailed("test payload"))
                        .hasRootCauseMessage("fold failed for bad1"))
                .verify(Duration.ofSeconds(5));
    }

    // The refusal goes to the call that was made, and the answer to L1 does not depend on how L1's fold handled it.
    @Test
    void accept_if_live_composed_into_a_live_fold_and_swallowed_leaves_the_answer_to_that_payload_intact() {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Throwable> refusals = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverFolding(CatchupThenLiveOptions.defaults(), (self, payload) -> switch (payload) {
            case "L1" -> self.acceptIfLive("N1").doOnError(refusals::add).onErrorComplete().then(Mono.fromRunnable(() -> log.add("L1")));
            default -> Mono.fromRunnable(() -> log.add(payload));
        });
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();

        StepVerifier.create(handover.accept("L1")).expectComplete().verify(Duration.ofSeconds(5));
        StepVerifier.create(handover.accept("L1")).expectComplete().verify(Duration.ofSeconds(5));
        StepVerifier.create(handover.accept("L2")).expectComplete().verify(Duration.ofSeconds(5));

        assertThat(refusals).singleElement(InstanceOfAssertFactories.THROWABLE)
                .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                .hasMessage(HandoverMessages.acceptedOrDeferredFromOwnDelivery("test payload"));
        assertThat(log).as("N1 is never folded and L1 is folded once").containsExactly("L1", "L2");
        assertThat(handover.refusesPermanently()).isFalse();
    }

    // The same refusal when the fold subscribes the call itself rather than composing it.
    @Test
    void accept_if_live_subscribed_by_a_live_fold_and_swallowed_leaves_the_answer_to_that_payload_intact() {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Throwable> refusals = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverFolding(CatchupThenLiveOptions.defaults(), (self, payload) -> switch (payload) {
            case "L1" -> Mono.fromRunnable(() -> self.acceptIfLive("N1").subscribe(delivered -> {}, refusals::add))
                    .then(Mono.fromRunnable(() -> log.add("L1")));
            default -> Mono.fromRunnable(() -> log.add(payload));
        });
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();

        StepVerifier.create(handover.accept("L1")).expectComplete().verify(Duration.ofSeconds(5));
        StepVerifier.create(handover.accept("L1")).expectComplete().verify(Duration.ofSeconds(5));
        StepVerifier.create(handover.accept("L2")).expectComplete().verify(Duration.ofSeconds(5));

        assertThat(refusals).singleElement(InstanceOfAssertFactories.THROWABLE)
                .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                .hasMessage(HandoverMessages.acceptedOrDeferredFromOwnDelivery("test payload"));
        assertThat(log).as("N1 is never folded and L1 is folded once").containsExactly("L1", "L2");
        assertThat(handover.refusesPermanently()).isFalse();
    }

    // L1's fold fed N1 and has ended, and a second replay holds live delivery back before N1 is folded. L1 would have
    // to wait for that replay, which acceptIfLive(..) never does.
    @Test
    void accept_if_live_whose_fold_fed_a_payload_is_answered_false_once_a_replay_holds_live_delivery_back_and_is_delivered_when_offered_again() throws Exception {
        LiveFoldWithSecondReplay scenario = new LiveFoldWithSecondReplay("N1");
        try {
            scenario.startWithTheSecondReplayHolding();

            scenario.releaseL1.countDown();

            assertThat(scenario.l1).as("L1 while R2 is held").succeedsWithin(Duration.ofSeconds(3)).isEqualTo(false);
            assertThat(scenario.foldingR2.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(scenario.secondReplay).isNotDone();
            scenario.releaseR2.countDown();
            assertThat(scenario.secondReplay).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(scenario.handover.acceptIfLive("B").block(Duration.ofSeconds(5))).as("the barrier behind N1").isTrue();
            assertThat(scenario.log).containsExactly("L1", "R2", "N1", "B");

            assertThat(scenario.handover.acceptIfLive("L1").block(Duration.ofSeconds(5))).isTrue();
            assertThat(scenario.log).as("L1 is not folded again").containsExactly("L1", "R2", "N1", "B");
        } finally {
            scenario.releaseAll();
        }
    }

    @Test
    void accept_if_live_whose_fold_fed_a_payload_errors_with_its_own_failure_rather_than_waiting_for_a_replay_that_holds_live_delivery_back() throws Exception {
        LiveFoldWithSecondReplay scenario = new LiveFoldWithSecondReplay("N1");
        scenario.l1Fails.set(true);
        try {
            scenario.startWithTheSecondReplayHolding();

            scenario.releaseL1.countDown();

            assertThat(catchThrowable(() -> scenario.l1.get(3, TimeUnit.SECONDS)))
                    .as("L1 while R2 is held")
                    .hasRootCauseInstanceOf(IllegalStateException.class)
                    .hasRootCauseMessage("fold failed for L1");
            assertThat(scenario.foldingR2.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(scenario.secondReplay).isNotDone();
            scenario.releaseR2.countDown();
            assertThat(scenario.secondReplay).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(scenario.handover.acceptIfLive("B").block(Duration.ofSeconds(5))).as("the barrier behind N1").isTrue();
            assertThat(scenario.log).containsExactly("R2", "N1", "B");

            scenario.l1Fails.set(false);
            assertThat(scenario.handover.acceptIfLive("L1").block(Duration.ofSeconds(5))).isTrue();
            assertThat(scenario.log).as("L1 failed, so it is folded when offered again").containsExactly("R2", "N1", "B", "L1");
        } finally {
            scenario.releaseAll();
        }
    }

    @Test
    void accept_if_live_whose_fold_fed_a_payload_that_fails_is_folded_again_when_offered_after_the_replay_that_held_live_delivery_back() throws Exception {
        LiveFoldWithSecondReplay scenario = new LiveFoldWithSecondReplay("bad1");
        try {
            scenario.startWithTheSecondReplayHolding();

            scenario.releaseL1.countDown();

            assertThat(scenario.l1).as("L1 while R2 is held").succeedsWithin(Duration.ofSeconds(3)).isEqualTo(false);
            scenario.releaseR2.countDown();
            assertThat(scenario.secondReplay).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(scenario.handover.acceptIfLive("B").block(Duration.ofSeconds(5))).as("the barrier behind bad1").isTrue();
            assertThat(scenario.log).containsExactly("L1", "R2", "B");

            assertThat(scenario.handover.acceptIfLive("L1").block(Duration.ofSeconds(5))).isTrue();
            assertThat(scenario.log).as("bad1 failed, so L1 is folded when offered again").containsExactly("L1", "R2", "B", "L1");
            assertThat(scenario.handover.refusesPermanently()).isFalse();
        } finally {
            scenario.releaseAll();
        }
    }

    // L1 is offered and held, with a payload fed from its fold queued behind it. The replay of R2 then asks for the
    // turn and R2 is held in its own fold. The witness is queued behind L1 and the fed payload, and is answered as not
    // live at the moment the replay holds live delivery back.
    private static final class LiveFoldWithSecondReplay {
        private final List<String> log = new CopyOnWriteArrayList<>();
        private final CountDownLatch foldingL1 = new CountDownLatch(1);
        private final CountDownLatch releaseL1 = new CountDownLatch(1);
        private final CountDownLatch foldingR2 = new CountDownLatch(1);
        private final CountDownLatch releaseR2 = new CountDownLatch(1);
        private final AtomicBoolean l1Feeds = new AtomicBoolean(true);
        private final AtomicBoolean l1Fails = new AtomicBoolean();
        private final ReactiveHandover<String, String> handover;
        private CompletableFuture<Boolean> l1;
        private CompletableFuture<Boolean> secondReplay;

        private LiveFoldWithSecondReplay(String fed) {
            handover = handoverFolding(CatchupThenLiveOptions.defaults(), (self, payload) -> switch (payload) {
                case "L1" -> Mono.fromRunnable(() -> {
                    if (l1Feeds.getAndSet(false)) {
                        self.accept(fed).subscribe(delivered -> {}, error -> {});
                    }
                    foldingL1.countDown();
                    awaitQuietly(releaseL1);
                    if (l1Fails.get()) {
                        throw new IllegalStateException("fold failed for L1");
                    }
                    log.add("L1");
                });
                case "R2" -> Mono.fromRunnable(() -> {
                    foldingR2.countDown();
                    awaitQuietly(releaseR2);
                    log.add("R2");
                });
                case "bad1" -> Mono.error(new IllegalArgumentException("fold failed for bad1"));
                default -> Mono.fromRunnable(() -> log.add(payload));
            });
        }

        private void startWithTheSecondReplayHolding() throws Exception {
            StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();
            l1 = handover.acceptIfLive("L1").subscribeOn(Schedulers.boundedElastic()).toFuture();
            assertThat(foldingL1.await(5, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<Boolean> witness = offeredFromOutside(() -> handover.acceptIfLive("W"));
            assertThat(witness).as("W queued behind L1").isNotDone();

            secondReplay = handover.catchUp(source(List.of("R2"), false)).toFuture();

            assertThat(witness).as("W once the replay holds live delivery back").succeedsWithin(Duration.ofSeconds(5)).isEqualTo(false);
        }

        private void releaseAll() {
            releaseL1.countDown();
            releaseR2.countDown();
        }
    }

    // bad1 is offered from L1's fold on this thread and subscribed on another one, and L1's fold waits until bad1 is
    // queued, so bad1 is fed from L1 before the answer to L1 can have gone out.
    @Test
    void a_payload_a_live_fold_offers_and_subscribes_on_another_thread_holds_back_the_answer_of_that_payload_and_fails_it() {
        List<String> log = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverFolding(CatchupThenLiveOptions.defaults(), (self, payload) -> switch (payload) {
            case "L1" -> Mono.fromRunnable(() -> {
                CountDownLatch bad1Queued = new CountDownLatch(1);
                self.accept("bad1").doOnSuccess(queued -> bad1Queued.countDown())
                        .subscribeOn(Schedulers.boundedElastic()).subscribe(queued -> {}, error -> {});
                awaitQuietly(bad1Queued);
            }).then(Mono.fromRunnable(() -> log.add("L1")));
            case "bad1" -> Mono.error(new IllegalArgumentException("fold failed for bad1"));
            default -> Mono.fromRunnable(() -> log.add(payload));
        });
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();

        StepVerifier.create(handover.accept("L1"))
                .expectErrorSatisfies(error -> assertThat(error).isInstanceOf(IllegalArgumentException.class).hasMessage("fold failed for bad1"))
                .verify(Duration.ofSeconds(5));

        assertThat(handover.refusesPermanently()).isFalse();
        assertThat(log).containsExactly("L1");
        StepVerifier.create(handover.accept("L1"))
                .expectErrorSatisfies(error -> assertThat(error).hasMessage("fold failed for bad1"))
                .verify(Duration.ofSeconds(5));
        assertThat(log).as("L1 was not recorded as delivered, so it is folded again").containsExactly("L1", "L1");
    }

    // The call is made on a timer thread after the fold has handed its work off, so it is offered from outside. Its
    // failure goes to whoever subscribed it, and L1 is answered on its own.
    @Test
    void a_payload_offered_from_another_thread_after_a_live_fold_handed_off_its_work_is_answered_on_its_own() {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Throwable> bad1Failures = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverFolding(CatchupThenLiveOptions.defaults(), (self, payload) -> switch (payload) {
            case "L1" -> Mono.fromRunnable(() -> Mono.delay(Duration.ofMillis(100))
                    .doOnNext(tick -> self.accept("bad1").subscribe(queued -> {}, bad1Failures::add))
                    .subscribe()).then(Mono.fromRunnable(() -> log.add("L1")));
            case "bad1" -> Mono.error(new IllegalArgumentException("fold failed for bad1"));
            default -> Mono.fromRunnable(() -> log.add(payload));
        });
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();

        StepVerifier.create(handover.accept("L1")).expectComplete().verify(Duration.ofSeconds(5));

        await().atMost(Duration.ofSeconds(5)).until(() -> !bad1Failures.isEmpty());
        assertThat(bad1Failures).singleElement(InstanceOfAssertFactories.THROWABLE)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("fold failed for bad1");
        assertThat(handover.refusesPermanently()).isFalse();
        StepVerifier.create(handover.accept("L2")).expectComplete().verify(Duration.ofSeconds(5));
        assertThat(log).containsExactly("L1", "L2");
    }

    // N1 is fed by the replayed R1 and its fold is held, so the marker has to wait for it.
    @Test
    void a_payload_a_replayed_fold_feeds_is_folded_before_the_marker_is_written() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch foldingN1 = new CountDownLatch(1);
        CountDownLatch releaseN1 = new CountDownLatch(1);
        ReactiveHandover<String, String> handover = handoverFolding(CatchupThenLiveOptions.defaults(), (self, payload) -> switch (payload) {
            case "R1" -> self.accept("N1").then(Mono.fromRunnable(() -> log.add("R1")));
            case "N1" -> Mono.fromRunnable(() -> {
                foldingN1.countDown();
                awaitQuietly(releaseN1);
                log.add("N1");
            });
            default -> Mono.fromRunnable(() -> log.add(payload));
        });
        FakeSource source = source(List.of("R1"), false);
        source.onMarkCaughtUp = () -> log.add("marker");
        try {
            CompletableFuture<Boolean> catchUp = handover.catchUp(source).toFuture();
            assertThat(foldingN1.await(5, TimeUnit.SECONDS)).isTrue();

            assertThat(source.markCaughtUpCallCount()).as("the marker while N1 is unfolded").isZero();
            assertThat(catchUp).isNotDone();
            releaseN1.countDown();

            assertThat(catchUp).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(log).containsExactly("R1", "N1", "marker");
        } finally {
            releaseN1.countDown();
        }
    }

    // The call is made on the fold's thread and subscribed on another one, and replayCompleted() waits until it is
    // queued, so it is made while the replay still folds what its folds fed.
    @Test
    void a_payload_a_replayed_fold_offers_and_subscribes_on_another_thread_is_folded_before_the_marker_is_written() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch n1Queued = new CountDownLatch(1);
        CountDownLatch foldingN1 = new CountDownLatch(1);
        CountDownLatch releaseN1 = new CountDownLatch(1);
        ReactiveHandover<String, String> handover = handoverFolding(CatchupThenLiveOptions.defaults(), (self, payload) -> switch (payload) {
            case "R1" -> Mono.fromRunnable(() -> self.accept("N1").doOnSuccess(queued -> n1Queued.countDown())
                    .subscribeOn(Schedulers.boundedElastic()).subscribe(queued -> {}, error -> {})).then(Mono.fromRunnable(() -> log.add("R1")));
            case "N1" -> Mono.fromRunnable(() -> {
                foldingN1.countDown();
                awaitQuietly(releaseN1);
                log.add("N1");
            });
            default -> Mono.fromRunnable(() -> log.add(payload));
        });
        FakeSource source = source(List.of("R1"), false);
        source.onReplayCompleted = () -> awaitQuietly(n1Queued);
        source.onMarkCaughtUp = () -> log.add("marker");
        try {
            CompletableFuture<Boolean> catchUp = handover.catchUp(source).toFuture();
            assertThat(foldingN1.await(5, TimeUnit.SECONDS)).isTrue();

            assertThat(source.markCaughtUpCallCount()).as("the marker while N1 is unfolded").isZero();
            releaseN1.countDown();

            assertThat(catchUp).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(log).containsExactly("R1", "N1", "marker");
        } finally {
            releaseN1.countDown();
        }
    }

    // bad1 fails, and Y behind it and Z that Y's fold feeds are folded anyway. The catch-up fails with the failure of
    // bad1, so the next catch-up replays the history that feeds them again.
    @Test
    void a_payload_a_replayed_fold_feeds_that_fails_fails_the_catch_up_after_the_rest_are_folded_and_writes_no_marker() {
        List<String> log = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of(
                "R1", self -> self.accept("bad1").then(self.accept("Y")),
                "Y", self -> self.accept("Z")));
        FakeSource source = source(List.of("R1"), false);

        StepVerifier.create(handover.catchUp(source))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("fold failed for bad1"))
                .verify(Duration.ofSeconds(5));

        assertThat(log).containsExactly("R1", "Y", "Z");
        assertThat(source.markCaughtUpCallCount()).isZero();
        assertThat(source.forgetCaughtUpCallCount()).isEqualTo(1);
        assertThat(handover.refusesPermanently()).isTrue();
        StepVerifier.create(handover.accept("L2"))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                        .hasMessage(HandoverMessages.catchUpFailed("test payload"))
                        .hasRootCauseMessage("fold failed for bad1"))
                .verify(Duration.ofSeconds(5));
    }

    // Each payload in the chain is fed by the fold of the one before it, and the chain ends at N100 so that a handover
    // without the cap fails the assertion instead of running out of memory.
    @Test
    void a_chain_of_payloads_a_replayed_fold_feeds_longer_than_the_live_buffer_fails_the_catch_up_with_the_cap() {
        ReactiveHandover<String, String> handover = handoverFolding(new CatchupThenLiveOptions(10, 3), (self, payload) -> {
            int next = payload.equals("R1") ? 1 : Integer.parseInt(payload.substring(1)) + 1;
            return next > 100 ? Mono.empty() : self.accept("N" + next);
        });
        FakeSource source = source(List.of("R1"), false);

        StepVerifier.create(handover.catchUp(source))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                        .hasMessage(HandoverMessages.fedChainTooLong("test payload", 3)))
                .verify(Duration.ofSeconds(5));

        assertThat(source.markCaughtUpCallCount()).isZero();
    }

    // The callback runs as part of the replay, so what it feeds is folded before the marker and a crash after the
    // marker cannot lose it.
    @Test
    void a_payload_replay_started_feeds_is_folded_before_the_marker_is_written() throws Exception {
        assertThatAPayloadACallbackFeedsIsFoldedBeforeTheMarker((source, handover) ->
                source.onReplayStarted = () -> handover.accept("N1").subscribe());
    }

    @Test
    void a_payload_replay_completed_feeds_is_folded_before_the_marker_is_written() throws Exception {
        assertThatAPayloadACallbackFeedsIsFoldedBeforeTheMarker((source, handover) ->
                source.onReplayCompleted = () -> handover.accept("N1").subscribe());
    }

    @Test
    void a_payload_history_done_feeds_is_folded_before_the_marker_is_written() throws Exception {
        assertThatAPayloadACallbackFeedsIsFoldedBeforeTheMarker((source, handover) ->
                source.onHistoryDone = () -> handover.accept("N1").subscribe());
    }

    // The Mono replayCompleted() returns waits for the call, which is answered once N1 is queued and not once it is
    // folded, so the replay does not wait for a fold that only runs after replayCompleted() has ended.
    @Test
    void a_replay_completed_that_waits_for_the_payload_it_feeds_does_not_deadlock_and_the_payload_is_folded_before_the_marker() {
        List<String> log = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of());
        FakeSource source = source(List.of("R1"), false);
        source.replayCompletedWaitsFor = () -> handover.accept("N1");
        source.onMarkCaughtUp = () -> log.add("marker");

        StepVerifier.create(handover.catchUp(source)).expectNext(true).expectComplete().verify(Duration.ofSeconds(5));

        assertThat(log).containsExactly("R1", "N1", "marker");
    }

    // N1 is fed by historyDone() and each fold feeds the next payload. The chain ends at N100 so that a handover
    // without the cap fails the assertion instead of running out of memory.
    @Test
    void a_chain_of_payloads_history_done_feeds_longer_than_the_live_buffer_fails_the_catch_up_with_the_cap() {
        ReactiveHandover<String, String> handover = handoverFolding(new CatchupThenLiveOptions(10, 3), (self, payload) -> {
            if (payload.equals("R1")) {
                return Mono.empty();
            }
            int next = Integer.parseInt(payload.substring(1)) + 1;
            return next > 100 ? Mono.empty() : self.accept("N" + next);
        });
        FakeSource source = source(List.of("R1"), false);
        source.onHistoryDone = () -> handover.accept("N1").subscribe();

        StepVerifier.create(handover.catchUp(source))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                        .hasMessage(HandoverMessages.fedChainTooLong("test payload", 3)))
                .verify(Duration.ofSeconds(5));

        assertThat(source.markCaughtUpCallCount()).isZero();
    }

    // The fold swallows the refusal of N2, so the catch-up is the only one left to report that N2 was never taken in.
    // Without the failure the marker would be written and no later replay would feed N2 again. The live counterpart
    // is a_fed_payload_refused_for_a_full_buffer_fails_the_answer_of_the_payload_that_fed_it.
    @Test
    void a_payload_a_replayed_fold_feeds_that_is_refused_for_a_full_buffer_fails_the_catch_up_although_the_fold_swallows_it() {
        List<String> log = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverFolding(new CatchupThenLiveOptions(10, 1), (self, payload) -> switch (payload) {
            case "R1" -> Mono.fromRunnable(() -> {
                self.accept("N1").subscribe(queued -> {}, error -> {});
                self.accept("N2").subscribe(queued -> {}, error -> {});
            }).then(Mono.fromRunnable(() -> log.add("R1")));
            default -> Mono.fromRunnable(() -> log.add(payload));
        });
        FakeSource source = source(List.of("R1"), false);

        StepVerifier.create(handover.catchUp(source))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                        .hasMessage(HandoverMessages.bufferOverflow(1)))
                .verify(Duration.ofSeconds(5));

        assertThat(log).containsExactly("R1", "N1");
        assertThat(source.markCaughtUpCallCount()).isZero();
        assertThat(source.forgetCaughtUpCallCount()).isEqualTo(1);
    }

    // N1 is offered on the fold's thread and subscribed on another one, and the live buffer is full. Its key function
    // is the only user code between the check that the replay still folds and taking N1 in, and it holds N1 there until
    // R1 has returned. Had the marker been written by then, N1 would be refused to nobody. It waits with a timeout
    // because the marker is never written when the catch-up waits for N1, so it gives up and lets the offer go on.
    @Test
    void a_payload_a_replayed_fold_subscribes_elsewhere_that_is_refused_for_a_full_buffer_after_the_replay_folded_what_it_fed_fails_the_catch_up() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch n1InKey = new CountDownLatch(1);
        CountDownLatch markerWritten = new CountDownLatch(1);
        CountDownLatch releaseL1 = new CountDownLatch(1);
        AtomicReference<ReactiveHandover<String, String>> self = new AtomicReference<>();
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> {
            switch (payload) {
                case "R1" -> {
                    self.get().accept("N1").subscribeOn(Schedulers.boundedElastic()).subscribe(queued -> {}, error -> {});
                    awaitQuietly(n1InKey);
                    log.add("R1");
                }
                case "L1" -> {
                    awaitQuietly(releaseL1);
                    log.add("L1");
                }
                default -> log.add(payload);
            }
        }), payload -> {
            if (payload.equals("N1")) {
                n1InKey.countDown();
                countsDownWithin(markerWritten, Duration.ofSeconds(1));
            }
            return payload;
        }, new CatchupThenLiveOptions(10, 1), "test payload");
        self.set(handover);
        FakeSource source = source(List.of("R1"), false);
        source.onMarkCaughtUp = markerWritten::countDown;
        try {
            handover.accept("L1").subscribe(queued -> {}, error -> {});

            StepVerifier.create(handover.catchUp(source))
                    .expectErrorSatisfies(error -> assertThat(error)
                            .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                            .hasMessage(HandoverMessages.bufferOverflow(1)))
                    .verify(Duration.ofSeconds(5));

            assertThat(source.markCaughtUpCallCount()).as("the marker for a replay that fed a refused payload").isZero();
            assertThat(log).as("what was folded").containsExactly("R1");
        } finally {
            releaseL1.countDown();
        }
    }

    // Each fold subscribes the next payload and swallows what it gets back, so the refusal at the end of the chain
    // reaches nobody but the catch-up.
    @Test
    void a_chain_of_payloads_a_replayed_fold_feeds_longer_than_the_live_buffer_fails_the_catch_up_although_the_folds_swallow_the_refusal() {
        ReactiveHandover<String, String> handover = handoverFolding(new CatchupThenLiveOptions(10, 3), (self, payload) -> {
            int next = payload.equals("R1") ? 1 : Integer.parseInt(payload.substring(1)) + 1;
            return next > 100 ? Mono.empty() : Mono.fromRunnable(() -> self.accept("N" + next).subscribe(queued -> {}, error -> {}));
        });
        FakeSource source = source(List.of("R1"), false);

        StepVerifier.create(handover.catchUp(source))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                        .hasMessage(HandoverMessages.fedChainTooLong("test payload", 3)))
                .verify(Duration.ofSeconds(5));

        assertThat(source.markCaughtUpCallCount()).isZero();
    }

    // The key of K cannot be computed, so K is refused before it is taken in, and the fold of R1 swallows that refusal.
    @Test
    void a_payload_a_replayed_fold_feeds_whose_key_cannot_be_computed_fails_the_catch_up_although_the_fold_swallows_it() {
        List<String> log = new CopyOnWriteArrayList<>();
        AtomicReference<ReactiveHandover<String, String>> self = new AtomicReference<>();
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.defer(() -> switch (payload) {
            case "R1" -> Mono.fromRunnable(() -> {
                self.get().accept("K").subscribe(queued -> {}, error -> log.add("K refused: " + error.getMessage()));
                log.add("R1");
            });
            default -> Mono.fromRunnable(() -> log.add(payload));
        }), payload -> {
            if (payload.equals("K")) {
                throw new IllegalStateException("no key for K");
            }
            return payload;
        }, CatchupThenLiveOptions.defaults(), "test payload");
        self.set(handover);
        FakeSource source = source(List.of("R1"), false);

        StepVerifier.create(handover.catchUp(source))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessage("no key for K"))
                .verify(Duration.ofSeconds(5));

        assertThat(log).containsExactly("K refused: no key for K", "R1");
        assertThat(source.markCaughtUpCallCount()).isZero();
        assertThat(source.forgetCaughtUpCallCount()).isEqualTo(1);
    }

    // A key function can throw what is not a RuntimeException, an AssertionError or a checked exception that Kotlin or
    // Lombok throws without declaring it. K is refused all the same, and the fold of R1 swallows that refusal.
    @Test
    void a_payload_a_replayed_fold_feeds_and_subscribes_on_its_own_thread_whose_key_function_throws_an_error_fails_the_catch_up_although_the_fold_swallows_it() {
        AssertionError thrown = new AssertionError("no key for K");

        KeyFailureOutcome outcome = catchUpOfAFoldFeedingAPayloadWhoseKeyFunctionThrows(thrown, false);

        assertThatTheCatchUpFailedWith(outcome, thrown);
    }

    @Test
    void a_payload_a_replayed_fold_feeds_and_subscribes_elsewhere_whose_key_function_throws_an_error_fails_the_catch_up_although_the_fold_swallows_it() {
        AssertionError thrown = new AssertionError("no key for K");

        KeyFailureOutcome outcome = catchUpOfAFoldFeedingAPayloadWhoseKeyFunctionThrows(thrown, true);

        assertThatTheCatchUpFailedWith(outcome, thrown);
    }

    @Test
    void a_payload_a_replayed_fold_feeds_and_subscribes_on_its_own_thread_whose_key_function_throws_a_checked_exception_fails_the_catch_up_although_the_fold_swallows_it() {
        IOException thrown = new IOException("no key for K");

        KeyFailureOutcome outcome = catchUpOfAFoldFeedingAPayloadWhoseKeyFunctionThrows(thrown, false);

        assertThatTheCatchUpFailedWith(outcome, thrown);
    }

    @Test
    void a_payload_a_replayed_fold_feeds_and_subscribes_elsewhere_whose_key_function_throws_a_checked_exception_fails_the_catch_up_although_the_fold_swallows_it() {
        IOException thrown = new IOException("no key for K");

        KeyFailureOutcome outcome = catchUpOfAFoldFeedingAPayloadWhoseKeyFunctionThrows(thrown, true);

        assertThatTheCatchUpFailedWith(outcome, thrown);
    }

    // The key function of K returns only after the catch-up has logged that it waits for it, and then a while longer,
    // so a warning repeated on every look of the catch-up would show up as more than one. K is still folded and the
    // marker written when the key function returns.
    @Test
    void a_catch_up_that_waits_more_than_five_seconds_for_the_key_function_of_a_payload_its_replay_fed_logs_one_warning_and_still_writes_its_marker() {
        List<String> log = new CopyOnWriteArrayList<>();
        AtomicReference<ReactiveHandover<String, String>> self = new AtomicReference<>();
        CountDownLatch keyFunctionEntered = new CountDownLatch(1);
        try (CapturedLog captured = new CapturedLog()) {
            ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> {
                if (payload.equals("R1")) {
                    self.get().accept("K").subscribeOn(Schedulers.boundedElastic()).subscribe(queued -> {}, error -> log.add("K refused: " + error));
                    awaitQuietly(keyFunctionEntered);
                }
                log.add(payload);
            }), payload -> {
                if (payload.equals("K")) {
                    keyFunctionEntered.countDown();
                    await().atMost(Duration.ofSeconds(15)).until(() -> !captured.warnings().isEmpty());
                    countsDownWithin(new CountDownLatch(1), Duration.ofMillis(500));
                }
                return payload;
            }, CatchupThenLiveOptions.defaults(), "test payload");
            self.set(handover);
            FakeSource source = source(List.of("R1"), false);

            StepVerifier.create(handover.catchUp(source)).expectNext(true).expectComplete().verify(Duration.ofSeconds(20));

            await().atMost(Duration.ofSeconds(5)).until(() -> log.contains("K"));
            assertThat(log).as("what was folded").containsExactly("R1", "K");
            assertThat(source.markCaughtUpCallCount()).as("the marker for a replay whose fed payload was folded").isEqualTo(1);
            assertThat(captured.warnings())
                    .singleElement()
                    .satisfies(warning -> assertThat(warning.getFormattedMessage())
                            .startsWith("A catch-up of this test payload has waited more than 5 seconds for the de-dup key function"));
        }
    }

    @Test
    void a_catch_up_that_waits_a_short_while_for_the_key_function_of_a_payload_its_replay_fed_logs_no_warning() {
        List<String> log = new CopyOnWriteArrayList<>();
        AtomicReference<ReactiveHandover<String, String>> self = new AtomicReference<>();
        CountDownLatch keyFunctionEntered = new CountDownLatch(1);
        try (CapturedLog captured = new CapturedLog()) {
            ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> {
                if (payload.equals("R1")) {
                    self.get().accept("K").subscribeOn(Schedulers.boundedElastic()).subscribe(queued -> {}, error -> log.add("K refused: " + error));
                    awaitQuietly(keyFunctionEntered);
                }
                log.add(payload);
            }), payload -> {
                if (payload.equals("K")) {
                    keyFunctionEntered.countDown();
                    countsDownWithin(new CountDownLatch(1), Duration.ofMillis(300));
                }
                return payload;
            }, CatchupThenLiveOptions.defaults(), "test payload");
            self.set(handover);
            FakeSource source = source(List.of("R1"), false);

            StepVerifier.create(handover.catchUp(source)).expectNext(true).expectComplete().verify(Duration.ofSeconds(5));

            await().atMost(Duration.ofSeconds(5)).until(() -> log.contains("K"));
            assertThat(captured.warnings()).isEmpty();
            assertThat(source.markCaughtUpCallCount()).isEqualTo(1);
        }
    }

    // The handover has failed for good, and a catch-up asked for after that still replays. The fold of R1 feeds N1,
    // which the failed handover refuses, and swallows that refusal, so the catch-up is the only one left to report it.
    @Test
    void a_payload_a_replayed_fold_feeds_that_a_handover_which_failed_for_good_refuses_fails_the_catch_up_although_the_fold_swallows_it() {
        List<String> log = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverFolding(CatchupThenLiveOptions.defaults(), (self, payload) -> switch (payload) {
            case "bad" -> Mono.error(new IllegalArgumentException("fold failed for bad"));
            case "R1" -> Mono.fromRunnable(() -> {
                self.accept("N1").subscribe(queued -> {}, error -> log.add("N1 refused"));
                log.add("R1");
            });
            default -> Mono.fromRunnable(() -> log.add(payload));
        });
        StepVerifier.create(handover.catchUp(source(List.of("bad"), false)))
                .expectErrorSatisfies(error -> assertThat(error).hasMessage("fold failed for bad"))
                .verify(Duration.ofSeconds(5));
        assertThat(handover.refusesPermanently()).isTrue();
        FakeSource source = source(List.of("R1"), false);

        StepVerifier.create(handover.catchUp(source))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                        .hasMessage(HandoverMessages.catchUpFailed("test payload"))
                        .hasRootCauseMessage("fold failed for bad"))
                .verify(Duration.ofSeconds(5));

        assertThat(log).containsExactly("N1 refused", "R1");
        assertThat(source.markCaughtUpCallCount()).isZero();
    }

    @Test
    void a_payload_replay_started_feeds_that_fails_fails_the_catch_up_after_the_replay_is_folded_and_writes_no_marker() {
        assertThatACatchUpFailsForAPayloadACallbackFeeds((source, handover) ->
                source.onReplayStarted = () -> handover.accept("bad").subscribe(queued -> {}, error -> {}));
    }

    @Test
    void a_payload_history_done_feeds_that_fails_fails_the_catch_up_after_the_replay_is_folded_and_writes_no_marker() {
        assertThatACatchUpFailsForAPayloadACallbackFeeds((source, handover) ->
                source.onHistoryDone = () -> handover.accept("bad").subscribe(queued -> {}, error -> {}));
    }

    @Test
    void a_chain_of_payloads_a_live_fold_feeds_longer_than_the_live_buffer_fails_the_answer_to_the_payload_that_started_it_although_the_folds_swallow_the_refusal() {
        ReactiveHandover<String, String> handover = handoverFolding(new CatchupThenLiveOptions(10, 3), (self, payload) -> {
            int next = payload.equals("L1") ? 1 : Integer.parseInt(payload.substring(1)) + 1;
            return next > 100 ? Mono.empty() : Mono.fromRunnable(() -> self.accept("N" + next).subscribe(queued -> {}, error -> {}));
        });
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();

        StepVerifier.create(handover.accept("L1"))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                        .hasMessage(HandoverMessages.fedChainTooLong("test payload", 3)))
                .verify(Duration.ofSeconds(5));
    }

    // A live L1 whose fold feeds bad1 and then Y: Y is folded although bad1 failed, and the answer to L1 comes after it.
    @Test
    void a_payload_fed_after_one_that_failed_is_still_folded_before_the_answer_to_the_payload_that_fed_both() {
        List<String> log = new CopyOnWriteArrayList<>();
        AtomicReference<List<String>> loggedWhenAnswered = new AtomicReference<>();
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of(
                "L1", self -> self.accept("bad1").then(self.accept("Y"))));
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();

        StepVerifier.create(handover.accept("L1").doOnError(error -> loggedWhenAnswered.set(List.copyOf(log))))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("fold failed for bad1"))
                .verify(Duration.ofSeconds(5));

        assertThat(loggedWhenAnswered.get()).containsExactly("L1", "Y");
        assertThat(handover.refusesPermanently()).isFalse();
    }

    // The callback feeds a payload whose fold fails and swallows what it gets back, so the catch-up is the only one left
    // to report the failure.
    private static void assertThatACatchUpFailsForAPayloadACallbackFeeds(BiConsumer<FakeSource, ReactiveHandover<String, String>> feedsBad) {
        List<String> log = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = handoverWhoseFoldFeedsItself(log, Map.of());
        FakeSource source = source(List.of("R1"), false);
        feedsBad.accept(source, handover);

        StepVerifier.create(handover.catchUp(source))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("fold failed for bad"))
                .verify(Duration.ofSeconds(5));

        assertThat(log).containsExactly("R1");
        assertThat(source.markCaughtUpCallCount()).isZero();
        assertThat(source.forgetCaughtUpCallCount()).isEqualTo(1);
    }

    // N1 is fed from a Source callback of a catch-up that replays R1, and its fold is held. The marker must wait for it.
    private static void assertThatAPayloadACallbackFeedsIsFoldedBeforeTheMarker(
            BiConsumer<FakeSource, ReactiveHandover<String, String>> feedsN1) throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch foldingN1 = new CountDownLatch(1);
        CountDownLatch releaseN1 = new CountDownLatch(1);
        ReactiveHandover<String, String> handover = handoverFolding(CatchupThenLiveOptions.defaults(), (self, payload) -> switch (payload) {
            case "N1" -> Mono.fromRunnable(() -> {
                foldingN1.countDown();
                awaitQuietly(releaseN1);
                log.add("N1");
            });
            default -> Mono.fromRunnable(() -> log.add(payload));
        });
        FakeSource source = source(List.of("R1"), false);
        feedsN1.accept(source, handover);
        source.onMarkCaughtUp = () -> log.add("marker");
        try {
            CompletableFuture<Boolean> catchUp = handover.catchUp(source).toFuture();
            assertThat(foldingN1.await(5, TimeUnit.SECONDS)).isTrue();

            assertThat(source.markCaughtUpCallCount()).as("the marker while N1 is unfolded").isZero();
            assertThat(catchUp).isNotDone();
            releaseN1.countDown();

            assertThat(catchUp).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(log).containsExactly("R1", "N1", "marker");
        } finally {
            releaseN1.countDown();
        }
    }

    // Offers a payload from outside once the caller has waited for the handover to deliver what it took in and fail.
    private static void assertThatALaterPayloadIsRefusedFor(ReactiveHandover<String, String> handover, String failedPayload) {
        StepVerifier.create(handover.accept("L2"))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                        .hasRootCauseMessage("fold failed for " + failedPayload))
                .verify(Duration.ofSeconds(5));
    }

    private record KeyFailureOutcome(Signal<Boolean> catchUp, List<Throwable> refusals, List<String> log, FakeSource source) {
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> void throwWithoutDeclaring(Throwable throwable) throws E {
        throw (E) throwable;
    }

    // The fold of R1 feeds K, whose key function throws what it is given, and swallows the refusal of K.
    private static KeyFailureOutcome catchUpOfAFoldFeedingAPayloadWhoseKeyFunctionThrows(Throwable thrown, boolean subscribedElsewhere) {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Throwable> refusals = new CopyOnWriteArrayList<>();
        AtomicReference<ReactiveHandover<String, String>> self = new AtomicReference<>();
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.defer(() -> switch (payload) {
            case "R1" -> Mono.fromRunnable(() -> {
                CountDownLatch answered = new CountDownLatch(1);
                Mono<Void> fed = self.get().accept("K");
                if (subscribedElsewhere) {
                    fed = fed.subscribeOn(Schedulers.boundedElastic());
                }
                fed.subscribe(queued -> {}, error -> {
                    refusals.add(error);
                    answered.countDown();
                }, answered::countDown);
                awaitQuietly(answered);
                log.add("R1");
            });
            default -> Mono.fromRunnable(() -> log.add(payload));
        }), payload -> {
            if (payload.equals("K")) {
                ReactiveHandoverTest.<RuntimeException>throwWithoutDeclaring(thrown);
            }
            return payload;
        }, CatchupThenLiveOptions.defaults(), "test payload");
        self.set(handover);
        FakeSource source = source(List.of("R1"), false);

        Signal<Boolean> catchUp = handover.catchUp(source).materialize().block(Duration.ofSeconds(10));

        return new KeyFailureOutcome(catchUp, refusals, log, source);
    }

    private static void assertThatTheCatchUpFailedWith(KeyFailureOutcome outcome, Throwable thrown) {
        assertThat(outcome.catchUp().getThrowable()).as("what the catch-up ended with").isSameAs(thrown);
        assertThat(outcome.refusals()).as("what the fold was told about K").containsExactly(thrown);
        assertThat(outcome.log()).as("what was folded").containsExactly("R1");
        assertThat(outcome.source().markCaughtUpCallCount()).as("the marker for a replay that fed a refused payload").isZero();
    }

    // Thread safe, unlike a ListAppender, since the handover logs from the thread of a timer
    private static final class CapturedLog implements AutoCloseable {
        private final Logger logger = (Logger) LoggerFactory.getLogger(ReactiveHandover.class);
        private final List<ILoggingEvent> events = new CopyOnWriteArrayList<>();
        private final AppenderBase<ILoggingEvent> appender = new AppenderBase<>() {
            @Override
            protected void append(ILoggingEvent event) {
                events.add(event);
            }
        };

        CapturedLog() {
            appender.start();
            logger.addAppender(appender);
        }

        List<ILoggingEvent> warnings() {
            return events.stream().filter(event -> event.getLevel() == Level.WARN).toList();
        }

        @Override
        public void close() {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    private static boolean countsDownWithin(CountDownLatch latch, Duration timeout) {
        try {
            return latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    // Each payload runs what it feeds back before it is logged, so a payload fed back is logged after the one that
    // fed it only when it is delivered after that one. A payload starting with "bad" fails its fold.
    private static ReactiveHandover<String, String> handoverWhoseFoldFeedsItself(
            List<String> log, Map<String, Function<ReactiveHandover<String, String>, Mono<?>>> feedsBack) {
        AtomicReference<ReactiveHandover<String, String>> self = new AtomicReference<>();
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> {
            if (payload.startsWith("bad")) {
                return Mono.error(new IllegalArgumentException("fold failed for " + payload));
            }
            Mono<Void> folded = Mono.fromRunnable(() -> log.add(payload));
            Function<ReactiveHandover<String, String>, Mono<?>> feedBack = feedsBack.get(payload);
            return feedBack == null ? folded : Mono.defer(() -> feedBack.apply(self.get())).then(folded);
        }, payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        self.set(handover);
        return handover;
    }

    // For folds that depend on the payload or on state the test holds, which a fixed map of what each payload feeds back
    // cannot express.
    private static ReactiveHandover<String, String> handoverFolding(
            CatchupThenLiveOptions options, BiFunction<ReactiveHandover<String, String>, String, Mono<Void>> fold) {
        AtomicReference<ReactiveHandover<String, String>> self = new AtomicReference<>();
        ReactiveHandover<String, String> handover = ReactiveHandover.create(
                payload -> Mono.defer(() -> fold.apply(self.get(), payload)), payload -> payload, options, "test payload");
        self.set(handover);
        return handover;
    }

    private static void awaitSize(List<String> log, int size) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (log.size() < size && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
    }

    // Offered from a thread this handover is not running, so the payload is a live one arriving while the fold runs
    // rather than one the fold feeds back. The payload is taken in before this returns.
    private static <R> CompletableFuture<R> offeredFromOutside(Supplier<Mono<R>> offer) {
        return CompletableFuture.supplyAsync(() -> offer.get().toFuture()).join();
    }

    // Holds L1's fold until R2 has put its hold on live delivery in place and waits for that fold, then lets the fold
    // go on, and checks that L1 and R2 both finish.
    private static void assertThatALiveFoldGoesLiveWhileAReplayStarts(ReactiveHandover<String, String> handover, CountDownLatch foldingL1,
                                                                      CountDownLatch releaseL1) throws Exception {
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();
        // Subscribed on another thread, since the live fold runs on the thread that offers the payload.
        CompletableFuture<Boolean> l1 = handover.acceptIfLive("L1").subscribeOn(Schedulers.boundedElastic()).toFuture();
        assertThat(foldingL1.await(5, TimeUnit.SECONDS)).isTrue();
        CountDownLatch r2Holding = new CountDownLatch(1);
        FakeSource r2 = source(List.of("R2"), false);
        // Runs once the marker read reached the replay, which then takes the free turn and puts its hold in place on
        // the same thread.
        r2.onCaughtUpChecked = r2Holding::countDown;
        CompletableFuture<Boolean> secondReplay = handover.catchUp(r2).toFuture();
        assertThat(r2Holding.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(r2.replayCallCount).as("R2 waits for L1's fold").isZero();

        releaseL1.countDown();

        assertThat(l1.get(10, TimeUnit.SECONDS)).isTrue();
        assertThat(secondReplay.get(10, TimeUnit.SECONDS)).isTrue();
    }

    // replayStarted() runs once the replay holds live delivery back, so a catch-up with nothing to replay it blocks on
    // would wait for that hold while the replay waits for replayStarted() to return.
    @Test
    void a_catch_up_with_nothing_to_replay_replay_started_blocks_on_answers_without_waiting() {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Boolean> answers = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> log.add(payload)),
                payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        FakeSource replaying = source(List.of("R1"), false);
        replaying.onReplayStarted = () -> answers.add(handover.catchUp(source(List.of(), true)).block(Duration.ofSeconds(5)));

        StepVerifier.create(handover.catchUp(replaying)).expectNext(true).expectComplete().verify(Duration.ofSeconds(10));

        assertThat(answers).containsExactly(true);
        assertThat(handover.acceptIfLive("L1").block(Duration.ofSeconds(5))).isTrue();
        assertThat(log).containsExactly("R1", "L1");
    }

    // replayCompleted() runs before the replay gives back its hold on live delivery, so a catch-up with nothing to
    // replay it blocks on would wait for a replay that is waiting for replayCompleted() to complete.
    @Test
    void a_catch_up_with_nothing_to_replay_replay_completed_blocks_on_answers_without_waiting() {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Object> answers = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> log.add(payload)),
                payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        FakeSource replaying = source(List.of("R1"), false);
        replaying.onReplayCompleted = () -> answers.add(answerOrFailure(() -> handover.catchUp(source(List.of(), true)).block(Duration.ofSeconds(5))));

        StepVerifier.create(handover.catchUp(replaying)).expectNext(true).expectComplete().verify(Duration.ofSeconds(10));

        assertThat(answers).containsExactly(true);
        assertThat(handover.acceptIfLive("L1").block(Duration.ofSeconds(5))).isTrue();
        assertThat(log).containsExactly("R1", "L1");
    }

    // A replay that starts waits for alreadyDeliveredByReplay(..) the way it waits for a live fold, so a catch-up with
    // nothing to replay that call blocks on would wait for a replay that is waiting for it.
    @Test
    void a_catch_up_with_nothing_to_replay_already_delivered_by_replay_blocks_on_while_a_replay_starts_answers_without_waiting() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Object> answers = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> log.add(payload)),
                payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        FakeSource replayed = source(List.of("R1"), false);
        StepVerifier.create(handover.catchUp(replayed)).expectNext(true).expectComplete().verify(Duration.ofSeconds(10));
        CountDownLatch reportingR1 = new CountDownLatch(1);
        CountDownLatch releaseReport = new CountDownLatch(1);
        replayed.onAlreadyDeliveredByReplay = () -> {
            reportingR1.countDown();
            awaitLatchQuietly(releaseReport);
            answers.add(answerOrFailure(() -> handover.catchUp(source(List.of(), true)).block(Duration.ofSeconds(5))));
        };
        try {
            CompletableFuture<Boolean> reporting = handover.acceptIfLive("R1").subscribeOn(Schedulers.boundedElastic()).toFuture();
            assertThat(reportingR1.await(5, TimeUnit.SECONDS)).isTrue();
            CountDownLatch r2Holding = new CountDownLatch(1);
            FakeSource r2 = source(List.of("R2"), false);
            r2.onCaughtUpChecked = r2Holding::countDown;
            CompletableFuture<Boolean> secondReplay = handover.catchUp(r2).toFuture();
            assertThat(r2Holding.await(5, TimeUnit.SECONDS)).isTrue();

            releaseReport.countDown();

            assertThat(reporting).succeedsWithin(Duration.ofSeconds(10)).isEqualTo(true);
            assertThat(secondReplay).succeedsWithin(Duration.ofSeconds(10)).isEqualTo(true);
            assertThat(answers).containsExactly(true);
            assertThat(log).containsExactly("R1", "R2");
        } finally {
            releaseReport.countDown();
        }
    }

    private static Object answerOrFailure(Supplier<Object> call) {
        try {
            return call.get();
        } catch (RuntimeException e) {
            return e;
        }
    }

    // replayAbandoned() runs before a stopped replay gives back its hold on live delivery, and this engine swallows
    // what it throws, so the answer it got is the only trace of a call that waited.
    @Test
    void a_catch_up_with_nothing_to_replay_replay_abandoned_blocks_on_answers_without_waiting() {
        List<String> log = new CopyOnWriteArrayList<>();
        List<Object> answers = new CopyOnWriteArrayList<>();
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> log.add(payload)),
                payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        FakeSource replaying = source(List.of("R1"), false);
        replaying.stopAfter(0);
        replaying.onReplayAbandoned = () -> {
            try {
                answers.add(handover.catchUp(source(List.of(), true)).block(Duration.ofSeconds(5)));
            } catch (RuntimeException e) {
                answers.add(e);
            }
        };

        StepVerifier.create(handover.catchUp(replaying)).expectNext(false).expectComplete().verify(Duration.ofSeconds(10));

        assertThat(answers).containsExactly(true);
        assertThat(log).isEmpty();
    }

    // The first catch-up already failed, so the failure recorded before the call is not the one the call waited for.
    // The catch-up with nothing to replay still errors with the failure of the replay it waited for.
    @Test
    void a_catch_up_with_nothing_to_replay_fails_when_the_replay_it_waited_for_fails_on_a_handover_that_had_already_failed() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch replayReached = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        IllegalStateException firstFailure = new IllegalStateException("first boom");
        IllegalStateException secondFailure = new IllegalStateException("second boom");
        Function<String, Mono<Void>> heldAtR1 = holdingAt("R1", log, replayReached, releaseReplay, secondFailure);
        ReactiveHandover<String, String> handover = ReactiveHandover.create(
                payload -> payload.equals("R0") ? Mono.error(firstFailure) : heldAtR1.apply(payload),
                payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        try {
            StepVerifier.create(handover.catchUp(source(List.of("R0"), false))).verifyErrorMessage("first boom");
            Mono<Boolean> replaying = handover.catchUp(source(List.of("R1"), false));
            assertThat(replayReached.await(5, TimeUnit.SECONDS)).isTrue();

            CompletableFuture<Boolean> nothingToReplay = waitingBehindTheRunningReplay(handover);

            assertThat(nothingToReplay).as("the catch-up with nothing to replay while R1 is held").isNotDone();
            releaseReplay.countDown();
            StepVerifier.create(replaying).verifyErrorMessage("second boom");
            assertThatThrownBy(() -> nothingToReplay.get(5, TimeUnit.SECONDS)).cause()
                    .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                    .hasCauseReference(secondFailure);
        } finally {
            releaseReplay.countDown();
        }
    }

    // The first waiter is refused inside the failed replay's release of live delivery, before the second one reads
    // which failure it waited for. Its refusal is not a failure of the replay, so the second one still gets the
    // replay's own failure, wrapped once.
    @Test
    void two_catch_ups_with_nothing_to_replay_waiting_for_a_replay_that_fails_each_fail_with_that_replays_failure() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch replayReached = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        IllegalStateException failure = new IllegalStateException("boom");
        ReactiveHandover<String, String> handover = ReactiveHandover.create(holdingAt("R1", log, replayReached, releaseReplay, failure),
                payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        try {
            Mono<Boolean> replaying = handover.catchUp(source(List.of("R1"), false));
            assertThat(replayReached.await(5, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<Boolean> first = waitingBehindTheRunningReplay(handover);
            CompletableFuture<Boolean> second = waitingBehindTheRunningReplay(handover);

            releaseReplay.countDown();

            StepVerifier.create(replaying).verifyErrorMessage("boom");
            for (CompletableFuture<Boolean> waiter : List.of(first, second)) {
                assertThat(catchThrowable(() -> waiter.get(5, TimeUnit.SECONDS))).cause()
                        .isInstanceOf(ReactiveHandover.PreDispatchRefusalException.class)
                        .hasCauseReference(failure);
            }
        } finally {
            releaseReplay.countDown();
        }
    }

    // Starts a catch-up with nothing to replay and returns once it waits for the replay holding live delivery back.
    // Its source is told the marker was read only after the wait was subscribed, on the same thread.
    private static CompletableFuture<Boolean> waitingBehindTheRunningReplay(ReactiveHandover<String, String> handover) throws InterruptedException {
        CountDownLatch waiting = new CountDownLatch(1);
        FakeSource nothingToReplay = source(List.of(), true);
        nothingToReplay.onCaughtUpChecked = waiting::countDown;
        CompletableFuture<Boolean> result = handover.catchUp(nothingToReplay).toFuture();
        assertThat(waiting.await(5, TimeUnit.SECONDS)).isTrue();
        return result;
    }

    // Records every payload it folds, and holds the fold of the given payload until released, failing it afterwards
    // when given a failure.
    private static Function<String, Mono<Void>> holdingAt(String held, List<String> log, CountDownLatch reached, CountDownLatch release,
                                                          RuntimeException failure) {
        return payload -> Mono.fromRunnable(() -> {
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
        });
    }

    // The payloads a replay holds back stay held until the whole catch-up has succeeded. A marker write that fails
    // after the replay finished fails their acknowledgements, so they must not have been delivered and acknowledged in
    // the meantime.
    @Test
    void live_payloads_held_back_are_not_delivered_before_the_catch_up_marker_is_written() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        List<CompletableFuture<Boolean>> liveAcks = new CopyOnWriteArrayList<>();
        AtomicReference<ReactiveHandover<String, String>> self = new AtomicReference<>();
        AtomicBoolean offered = new AtomicBoolean();
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> {
            log.add(payload);
            if (payload.equals("R1") && offered.compareAndSet(false, true)) {
                liveAcks.add(offeredFromOutside(() -> self.get().acceptReportingDelivery("L1")));
            }
        }), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        self.set(handover);
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();
        FakeSource failingMarker = source(List.of("R1"), false);
        failingMarker.onMarkCaughtUp = () -> {
            throw new IllegalStateException("marker boom");
        };

        StepVerifier.create(handover.catchUp(failingMarker)).verifyErrorMessage("marker boom");
        Throwable ackFailure = catchThrowable(() -> liveAcks.get(0).get(5, TimeUnit.SECONDS));
        Mono.delay(Duration.ofMillis(300)).block();

        assertThat(ackFailure).hasCauseInstanceOf(ReactiveHandover.PreDispatchRefusalException.class);
        assertThat(log).containsExactly("R1");
    }

    // acceptIfLive refuses while a replay runs, even on a handover that is already live, the same as the blocking
    // engine, so a caller that can redeliver is told to try again rather than having its payload held until the replay
    // ends.
    @Test
    void acceptIfLive_refuses_while_a_replay_runs_on_a_live_handover() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        List<CompletableFuture<Boolean>> answers = new CopyOnWriteArrayList<>();
        AtomicReference<ReactiveHandover<String, String>> self = new AtomicReference<>();
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.defer(() -> {
            log.add(payload);
            if (payload.equals("R1")) {
                answers.add(offeredFromOutside(() -> self.get().acceptIfLive("L1")));
            }
            return Mono.empty();
        }), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        self.set(handover);
        StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();

        StepVerifier.create(handover.catchUp(source(List.of("R1"), false))).expectNext(true).verifyComplete();

        assertThat(answers.get(0).get(5, TimeUnit.SECONDS)).isFalse();
        Mono.delay(Duration.ofMillis(300)).block();
        assertThat(log).containsExactly("R1");
    }

    // The dedup key is read after the payload is registered and before it is queued, so the pause lands with L1
    // registered and not yet queued. The replay still holds the pause when L1 is answered.
    @Test
    void acceptIfLive_registered_but_not_yet_queued_when_a_replay_pauses_live_delivery_is_answered_false_and_never_delivered() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch readingKeyOfL1 = new CountDownLatch(1);
        CountDownLatch releaseKeyOfL1 = new CountDownLatch(1);
        CountDownLatch foldingR1 = new CountDownLatch(1);
        CountDownLatch releaseR1 = new CountDownLatch(1);
        ReactiveHandover<String, String> handover = ReactiveHandover.create(
                holdingAt("R1", log, foldingR1, releaseR1, null), payload -> {
                    if (payload.equals("L1")) {
                        readingKeyOfL1.countDown();
                        awaitLatchQuietly(releaseKeyOfL1);
                    }
                    return payload;
                }, CatchupThenLiveOptions.defaults(), "test payload");
        try {
            StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();
            CompletableFuture<Boolean> l1 = handover.acceptIfLive("L1").subscribeOn(Schedulers.boundedElastic()).toFuture();
            assertThat(readingKeyOfL1.await(5, TimeUnit.SECONDS)).isTrue();

            CompletableFuture<Boolean> replaying = handover.catchUp(source(List.of("R1"), false)).toFuture();
            assertThat(foldingR1.await(5, TimeUnit.SECONDS)).isTrue();

            assertThat(l1).as("L1 while the replay folds R1").succeedsWithin(Duration.ofSeconds(5)).isEqualTo(false);
            releaseKeyOfL1.countDown();
            releaseR1.countDown();
            assertThat(replaying).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(handover.acceptIfLive("L2").block(Duration.ofSeconds(5))).as("the barrier behind L1").isTrue();
            assertThat(log).containsExactly("R1", "L2");
        } finally {
            releaseKeyOfL1.countDown();
            releaseR1.countDown();
        }
    }

    // L2 is queued behind L1's fold when the replay pauses live delivery. L1 is being folded, so the replay waits for it
    // and L1 answers true. L2 has not started, so it is answered false at once and taken out of the queue.
    @Test
    void acceptIfLive_queued_behind_a_live_fold_is_answered_false_when_a_replay_pauses_live_delivery_and_never_delivered() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        CountDownLatch foldingL1 = new CountDownLatch(1);
        CountDownLatch releaseL1 = new CountDownLatch(1);
        ReactiveHandover<String, String> handover = ReactiveHandover.create(
                holdingAt("L1", log, foldingL1, releaseL1, null), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        try {
            StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();
            CompletableFuture<Boolean> l1 = handover.acceptIfLive("L1").subscribeOn(Schedulers.boundedElastic()).toFuture();
            assertThat(foldingL1.await(5, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<Boolean> l2 = offeredFromOutside(() -> handover.acceptIfLive("L2"));
            assertThat(l2).as("L2 queued behind L1").isNotDone();

            CompletableFuture<Boolean> replaying = handover.catchUp(source(List.of("R1"), false)).toFuture();

            assertThat(l2).as("L2 before L1 is released").succeedsWithin(Duration.ofSeconds(5)).isEqualTo(false);
            assertThat(l1).as("L1 while its fold is held").isNotDone();
            assertThat(replaying).as("the replay while L1 is folded").isNotDone();
            releaseL1.countDown();
            assertThat(l1).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(replaying).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(handover.acceptIfLive("L3").block(Duration.ofSeconds(5))).as("the barrier behind L2").isTrue();
            assertThat(log).containsExactly("L1", "R1", "L3");
        } finally {
            releaseL1.countDown();
        }
    }

    // L1 feeds nothing and its fold completes on the test thread, which ends its delivery there, while another thread
    // starts a catch-up whose replay pauses live delivery. L1's delivery had started before that, so whichever of the two
    // happens first, the replay waits for it and L1 is answered true. A false would mean the pause saw L1's delivery as
    // ended while L1 was still counted as running. Nothing runs between the two steps that end the delivery, so the
    // threads start at offsets that differ per handover, over many handovers, so that some pause falls between them.
    // Checked against the bad order of the two steps, which failed it in 10 of 10 runs, at handover 123 to 386, and
    // in 5 of 5 on 2 CPUs. Keep the count at 10,000, since fewer handovers can pass the bad order.
    @Test
    void acceptIfLive_whose_fold_completes_on_one_thread_while_a_replay_pauses_live_delivery_on_another_is_answered_true() throws Exception {
        int handovers = 10_000;
        int offsetSpreadNanos = 20000;
        ExecutorService pausing = Executors.newSingleThreadExecutor();
        CyclicBarrier bothReady = new CyclicBarrier(2);
        try {
            for (int i = 0; i < handovers; i++) {
                Random offsets = new Random(i);
                AtomicReference<MonoSink<Void>> foldOfL1 = new AtomicReference<>();
                CountDownLatch foldingL1 = new CountDownLatch(1);
                ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.<Void>create(sink -> {
                    foldOfL1.set(sink);
                    foldingL1.countDown();
                }), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
                StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();
                CompletableFuture<Boolean> l1 = handover.acceptIfLive("L1").subscribeOn(Schedulers.boundedElastic()).toFuture();
                assertThat(foldingL1.await(5, TimeUnit.SECONDS)).isTrue();
                long pauseOffset = offsets.nextInt(offsetSpreadNanos);
                long foldEndOffset = offsets.nextInt(offsetSpreadNanos);

                Future<CompletableFuture<Boolean>> replaying = pausing.submit(() -> {
                    bothReady.await(5, TimeUnit.SECONDS);
                    spinFor(pauseOffset);
                    return handover.catchUp(source(List.of(), false)).toFuture();
                });
                bothReady.await(5, TimeUnit.SECONDS);
                spinFor(foldEndOffset);
                foldOfL1.get().success();

                assertThat(l1).as("L1 of handover %d", i).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
                assertThat(replaying.get(5, TimeUnit.SECONDS)).as("the replay of handover %d", i).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            }
        } finally {
            pausing.shutdownNow();
        }
    }

    private static void spinFor(long nanos) {
        long until = System.nanoTime() + nanos;
        while (System.nanoTime() < until) {
            Thread.onSpinWait();
        }
    }

    // L1 is delivered inline on the thread that subscribed to it, and that thread ends the live delivery the replay
    // waits for. The replay must not continue there, or block() cannot return until the replay is done.
    @Test
    void a_replay_that_waited_for_an_inline_live_delivery_runs_on_another_thread_than_the_caller_of_acceptIfLive() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        AtomicReference<String> replayThread = new AtomicReference<>();
        CountDownLatch foldingL1 = new CountDownLatch(1);
        CountDownLatch releaseL1 = new CountDownLatch(1);
        CountDownLatch foldingR1 = new CountDownLatch(1);
        CountDownLatch releaseR1 = new CountDownLatch(1);
        ReactiveHandover<String, String> handover = ReactiveHandover.create(payload -> Mono.fromRunnable(() -> {
            if (payload.equals("L1")) {
                foldingL1.countDown();
                awaitLatchQuietly(releaseL1);
            }
            if (payload.equals("R1")) {
                replayThread.set(Thread.currentThread().getName());
                foldingR1.countDown();
                awaitLatchQuietly(releaseR1);
            }
            log.add(payload);
        }), payload -> payload, CatchupThenLiveOptions.defaults(), "test payload");
        CompletableFuture<Boolean> blocked = new CompletableFuture<>();
        Thread caller = new Thread(() -> {
            try {
                blocked.complete(handover.acceptIfLive("L1").block(Duration.ofSeconds(10)));
            } catch (Throwable t) {
                blocked.completeExceptionally(t);
            }
        }, "accept-if-live-caller");
        try {
            StepVerifier.create(handover.catchUp(source(List.of(), true))).expectNext(true).verifyComplete();
            caller.start();
            assertThat(foldingL1.await(5, TimeUnit.SECONDS)).isTrue();
            CountDownLatch replayWaiting = new CountDownLatch(1);
            FakeSource history = source(List.of("R1"), false);
            history.onCaughtUpChecked = replayWaiting::countDown;
            CompletableFuture<Boolean> replaying = handover.catchUp(history).toFuture();
            assertThat(replayWaiting.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(history.replayCallCount).as("the replay waits for L1's fold").isZero();

            releaseL1.countDown();

            assertThat(foldingR1.await(5, TimeUnit.SECONDS)).as("R1 is folded").isTrue();
            assertThat(replayThread.get()).isNotEqualTo("accept-if-live-caller");
            assertThat(blocked).as("block() while R1 is held").succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            releaseR1.countDown();
            assertThat(replaying).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            assertThat(log).containsExactly("L1", "R1");
        } finally {
            releaseL1.countDown();
            releaseR1.countDown();
            caller.join(TimeUnit.SECONDS.toMillis(15));
        }
    }

    @Test
    void replay_lifecycle_is_started_then_completed_before_the_marker() throws Exception {
        List<String> log = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String, String> handover = ReactiveHandover.create(
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
        ReactiveHandover<String, String> handover = handover(delivered);
        FakeSource source = source(List.of("R1"), true);

        handover.catchUp(source).block(Duration.ofSeconds(5));

        assertThat(source.replayStartedCallCount).isZero();
        assertThat(source.replayCompletedCallCount).isZero();
        assertThat(source.replayAbandonedCallCount).isZero();
    }

    @Test
    void a_stopped_replay_calls_replay_abandoned_instead_of_replay_completed() {
        List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        ReactiveHandover<String, String> handover = handover(delivered);
        FakeSource source = source(List.of("R1", "R2", "R3"), false);
        source.stopAfter(2);

        StepVerifier.create(handover.catchUp(source)).expectNext(false).verifyComplete();

        assertThat(source.replayStartedCallCount).isEqualTo(1);
        assertThat(source.replayCompletedCallCount).isZero();
        assertThat(source.replayAbandonedCallCount).isEqualTo(1);
    }

    @Test
    void a_failed_replay_calls_replay_abandoned_before_the_failure_propagates() {
        ReactiveHandover<String, String> handover = handover(new ArrayList<>());
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
        ReactiveHandover<String, String> handover = handover(new ArrayList<>());
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
        private RuntimeException caughtUpFailure;
        private RuntimeException forgetFailure;
        private boolean forgetNeverCompletes;
        private Runnable onMarkCaughtUp;
        private Runnable onReplayStarted;
        private Runnable onReplayCompleted;
        private Supplier<Mono<?>> replayCompletedWaitsFor;
        private Runnable onReplayAbandoned;
        private Runnable onAlreadyDeliveredByReplay;
        private Runnable onHistoryDone;
        private Runnable onLiveDrained;
        private Runnable onCaughtUpChecked;
        private int replayCallCount = 0;
        private int markCaughtUpCallCount = 0;
        private int stopAfter = Integer.MAX_VALUE;
        private int keepReplayingCallCount = 0;
        private int replayStartedCallCount = 0;
        private int replayCompletedCallCount = 0;
        private int replayAbandonedCallCount = 0;
        private final List<String> alreadyDeliveredByReplay = Collections.synchronizedList(new ArrayList<>());
        private final java.util.concurrent.atomic.AtomicInteger forgetCaughtUpCallCount = new java.util.concurrent.atomic.AtomicInteger();

        @Override
        public Mono<Void> forgetCaughtUp() {
            return Mono.defer(() -> {
                forgetCaughtUpCallCount.incrementAndGet();
                if (forgetNeverCompletes) {
                    return Mono.never();
                }
                return forgetFailure == null ? Mono.empty() : Mono.error(forgetFailure);
            });
        }

        private int forgetCaughtUpCallCount() {
            return forgetCaughtUpCallCount.get();
        }

        @Override
        public Mono<Void> alreadyDeliveredByReplay(String payload) {
            return Mono.fromRunnable(() -> {
                alreadyDeliveredByReplay.add(payload);
                if (onAlreadyDeliveredByReplay != null) {
                    onAlreadyDeliveredByReplay.run();
                }
            });
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
            Mono<Boolean> caughtUp = caughtUpFailure == null ? Mono.just(alreadyCaughtUp) : Mono.error(caughtUpFailure);
            return onCaughtUpChecked == null ? caughtUp : caughtUp.doAfterTerminate(onCaughtUpChecked);
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
            if (replayCompletedWaitsFor != null) {
                return Mono.defer(() -> replayCompletedWaitsFor.get()).then();
            }
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
