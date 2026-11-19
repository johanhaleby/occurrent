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

package org.occurrent.subscription.synchronous.reactor;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.service.WriteRefusedException;
import org.occurrent.application.service.reactor.ReactiveSynchronousEventDispatcher.WriteCheck;
import org.occurrent.application.service.reactor.dcb.GenericDcbApplicationService;
import org.occurrent.application.service.reactor.generic.GenericApplicationService;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.WriteResult;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.EventStream;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.StreamSubscriptionFilter;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.tags;

/**
 * The window a write holds open on the model, which a pause or a stop waits for. The services must close it before the
 * result reaches the subscriber, since the subscriber may pause or stop right then, and also after a failed, refused or
 * cancelled write. The model's own handling of a pause or a stop that waits for a window is driven directly.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SynchronousSubscriptionModelWriteWindowTest {

    private static final Tag TAG = Tag.parse("name:1");
    private static final long WAIT_SECONDS = 10;

    private ExecutorService plainThreads;
    private ProgrammableEventStore eventStore;
    private ProgrammableDcbEventStore dcbEventStore;
    private SynchronousSubscriptionModel model;
    private List<String> handled;
    private GenericApplicationService<TestEvent> applicationService;
    private GenericDcbApplicationService<TestEvent> dcbApplicationService;

    @BeforeEach
    void initialize() {
        plainThreads = Executors.newCachedThreadPool(action -> {
            Thread thread = new Thread(action, "plain-store-thread");
            thread.setDaemon(true);
            return thread;
        });
        eventStore = new ProgrammableEventStore();
        dcbEventStore = new ProgrammableDcbEventStore();
        model = new SynchronousSubscriptionModel();
        handled = new CopyOnWriteArrayList<>();
        applicationService = GenericApplicationService.builder(eventStore, new TestEventConverter())
                .synchronousSubscriptions(model)
                .build();
        dcbApplicationService = GenericDcbApplicationService.builder(dcbEventStore, new TestEventConverter())
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(model)
                .build();
    }

    @AfterEach
    void shutDownPlainThreads() {
        plainThreads.shutdownNow();
    }

    // The pause after the write

    @Test
    void a_pause_chained_after_a_generic_write_that_completes_on_a_plain_thread_returns_and_applies() throws Exception {
        eventStore.completeWritesOnPlainThreads(() -> subscribeHandler("p", Filter.type("NameDefined")));
        pauseChainedAfterWriteThatCompletesOnAPlainThread(() -> write("NameDefined"));
    }

    @Test
    void a_pause_chained_after_a_dcb_write_that_completes_on_a_plain_thread_returns_and_applies() throws Exception {
        dcbEventStore.completeWritesOnPlainThreads(() -> subscribeHandler("p", Filter.type("NameDefined")));
        pauseChainedAfterWriteThatCompletesOnAPlainThread(() -> dcbWrite("NameDefined"));
    }

    @Test
    void a_pause_made_from_the_result_of_a_generic_write_that_completes_on_a_plain_thread_returns_and_applies() throws Exception {
        eventStore.completeWritesOnPlainThreads(() -> subscribeHandler("p", Filter.type("NameDefined")));
        pauseMadeFromTheResultOfAWriteThatCompletesOnAPlainThread(() -> write("NameDefined"));
    }

    @Test
    void a_pause_made_from_the_result_of_a_dcb_write_that_completes_on_a_plain_thread_returns_and_applies() throws Exception {
        dcbEventStore.completeWritesOnPlainThreads(() -> subscribeHandler("p", Filter.type("NameDefined")));
        pauseMadeFromTheResultOfAWriteThatCompletesOnAPlainThread(() -> dcbWrite("NameDefined"));
    }

    @Test
    void a_stop_chained_after_a_generic_write_that_completes_on_a_plain_thread_returns_and_a_later_write_is_refused_until_the_model_starts() throws Exception {
        eventStore.completeWritesOnPlainThreads(() -> {
        });
        stopChainedAfterWriteThatCompletesOnAPlainThread(() -> write("NameDefined"));
    }

    @Test
    void a_stop_chained_after_a_dcb_write_that_completes_on_a_plain_thread_returns_and_a_later_write_is_refused_until_the_model_starts() throws Exception {
        dcbEventStore.completeWritesOnPlainThreads(() -> {
        });
        stopChainedAfterWriteThatCompletesOnAPlainThread(() -> dcbWrite("NameDefined"));
    }

    // The handler is registered while the write is in flight, so the write has nothing to dispatch and no routing is under
    // way when the result is emitted. With a dispatch under way the pause returns at once, which hides when the window closes.
    private void pauseChainedAfterWriteThatCompletesOnAPlainThread(Supplier<Mono<?>> write) throws Exception {
        Chained chained = new Chained();

        write.get().then(Mono.fromRunnable(chained.run(() -> model.pauseSubscription("p")))).subscribe(ignored -> {
        }, chained.failure::set);
        writesMayComplete.countDown();

        chained.awaitReturned("the pause chained after the write");
        chained.assertRanOffTheSubscriber();
        assertThat(chained.failure).as("[error of the write]").hasValue(null);
        assertThat(model.isPaused("p")).as("paused once the pause returned").isTrue();
    }

    private void pauseMadeFromTheResultOfAWriteThatCompletesOnAPlainThread(Supplier<Mono<?>> write) throws Exception {
        Chained chained = new Chained();

        write.get().flatMap(result -> Mono.fromRunnable(chained.run(() -> model.pauseSubscription("p")))).subscribe(ignored -> {
        }, chained.failure::set);
        writesMayComplete.countDown();

        chained.awaitReturned("the pause made from the result");
        chained.assertRanOffTheSubscriber();
        assertThat(chained.failure).as("[error of the write]").hasValue(null);
        assertThat(model.isPaused("p")).as("paused once the pause returned").isTrue();
    }

    private void stopChainedAfterWriteThatCompletesOnAPlainThread(Supplier<Mono<?>> write) throws Exception {
        Chained chained = new Chained();
        write.get().then(Mono.fromRunnable(chained.run(model::stop))).subscribe(ignored -> {
        }, chained.failure::set);
        writesMayComplete.countDown();
        chained.awaitReturned("the stop chained after the write");
        chained.assertRanOffTheSubscriber();
        assertThat(chained.failure).as("[error of the write]").hasValue(null);
        assertThat(model.isRunning()).as("running once the stop returned").isFalse();
        subscribeHandler("p", Filter.type("NameDefined"));

        StepVerifier.create(write.get())
                .expectErrorSatisfies(error -> assertThat(error).isInstanceOf(WriteRefusedException.class).hasMessageContaining("stopped"))
                .verify(Duration.ofSeconds(WAIT_SECONDS));
        model.start();
        StepVerifier.create(write.get()).expectNextCount(1).expectComplete().verify(Duration.ofSeconds(WAIT_SECONDS));

        assertThat(handled).as("events the handler got, the refused write not among them").containsExactly("NameDefined");
    }

    // The window after a write that fails, is refused or is cancelled

    @Test
    void a_pause_and_a_stop_from_another_thread_apply_after_a_generic_write_whose_store_failed() throws Exception {
        subscribeHandler("p", Filter.type("NameDefined"));
        eventStore.failWritesWith(new IllegalStateException("store failed"));

        StepVerifier.create(write("NameDefined")).expectErrorMessage("store failed").verify(Duration.ofSeconds(WAIT_SECONDS));

        assertThatAPauseAndAStopApply("a write whose store failed");
    }

    @Test
    void a_pause_and_a_stop_from_another_thread_apply_after_a_dcb_write_whose_store_failed() throws Exception {
        subscribeHandler("p", Filter.type("NameDefined"));
        dcbEventStore.failWritesWith(new IllegalStateException("store failed"));

        StepVerifier.create(dcbWrite("NameDefined")).expectErrorMessage("store failed").verify(Duration.ofSeconds(WAIT_SECONDS));

        assertThatAPauseAndAStopApply("a write whose store failed");
    }

    @Test
    void a_pause_and_a_stop_from_another_thread_apply_after_a_generic_write_whose_handler_failed() throws Exception {
        model.subscribe("p", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), event -> Mono.error(new IllegalStateException("handler failed")));

        StepVerifier.create(write("NameDefined")).expectErrorSatisfies(error -> assertThat(error).hasMessageContaining("handler failed")).verify(Duration.ofSeconds(WAIT_SECONDS));

        assertThatAPauseAndAStopApply("a write whose handler failed");
    }

    @Test
    void a_pause_and_a_stop_from_another_thread_apply_after_a_dcb_write_whose_handler_failed() throws Exception {
        model.subscribe("p", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), event -> Mono.error(new IllegalStateException("handler failed")));

        StepVerifier.create(dcbWrite("NameDefined")).expectErrorSatisfies(error -> assertThat(error).hasMessageContaining("handler failed")).verify(Duration.ofSeconds(WAIT_SECONDS));

        assertThatAPauseAndAStopApply("a write whose handler failed");
    }

    @Test
    void a_pause_and_a_stop_from_another_thread_apply_after_a_generic_write_that_a_paused_subscription_refused() throws Exception {
        subscribeHandler("p", Filter.type("NameWasChanged"));
        subscribeHandler("paused", Filter.type("NameDefined"));
        model.pauseSubscription("paused");

        StepVerifier.create(write("NameDefined"))
                .expectErrorSatisfies(error -> assertThat(error).isInstanceOf(WriteRefusedException.class).hasMessageContaining("paused"))
                .verify(Duration.ofSeconds(WAIT_SECONDS));

        assertThatAPauseAndAStopApply("a write that a paused subscription refused");
    }

    @Test
    void a_pause_and_a_stop_from_another_thread_apply_after_a_dcb_write_that_a_paused_subscription_refused() throws Exception {
        subscribeHandler("p", Filter.type("NameWasChanged"));
        subscribeHandler("paused", Filter.type("NameDefined"));
        model.pauseSubscription("paused");

        StepVerifier.create(dcbWrite("NameDefined"))
                .expectErrorSatisfies(error -> assertThat(error).isInstanceOf(WriteRefusedException.class).hasMessageContaining("paused"))
                .verify(Duration.ofSeconds(WAIT_SECONDS));

        assertThatAPauseAndAStopApply("a write that a paused subscription refused");
    }

    @Test
    void a_pause_and_a_stop_from_another_thread_apply_after_a_generic_write_that_was_cancelled() throws Exception {
        subscribeHandler("p", Filter.type("NameDefined"));
        eventStore.neverCompleteWrites();

        Disposable subscription = write("NameDefined").subscribe();
        assertThat(eventStore.writeReached.await(WAIT_SECONDS, TimeUnit.SECONDS)).as("the write reached the store, so its window is open").isTrue();
        subscription.dispose();

        assertThatAPauseAndAStopApply("a write that was cancelled");
    }

    @Test
    void a_pause_and_a_stop_from_another_thread_apply_after_a_dcb_write_that_was_cancelled() throws Exception {
        subscribeHandler("p", Filter.type("NameDefined"));
        dcbEventStore.neverCompleteWrites();

        Disposable subscription = dcbWrite("NameDefined").subscribe();
        assertThat(dcbEventStore.writeReached.await(WAIT_SECONDS, TimeUnit.SECONDS)).as("the write reached the store, so its window is open").isTrue();
        subscription.dispose();

        assertThatAPauseAndAStopApply("a write that was cancelled");
    }

    private void assertThatAPauseAndAStopApply(String after) throws InterruptedException {
        Thread pauser = start(() -> model.pauseSubscription("p"));
        assertThat(joinedOrInterrupted(pauser)).as("the pause from another thread returned after " + after).isTrue();
        assertThat(model.isPaused("p")).as("paused once the pause returned after " + after).isTrue();
        Thread stopper = start(model::stop);
        assertThat(joinedOrInterrupted(stopper)).as("the stop from another thread returned after " + after).isTrue();
        assertThat(model.isRunning()).as("running once the stop returned after " + after).isFalse();
    }

    // The pause and stop branches that wait for a window, driven by windows the test opens and closes itself. The
    // model's beforeWrite opens a window on the calling thread, so a pause or a stop made on that thread doesn't wait.

    @Test
    void a_pause_applies_once_the_windows_opened_before_it_close_even_when_a_window_opened_after_it_is_still_open() {
        model.subscribe("p", event -> Mono.empty());
        WriteCheck openedBefore = model.beforeWrite();
        model.pauseSubscription("p");
        WriteCheck openedAfter = model.beforeWrite();
        assertThat(model.isPaused("p")).as("paused while a window opened before the pause is open").isFalse();

        openedBefore.close();

        assertThat(model.isPaused("p")).as("paused once the window opened before the pause closed, with a later one still open").isTrue();
        openedAfter.close();
    }

    @Test
    void a_resume_while_a_pause_waits_calls_the_pause_off() {
        model.subscribe("p", event -> Mono.empty());
        WriteCheck open = model.beforeWrite();
        model.pauseSubscription("p");

        Throwable thrown = catchThrowable(() -> model.resumeSubscription("p"));
        open.close();

        assertThat(thrown).as("what resuming the subscription threw").isNull();
        assertThat(model.isPaused("p")).as("paused after the window closed").isFalse();
        assertThat(model.isRunning("p")).as("running after the window closed").isTrue();
    }

    @Test
    void a_resume_while_a_stop_waits_calls_the_stop_off_and_pauses_the_other_subscriptions() {
        model.subscribe("p", event -> Mono.empty());
        model.subscribe("other", event -> Mono.empty());
        WriteCheck open = model.beforeWrite();
        model.stop();

        Throwable thrown = catchThrowable(() -> model.resumeSubscription("p"));
        open.close();

        assertThat(thrown).as("what resuming the subscription threw").isNull();
        assertThat(model.isRunning("p")).as("the resumed subscription runs after the window closed").isTrue();
        assertThat(model.isPaused("other")).as("the other subscription is paused after the window closed").isTrue();
        assertThat(model.isRunning()).as("the model runs after the window closed").isTrue();
    }

    @Test
    void a_start_without_resuming_while_a_stop_waits_turns_the_stop_into_pauses() {
        model.subscribe("p", event -> Mono.empty());
        model.subscribe("other", event -> Mono.empty());
        WriteCheck open = model.beforeWrite();
        model.stop();

        model.start(false);
        open.close();

        assertThat(model.isRunning()).as("the model runs after the window closed").isTrue();
        assertThat(model.isPaused("p")).as("p is paused after the window closed").isTrue();
        assertThat(model.isPaused("other")).as("other is paused after the window closed").isTrue();
    }

    private void subscribeHandler(String subscriptionId, Filter filter) {
        model.subscribe(subscriptionId, StreamSubscriptionFilter.filter(filter), event -> Mono.fromRunnable(() -> handled.add(event.getType())));
    }

    private Mono<WriteResult> write(String type) {
        return applicationService.execute(UUID.randomUUID().toString(), events -> List.of(new TestEvent(type)));
    }

    private Mono<DcbAppendResult> dcbWrite(String type) {
        return dcbApplicationService.execute(tags(TAG), events -> List.of(new TestEvent(type)));
    }

    // Runs a pause or a stop chained after a write, and tells when it returned and on which thread it ran, so a test
    // whose pause hangs can interrupt it and end
    private static final class Chained {
        final CountDownLatch returned = new CountDownLatch(1);
        final AtomicReference<Thread> thread = new AtomicReference<>();
        final Thread subscriber = Thread.currentThread();
        final AtomicReference<Throwable> failure = new AtomicReference<>();

        Runnable run(Runnable action) {
            return () -> {
                thread.set(Thread.currentThread());
                action.run();
                returned.countDown();
            };
        }

        void awaitReturned(String what) throws InterruptedException {
            boolean done = returned.await(WAIT_SECONDS, TimeUnit.SECONDS);
            if (!done && thread.get() != null) {
                thread.get().interrupt();
            }
            assertThat(done).as(what + " returned").isTrue();
        }

        void assertRanOffTheSubscriber() {
            assertThat(thread.get()).as("[thread the chained action ran on, not the one that subscribed and owns the window]").isNotSameAs(subscriber);
        }
    }

    private static Thread start(Runnable action) {
        Thread thread = new Thread(action);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private static boolean joinedOrInterrupted(Thread thread) throws InterruptedException {
        thread.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
        if (thread.isAlive()) {
            thread.interrupt();
            thread.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
            return false;
        }
        return true;
    }

    // Held until the test has subscribed, so the future completes after Mono.fromFuture installed its handler and the result
    // is emitted on the pool thread and never inline on the thread that subscribed and owns the window
    private final CountDownLatch writesMayComplete = new CountDownLatch(1);

    private void awaitSubscribed() {
        try {
            assertThat(writesMayComplete.await(WAIT_SECONDS, TimeUnit.SECONDS)).as("the test subscribed").isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    // Completes a write on a thread of the test's own executor when asked to, which is not a Reactor thread and is not
    // the thread that subscribed
    private final class ProgrammableEventStore implements EventStore {
        private final ConcurrentHashMap<String, List<CloudEvent>> streams = new ConcurrentHashMap<>();
        private volatile Function<Mono<WriteResult>, Mono<WriteResult>> onWrite = Function.identity();
        final CountDownLatch writeReached = new CountDownLatch(1);

        void completeWritesOnPlainThreads(Runnable beforeCompleting) {
            onWrite = write -> Mono.fromFuture(CompletableFuture.supplyAsync(() -> {
                awaitSubscribed();
                beforeCompleting.run();
                return write.block();
            }, plainThreads));
        }

        void failWritesWith(RuntimeException failure) {
            onWrite = write -> Mono.error(failure);
        }

        void neverCompleteWrites() {
            onWrite = write -> Mono.never();
        }

        @Override
        public Mono<EventStream<CloudEvent>> read(String streamId, int skip, int limit) {
            return Mono.fromSupplier(() -> {
                List<CloudEvent> all = streams.getOrDefault(streamId, List.of());
                List<CloudEvent> slice = all.stream().skip(skip).limit(limit).toList();
                return new EventStream<>() {
                    @Override
                    public String id() {
                        return streamId;
                    }

                    @Override
                    public long version() {
                        return all.size();
                    }

                    @Override
                    public Flux<CloudEvent> events() {
                        return Flux.fromIterable(slice);
                    }
                };
            });
        }

        @Override
        public Mono<WriteResult> write(String streamId, WriteCondition writeCondition, Flux<CloudEvent> events) {
            return write(streamId, events);
        }

        @Override
        public Mono<WriteResult> write(String streamId, Flux<CloudEvent> events) {
            return Mono.defer(() -> {
                writeReached.countDown();
                return onWrite.apply(events.collectList().map(newEvents -> {
                    List<CloudEvent> stream = streams.computeIfAbsent(streamId, id -> new ArrayList<>());
                    long oldVersion = stream.size();
                    stream.addAll(newEvents);
                    return new WriteResult(streamId, oldVersion, stream.size());
                }));
            });
        }

        @Override
        public Mono<Boolean> exists(String streamId) {
            return Mono.fromSupplier(() -> streams.containsKey(streamId));
        }
    }

    private final class ProgrammableDcbEventStore implements DcbEventStore {
        private final List<CloudEvent> events = new CopyOnWriteArrayList<>();
        private volatile Function<Mono<DcbAppendResult>, Mono<DcbAppendResult>> onAppend = Function.identity();
        final CountDownLatch writeReached = new CountDownLatch(1);

        void completeWritesOnPlainThreads(Runnable beforeCompleting) {
            onAppend = append -> Mono.fromFuture(CompletableFuture.supplyAsync(() -> {
                awaitSubscribed();
                beforeCompleting.run();
                return append.block();
            }, plainThreads));
        }

        void failWritesWith(RuntimeException failure) {
            onAppend = append -> Mono.error(failure);
        }

        void neverCompleteWrites() {
            onAppend = append -> Mono.never();
        }

        @Override
        public Mono<DcbEventStream> read(DcbCriteria criteria, DcbReadOptions options) {
            return Mono.fromSupplier(() -> {
                List<CloudEvent> all = List.copyOf(events);
                long after = options.afterPosition().orElse(0);
                long upTo = options.upToPosition().orElse(all.size());
                List<CloudEvent> slice = all.subList((int) Math.min(after, all.size()), (int) Math.min(upTo, all.size()));
                return new DcbEventStream(slice, all.size(), DcbConsistencyToken.of(criteria, all.size()));
            });
        }

        @Override
        public Mono<DcbAppendResult> append(List<CloudEvent> newEvents) {
            return Mono.defer(() -> {
                writeReached.countDown();
                return onAppend.apply(Mono.fromSupplier(() -> {
                    synchronized (events) {
                        long first = events.size() + 1;
                        events.addAll(newEvents);
                        return new DcbAppendResult(first, events.size(), newEvents.size());
                    }
                }));
            });
        }

        @Override
        public Mono<DcbAppendResult> append(List<CloudEvent> newEvents, DcbAppendCondition condition) {
            return append(newEvents);
        }
    }

    private record TestEvent(String type) {
    }

    private static final class TestEventConverter implements CloudEventConverter<TestEvent> {
        @Override
        public CloudEvent toCloudEvent(TestEvent domainEvent) {
            return CloudEventBuilder.v1()
                    .withId(UUID.randomUUID().toString())
                    .withSource(URI.create("urn:occurrent:test"))
                    .withType(domainEvent.type())
                    .build();
        }

        @Override
        public TestEvent toDomainEvent(CloudEvent cloudEvent) {
            return new TestEvent(cloudEvent.getType());
        }

        @Override
        public String getCloudEventType(Class<? extends TestEvent> type) {
            throw new UnsupportedOperationException();
        }
    }
}
