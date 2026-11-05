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

package org.occurrent.subscription.reactor.durable;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.eventstore.api.reactor.internal.CatchupReadScope;
import org.occurrent.eventstore.api.reactor.internal.CatchupReads;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionNotRunningException;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import org.occurrent.subscription.reactor.durable.catchup.ReactorStreamCatchupSubscriptionModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * The subscribe a reactor catch-up calls with whether it has cancelled the subscribe, see
 * {@link org.occurrent.subscription.api.reactor.internal.CancellableRegistration}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorDurableSubscriptionModelCancellableRegistrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final Function<CloudEvent, Mono<Void>> NO_ACTION = __ -> Mono.empty();

    @Nested
    class OverAModelThatManagesNamedSubscriptions {

        @Test
        void registers_nothing_and_returns_null_when_the_call_was_cancelled() {
            // Given
            NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel("feed");
            ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage());

            // When
            Subscription subscription = model.subscribe("x", null, StartAt.now(), NO_ACTION, () -> true);
            List<String> subscribedAfterTheCall = List.copyOf(wrapped.subscribedIds);

            // Then
            assertAll(
                    () -> assertThat(subscription).as("the subscription returned").isNull(),
                    () -> assertThat(subscribedAfterTheCall).as("the subscribes the wrapped model got").isEmpty(),
                    () -> assertThat(model.subscribe("x", NO_ACTION).waitUntilStarted().block(TIMEOUT))
                            .as("what waiting for a subscribe of x afterwards gives").isNull());
        }

        @Test
        void hands_the_subscription_over_when_the_call_was_not_cancelled() {
            // Given
            NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel("feed");
            ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage());

            // When
            Subscription subscription = model.subscribe("x", null, StartAt.now(), NO_ACTION, () -> false);

            // Then
            assertAll(
                    () -> assertThat(requireNonNull(subscription).waitUntilStarted().block(TIMEOUT)).as("what waiting for the subscription gives").isNull(),
                    () -> assertThat(wrapped.subscribedIds).as("the subscribes the wrapped model got").containsExactly("x"));
        }

        @Test
        void returns_null_when_the_call_was_cancelled_and_the_id_is_taken() {
            // Given
            NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel("feed");
            ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage());
            model.subscribe("x", null, StartAt.now(), NO_ACTION).waitUntilStarted().block(TIMEOUT);

            // When
            CallResult result = CallResult.of(() -> model.subscribe("x", null, StartAt.now(), NO_ACTION, () -> true));

            // Then
            assertAll(
                    () -> assertThat(result.thrown).as("what the call threw").isNull(),
                    () -> assertThat(result.returned).as("the subscription returned").isNull());
        }

        @Test
        void returns_null_when_the_call_was_cancelled_and_the_model_is_shut_down() {
            // Given
            ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(new NamedRecordingSubscriptionModel("feed"), new InMemoryCheckpointStorage());
            model.shutdown();

            // When
            CallResult result = CallResult.of(() -> model.subscribe("x", null, StartAt.now(), NO_ACTION, () -> true));

            // Then
            assertAll(
                    () -> assertThat(result.thrown).as("what the call threw").isNull(),
                    () -> assertThat(result.returned).as("the subscription returned").isNull());
        }

        @Test
        void returns_null_when_the_call_is_cancelled_while_the_wrapped_model_is_asked_whether_it_holds_the_id() {
            // Given
            AtomicBoolean askedForTheId = new AtomicBoolean();
            NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel("feed") {
                @Override
                public boolean isRunning(String subscriptionId) {
                    askedForTheId.set(true);
                    return super.isRunning(subscriptionId);
                }
            };
            ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage());
            wrapped.subscribe("x", null, StartAt.now(), NO_ACTION);

            // When
            CallResult result = CallResult.of(() -> model.subscribe("x", null, StartAt.now(), NO_ACTION, askedForTheId::get));

            // Then
            assertAll(
                    () -> assertThat(result.thrown).as("what the call threw").isNull(),
                    () -> assertThat(result.returned).as("the subscription returned").isNull());
        }

        @Test
        void last_asks_whether_the_call_was_cancelled_holding_the_lock_its_cancel_takes() {
            // Given
            ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(new NamedRecordingSubscriptionModel("feed"), new InMemoryCheckpointStorage());
            Object positionLock = field(model, "positionLock");
            List<Boolean> askedHoldingTheLock = new CopyOnWriteArrayList<>();

            // When
            model.subscribe("x", null, StartAt.now(), NO_ACTION, () -> {
                askedHoldingTheLock.add(Thread.holdsLock(positionLock));
                return false;
            });

            // Then
            assertThat(askedHoldingTheLock).as("whether each ask held the lock the cancel takes").last().isEqualTo(true);
        }
    }

    @Nested
    class OverAModelThatOnlyStreamsEvents {

        @Test
        void registers_nothing_and_returns_null_when_the_call_was_cancelled() {
            // Given
            ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(new RecordingSubscriptionModel("feed"), new InMemoryCheckpointStorage());
            AtomicBoolean askedHoldingTheMonitor = new AtomicBoolean();

            // When
            Subscription subscription = model.subscribe("x", null, StartAt.now(), NO_ACTION, () -> {
                askedHoldingTheMonitor.set(Thread.holdsLock(model));
                return true;
            });

            // Then
            assertAll(
                    () -> assertThat(subscription).as("the subscription returned").isNull(),
                    () -> assertThat(askedHoldingTheMonitor).as("whether the model asked holding the monitor its cancel takes").isTrue(),
                    () -> assertThat(model.subscriptionIds()).as("the ids the model holds").isEmpty(),
                    () -> assertThat(model.subscribe("x", NO_ACTION).waitUntilStarted().block(TIMEOUT))
                            .as("what waiting for a subscribe of x afterwards gives").isNull());
        }

        @Test
        void registers_the_subscription_when_the_call_was_not_cancelled() {
            // Given
            ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(new RecordingSubscriptionModel("feed"), new InMemoryCheckpointStorage());

            // When
            Subscription subscription = model.subscribe("x", null, StartAt.now(), NO_ACTION, () -> false);

            // Then
            assertAll(
                    () -> assertThat(requireNonNull(subscription).waitUntilStarted().block(TIMEOUT)).as("what waiting for the subscription gives").isNull(),
                    () -> assertThat(model.isRunning("x")).as("whether the model runs x").isTrue());
        }

        @Test
        void returns_null_when_the_call_was_cancelled_and_the_id_is_taken() {
            // Given
            ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(new RecordingSubscriptionModel("feed"), new InMemoryCheckpointStorage());
            model.subscribe("x", NO_ACTION).waitUntilStarted().block(TIMEOUT);

            // When
            CallResult result = CallResult.of(() -> model.subscribe("x", null, StartAt.now(), NO_ACTION, () -> true));

            // Then
            assertAll(
                    () -> assertThat(result.thrown).as("what the call threw").isNull(),
                    () -> assertThat(result.returned).as("the subscription returned").isNull());
        }

        @Test
        void returns_null_when_the_call_was_cancelled_and_the_model_is_shut_down() {
            // Given
            ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(new RecordingSubscriptionModel("feed"), new InMemoryCheckpointStorage());
            model.shutdown();

            // When
            CallResult result = CallResult.of(() -> model.subscribe("x", null, StartAt.now(), NO_ACTION, () -> true));

            // Then
            assertAll(
                    () -> assertThat(result.thrown).as("what the call threw").isNull(),
                    () -> assertThat(result.returned).as("the subscription returned").isNull());
        }

        @Test
        void neither_takes_over_the_delete_of_a_cancel_nor_reads_storage_when_the_call_was_cancelled() throws Exception {
            // Given
            DeleteHeldStorage storage = new DeleteHeldStorage();
            storage.storage.save("x", new StringBasedCheckpoint("5"), CheckpointWriteCondition.any()).block(TIMEOUT);
            ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(new RecordingSubscriptionModel("feed"), storage);
            Mono<Void> cancel = model.cancelSubscription("x");
            awaitOrFail(storage.deleteEntered);
            int callsBeforeTheSubscribe = storage.calls.size();

            // When
            Subscription subscription = model.subscribe("x", null, StartAt.subscriptionModelDefault(), NO_ACTION, () -> true);
            List<String> callsOfTheSubscribe = List.copyOf(storage.calls.subList(callsBeforeTheSubscribe, storage.calls.size()));
            storage.releaseDeletes.countDown();
            cancel.block(TIMEOUT);

            // Then
            assertAll(
                    () -> assertThat(subscription).as("the subscription returned").isNull(),
                    () -> assertThat(callsOfTheSubscribe).as("what the subscribe asked storage").isEmpty(),
                    () -> assertThat(storage.calls).as("what storage was asked").doesNotContain("save x"),
                    () -> assertThat(storage.storage.read("x").blockOptional(TIMEOUT)).as("the checkpoint stored for x").isEmpty());
        }

        @Test
        void ends_its_read_of_the_position_when_the_call_is_cancelled_after_it_started_reading() {
            // Given
            UnansweredPositionModel wrapped = new UnansweredPositionModel();
            ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage());

            // When
            Subscription subscription = model.subscribe("x", null, StartAt.subscriptionModelDefault(), NO_ACTION, () -> wrapped.readsStarted.get() > 0);

            // Then
            assertAll(
                    () -> assertThat(subscription).as("the subscription returned").isNull(),
                    () -> assertThat(wrapped.readsStarted).as("the reads of the position the wrapped model got").hasValue(1),
                    () -> assertThat(wrapped.readsUnanswered).as("the reads of the position still waiting").hasValue(0));
        }

        @Test
        void ends_its_read_of_the_position_when_a_subscribe_of_the_id_registers_first() throws Exception {
            // Given
            UnansweredPositionModel wrapped = new UnansweredPositionModel();
            FirstReadHeldStorage storage = new FirstReadHeldStorage();
            ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage);
            CompletableFuture<Subscription> first = CompletableFuture.supplyAsync(
                    () -> model.subscribe("x", null, StartAt.subscriptionModelDefault(), NO_ACTION, () -> false));
            awaitOrFail(storage.readEntered);
            model.subscribe("x", null, StartAt.checkpoint(new StringBasedCheckpoint("1")), NO_ACTION);

            // When
            storage.releaseRead.countDown();
            Throwable thrown = catchThrowable(() -> first.get(TIMEOUT.toSeconds(), SECONDS));

            // Then
            assertAll(
                    () -> assertThat(thrown).as("what the first subscribe threw").hasCauseInstanceOf(DuplicateSubscriptionIdException.class),
                    () -> assertThat(wrapped.readsStarted).as("the reads of the position the wrapped model got").hasValue(1),
                    () -> assertThat(wrapped.readsUnanswered).as("the reads of the position still waiting").hasValue(0));
        }
    }

    @Nested
    class UnderAReactorCatchup {

        @Test
        void tells_a_model_whose_named_subscribe_is_not_overridden_of_a_cancel_that_came_first() throws Exception {
            // Given
            HeldOnceDurableModel durable = new HeldOnceDurableModel(new RecordingSubscriptionModel("feed"));
            ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(durable, new NoReplay());
            CompletableFuture<Subscription> cancelledSubscribe = CompletableFuture.supplyAsync(
                    () -> catchup.subscribe("x", StreamSubscriptionFilter.filter(Filter.all()), StartAt.now(), NO_ACTION));
            awaitOrFail(durable.held);
            catchup.cancelSubscription("x").block(TIMEOUT);

            // When
            durable.release.countDown();
            cancelledSubscribe.get(TIMEOUT.toSeconds(), SECONDS);

            // Then
            assertThat(durable.cancelledOnRelease).as("whether the subscribe the model got answered that it was cancelled").hasValue(true);
        }

        @Test
        void calls_the_named_subscribe_a_subclass_overrides() {
            // Given
            OverridingDurableModel durable = new OverridingDurableModel(new RecordingSubscriptionModel("feed"));
            ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(durable, new NoReplay());

            // When
            Subscription subscription = catchup.subscribe("x", StreamSubscriptionFilter.filter(Filter.all()), StartAt.now(), NO_ACTION);

            // Then
            assertAll(
                    () -> assertThat(durable.subscribed).as("the ids the override subscribed").containsExactly("x"),
                    () -> assertThat(subscription.waitUntilStarted().block(TIMEOUT)).as("what waiting for the subscription gives").isNull());
        }

        @Test
        void a_cancelled_subscribe_fails_its_start_after_a_later_subscribe_took_the_id_over_a_model_that_only_streams_events() throws Exception {
            cancelledSubscribeFailsItsStartAfterALaterSubscribeTookTheId(new RecordingSubscriptionModel("feed"));
        }

        @Test
        void a_cancelled_subscribe_fails_its_start_after_a_later_subscribe_took_the_id_over_a_model_that_manages_named_subscriptions() throws Exception {
            cancelledSubscribeFailsItsStartAfterALaterSubscribeTookTheId(new NamedRecordingSubscriptionModel("feed"));
        }

        private void cancelledSubscribeFailsItsStartAfterALaterSubscribeTookTheId(CheckpointAwareSubscriptionModel wrapped) throws Exception {
            // Given
            HeldOnceDurableModel durable = new HeldOnceDurableModel(wrapped);
            ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(durable, new NoReplay());
            StreamSubscriptionFilter filter = StreamSubscriptionFilter.filter(Filter.all());
            CompletableFuture<Subscription> cancelledSubscribe = CompletableFuture.supplyAsync(() -> catchup.subscribe("x", filter, StartAt.now(), NO_ACTION));
            awaitOrFail(durable.held);
            catchup.cancelSubscription("x").block(TIMEOUT);
            catchup.subscribe("x", filter, StartAt.now(), NO_ACTION).waitUntilStarted().block(TIMEOUT);

            // When
            durable.release.countDown();
            AtomicReference<Subscription> returned = new AtomicReference<>();
            Throwable thrown = catchThrowable(() -> returned.set(cancelledSubscribe.get(TIMEOUT.toSeconds(), SECONDS)));
            Throwable startFailure = returned.get() == null ? null : catchThrowable(() -> returned.get().waitUntilStarted().block(TIMEOUT));

            // Then
            assertAll(
                    () -> assertThat(thrown).as("what the cancelled subscribe threw").isNull(),
                    () -> assertThat(startFailure).as("what waiting for the cancelled subscribe threw").isInstanceOf(SubscriptionNotRunningException.class),
                    () -> assertThat(catchup.isRunning("x")).as("whether the later subscribe of x runs").isTrue());
        }
    }

    private static void awaitOrFail(CountDownLatch latch) throws InterruptedException {
        if (!latch.await(TIMEOUT.toSeconds(), SECONDS)) {
            throw new IllegalStateException("Timed out waiting for the test to reach its next step");
        }
    }

    private static Object field(ReactorDurableSubscriptionModel model, String name) {
        try {
            Field field = ReactorDurableSubscriptionModel.class.getDeclaredField(name);
            field.setAccessible(true);
            return field.get(model);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    private record CallResult(@Nullable Subscription returned, @Nullable Throwable thrown) {
        static CallResult of(java.util.function.Supplier<@Nullable Subscription> call) {
            try {
                return new CallResult(call.get(), null);
            } catch (RuntimeException e) {
                return new CallResult(null, e);
            }
        }
    }

    // Holds its first subscribe as it is entered, as if the thread were descheduled there, so the catch-up's cancel
    // and a later subscribe of the id come first
    private static final class HeldOnceDurableModel extends ReactorDurableSubscriptionModel {
        final CountDownLatch held = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        // What the held subscribe's cancelled answered once released
        final AtomicReference<@Nullable Boolean> cancelledOnRelease = new AtomicReference<>();
        private final AtomicBoolean holdNext = new AtomicBoolean(true);

        HeldOnceDurableModel(CheckpointAwareSubscriptionModel wrapped) {
            super(wrapped, new InMemoryCheckpointStorage());
        }

        @Override
        public @Nullable Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt,
                                                Function<CloudEvent, Mono<Void>> action, BooleanSupplier cancelled) {
            if (holdNext.compareAndSet(true, false)) {
                held.countDown();
                try {
                    awaitOrFail(release);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
                cancelledOnRelease.set(cancelled.getAsBoolean());
            }
            return super.subscribe(subscriptionId, filter, startAt, action, cancelled);
        }
    }

    // Overrides the named subscribe of SubscriptionModel, as a user's subclass can
    private static final class OverridingDurableModel extends ReactorDurableSubscriptionModel {
        final List<String> subscribed = new CopyOnWriteArrayList<>();

        OverridingDurableModel(CheckpointAwareSubscriptionModel wrapped) {
            super(wrapped, new InMemoryCheckpointStorage());
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
            subscribed.add(subscriptionId);
            return super.subscribe(subscriptionId, filter, startAt, action);
        }
    }

    // A subscribe at StartAt.now() goes straight to live, so nothing is replayed
    private static final class NoReplay implements PositionOrderedReader, CatchupReads {
        @Override
        public Mono<CatchupReadScope> openCatchupReadScope(@Nullable String notBefore) {
            return CatchupReads.unscoped(this).openCatchupReadScope(notBefore);
        }

        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            return Flux.error(new AssertionError("Nothing is replayed"));
        }

        @Override
        public Mono<Long> currentPosition() {
            return Mono.just(0L);
        }

        @Override
        public boolean writesPosition() {
            return true;
        }
    }

    // Never answers where the feed is, and counts the reads that still wait
    private static final class UnansweredPositionModel implements CheckpointAwareSubscriptionModel {
        final AtomicInteger readsStarted = new AtomicInteger();
        final AtomicInteger readsUnanswered = new AtomicInteger();

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.never();
        }

        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return unanswered();
        }

        @Override
        public Mono<Checkpoint> globalCheckpointAsOfNow() {
            return unanswered();
        }

        private Mono<Checkpoint> unanswered() {
            return Mono.<Checkpoint>never()
                    .doOnSubscribe(__ -> {
                        readsStarted.incrementAndGet();
                        readsUnanswered.incrementAndGet();
                    })
                    .doOnCancel(readsUnanswered::decrementAndGet);
        }
    }

    // Records what it is asked, and holds every delete until releaseDeletes
    private static final class DeleteHeldStorage implements CheckpointStorage {
        final InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        final List<String> calls = new CopyOnWriteArrayList<>();
        final CountDownLatch deleteEntered = new CountDownLatch(1);
        final CountDownLatch releaseDeletes = new CountDownLatch(1);

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return Mono.defer(() -> {
                calls.add("read " + subscriptionId);
                return storage.read(subscriptionId);
            });
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            return Mono.defer(() -> {
                calls.add("save " + subscriptionId);
                return storage.save(subscriptionId, checkpoint, condition);
            });
        }

        @Override
        public boolean evaluatesWriteConditions() {
            return storage.evaluatesWriteConditions();
        }

        @Override
        public Mono<Long> writeVersion(String subscriptionId) {
            return Mono.defer(() -> {
                calls.add("writeVersion " + subscriptionId);
                return storage.writeVersion(subscriptionId);
            });
        }

        @Override
        public Mono<Void> delete(String subscriptionId) {
            return Mono.defer(() -> {
                calls.add("delete " + subscriptionId);
                deleteEntered.countDown();
                return Mono.fromCallable(() -> releaseDeletes.await(TIMEOUT.toSeconds(), SECONDS))
                        .subscribeOn(Schedulers.boundedElastic())
                        .then(storage.delete(subscriptionId));
            });
        }
    }

    // Holds the caller of its first read until releaseRead
    private static final class FirstReadHeldStorage implements CheckpointStorage {
        private final InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        private final AtomicBoolean holdNext = new AtomicBoolean(true);
        final CountDownLatch readEntered = new CountDownLatch(1);
        final CountDownLatch releaseRead = new CountDownLatch(1);

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            if (holdNext.compareAndSet(true, false)) {
                readEntered.countDown();
                try {
                    awaitOrFail(releaseRead);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
            return storage.read(subscriptionId);
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            return storage.save(subscriptionId, checkpoint, condition);
        }

        @Override
        public boolean evaluatesWriteConditions() {
            return storage.evaluatesWriteConditions();
        }

        @Override
        public Mono<Long> writeVersion(String subscriptionId) {
            return storage.writeVersion(subscriptionId);
        }

        @Override
        public Mono<Void> delete(String subscriptionId) {
            return storage.delete(subscriptionId);
        }
    }
}
