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
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.QuietPositionReportingSubscriptions;
import org.occurrent.subscription.api.reactor.QuietPositionReportingSubscriptions.QuietPositionListener;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The save of the quiet position a wrapped model reports, which the durable model offers the wrapped model as a
 * function before each read. The wrapped model here records the listener instead of reading anything, so a
 * test asks for the function and calls it when it wants.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorDurableSubscriptionModelQuietPositionTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final Duration SHORTER_THAN_ANY_WAIT = Duration.ofNanos(1);
    private static final String SUBSCRIPTION_ID = "sub";
    private static final StringBasedCheckpoint STARTS_AT = new StringBasedCheckpoint("starts-at");
    private static final StringBasedCheckpoint QUIET_POSITION = new StringBasedCheckpoint("quiet-position");

    @Test
    void a_quiet_position_is_saved_as_the_checkpoint_of_the_subscription() {
        // Given
        QuietPositionReportingModel wrapped = new QuietPositionReportingModel();
        HoldableStorage storage = new HoldableStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage, quickToSave(1));
        subscribeFromTheStoredStart(model, storage);

        // When
        saveFunctionFor(wrapped).apply(QUIET_POSITION).block(TIMEOUT);

        // Then
        assertThat(storedPosition(storage)).as("checkpoint stored after the quiet position was saved").isEqualTo(QUIET_POSITION.asString());
    }

    @Test
    void a_cancel_waits_for_a_quiet_save_under_way_so_no_checkpoint_is_left_behind() throws Exception {
        // Given
        QuietPositionReportingModel wrapped = new QuietPositionReportingModel();
        HoldableStorage storage = new HoldableStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage, quickToSave(1));
        subscribeFromTheStoredStart(model, storage);
        Function<Checkpoint, Mono<Void>> save = saveFunctionFor(wrapped);
        CountDownLatch releaseSave = storage.holdSaves();

        try {
            // When
            CompletableFuture<Void> saved = save.apply(QUIET_POSITION).toFuture();
            assertThat(storage.saveEntered.await(5, TimeUnit.SECONDS)).as("the quiet save has reached the storage").isTrue();
            CompletableFuture<Void> cancelled = model.cancelSubscription(SUBSCRIPTION_ID).toFuture();
            releaseSave.countDown();
            cancelled.get(5, TimeUnit.SECONDS);
            saved.get(5, TimeUnit.SECONDS);

            // Then
            assertThat(storedPosition(storage)).as("checkpoint stored after a cancel that came while a quiet save was under way").isNull();
        } finally {
            releaseSave.countDown();
        }
    }

    @Test
    void a_quiet_save_that_starts_after_the_cancel_completed_stores_nothing_and_completes() {
        // Given
        QuietPositionReportingModel wrapped = new QuietPositionReportingModel();
        HoldableStorage storage = new HoldableStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage, quickToSave(1));
        subscribeFromTheStoredStart(model, storage);
        Function<Checkpoint, Mono<Void>> save = saveFunctionFor(wrapped);
        model.cancelSubscription(SUBSCRIPTION_ID).block(TIMEOUT);

        // When
        save.apply(QUIET_POSITION).block(TIMEOUT);

        // Then
        assertThat(storedPosition(storage)).as("checkpoint stored by a quiet save made after the cancel completed").isNull();
    }

    @Test
    void a_failed_quiet_save_fails_what_the_wrapped_model_waits_for_and_the_next_read_is_offered_the_save_again() {
        // Given
        QuietPositionReportingModel wrapped = new QuietPositionReportingModel();
        HoldableStorage storage = new HoldableStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage, quickToSave(1));
        subscribeFromTheStoredStart(model, storage);
        RuntimeException failure = new IllegalStateException("The storage cannot save right now");
        storage.failNextSave = failure;

        // When
        Mono<Void> failedSave = saveFunctionFor(wrapped).apply(QUIET_POSITION);

        // Then
        StepVerifier.create(failedSave).expectErrorMatches(error -> error == failure).verify(TIMEOUT);
        assertThat(storedPosition(storage)).as("checkpoint stored by the save that failed").isEqualTo(STARTS_AT.asString());
        Function<Checkpoint, Mono<Void>> offeredAgain = wrapped.beforeReading(SUBSCRIPTION_ID);
        assertThat(offeredAgain).as("save offered for the read that follows the failed one").isNotNull();
        offeredAgain.apply(QUIET_POSITION).block(TIMEOUT);
        assertThat(storedPosition(storage)).as("checkpoint stored by the save that followed the failed one").isEqualTo(QUIET_POSITION.asString());
    }

    @Test
    void a_failed_save_after_an_event_fails_the_action_the_same_way() {
        // Given
        QuietPositionReportingModel wrapped = new QuietPositionReportingModel();
        HoldableStorage storage = new HoldableStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage, quickToSave(1));
        subscribeFromTheStoredStart(model, storage);
        RuntimeException failure = new IllegalStateException("The storage cannot save right now");
        storage.failNextSave = failure;

        // When
        Mono<Void> delivery = wrapped.actions.getFirst().apply(eventAt(1));

        // Then
        StepVerifier.create(delivery).expectErrorMatches(error -> error == failure).verify(TIMEOUT);
    }

    @Test
    void a_quiet_save_is_offered_after_a_failed_delivery_once_a_later_event_is_stored() {
        // Given
        QuietPositionReportingModel wrapped = new QuietPositionReportingModel();
        HoldableStorage storage = new HoldableStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage, quickToSave(1));
        subscribeFromTheStoredStart(model, storage);
        Function<CloudEvent, Mono<Void>> action = wrapped.actions.getFirst();
        RuntimeException failure = new IllegalStateException("The storage cannot save right now");
        storage.failNextSave = failure;
        StepVerifier.create(action.apply(eventAt(1))).expectErrorMatches(error -> error == failure).verify(TIMEOUT);

        // When
        action.apply(eventAt(2)).block(TIMEOUT);

        // Then
        assertThat(wrapped.beforeReading(SUBSCRIPTION_ID)).as("save offered once an event is stored after a delivery that failed").isNotNull();
    }

    @Test
    void an_event_the_predicate_declined_blocks_quiet_saves_until_a_later_event_is_stored() {
        // Given
        QuietPositionReportingModel wrapped = new QuietPositionReportingModel();
        HoldableStorage storage = new HoldableStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage, quickToSave(2));
        subscribeFromTheStoredStart(model, storage);
        Function<CloudEvent, Mono<Void>> action = wrapped.actions.getFirst();

        // When
        action.apply(eventAt(1)).block(TIMEOUT);
        Function<Checkpoint, Mono<Void>> offeredAfterTheDeclinedEvent = wrapped.beforeReading(SUBSCRIPTION_ID);
        action.apply(eventAt(2)).block(TIMEOUT);
        Function<Checkpoint, Mono<Void>> offeredAfterTheStoredEvent = wrapped.beforeReading(SUBSCRIPTION_ID);

        // Then
        assertThat(offeredAfterTheDeclinedEvent).as("save offered while the latest event is one the predicate declined").isNull();
        assertThat(offeredAfterTheStoredEvent).as("save offered once the latest event is stored").isNotNull();
    }

    @Test
    void a_declined_event_delivered_from_the_completion_of_a_stored_one_blocks_quiet_saves() {
        // Given
        QuietPositionReportingModel wrapped = new QuietPositionReportingModel();
        HoldableStorage storage = new HoldableStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage, quickToSave(2));
        subscribeFromTheStoredStart(model, storage);
        Function<CloudEvent, Mono<Void>> action = wrapped.actions.getFirst();
        action.apply(eventAt(1)).block(TIMEOUT);

        // When the wrapped model delivers event 3 from the completion of event 2, as concatMap does
        action.apply(eventAt(2)).then(Mono.defer(() -> action.apply(eventAt(3)))).block(TIMEOUT);

        // Then
        // Read again for 500 ms, since a delivery counted as ended late would only show up after the block returned
        await().atMost(TIMEOUT).during(Duration.ofMillis(500)).untilAsserted(() ->
                assertThat(wrapped.beforeReading(SUBSCRIPTION_ID)).as("save offered while the latest event is one the predicate declined").isNull());
    }

    @Test
    void a_delivery_cancelled_by_its_subscriber_from_inside_the_completion_is_counted_as_ended_once() {
        // Given
        QuietPositionReportingModel wrapped = new QuietPositionReportingModel();
        HoldableStorage storage = new HoldableStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage, quickToSave(1));
        subscribeFromTheStoredStart(model, storage);

        // When
        CancellingOnCompleteSubscriber subscriber = new CancellingOnCompleteSubscriber();
        wrapped.actions.getFirst().apply(eventAt(1)).subscribe(subscriber);
        subscriber.completed.orTimeout(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).join();

        // Then
        assertThat(wrapped.beforeReading(SUBSCRIPTION_ID)).as("save offered once the only delivery has ended and was stored").isNotNull();
    }

    @Test
    void a_cancel_that_stalls_before_counting_the_end_of_a_stored_delivery_does_not_let_a_declined_event_end_before_it() throws Exception {
        // Given
        QuietPositionReportingModel wrapped = new QuietPositionReportingModel();
        HoldableStorage storage = new HoldableStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage, quickToSave(2));
        subscribeFromTheStoredStart(model, storage);
        Function<CloudEvent, Mono<Void>> action = wrapped.actions.getFirst();
        action.apply(eventAt(1)).block(TIMEOUT);
        // The cancelling thread is held where it has won the end and not yet counted it, so the completion comes second
        CountDownLatch cancellerHeld = new CountDownLatch(1);
        CountDownLatch releaseCanceller = new CountDownLatch(1);
        AtomicBoolean heldOnce = new AtomicBoolean();
        model.runBeforeADeliveryEndIsCounted(() -> {
            if (heldOnce.compareAndSet(false, true)) {
                cancellerHeld.countDown();
                awaitUninterruptibly(releaseCanceller);
            }
        });
        CountDownLatch releaseSave = storage.holdSaves();
        CompletableFuture<Void> declinedDelivered = new CompletableFuture<>();
        CancellableSubscriber subscriber = new CancellableSubscriber(() ->
                action.apply(eventAt(3)).subscribe(__ -> {
                }, declinedDelivered::completeExceptionally, () -> declinedDelivered.complete(null)));
        Thread canceller = new Thread(subscriber::cancel, "canceller");

        try {
            // When event 2 is stored while its subscriber cancels, and event 3 is delivered from the completion
            action.apply(eventAt(2)).subscribe(subscriber);
            assertThat(storage.saveEntered.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("the save of event 2 has reached the storage").isTrue();
            canceller.start();
            assertThat(cancellerHeld.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("the cancel has won the end of event 2 and is held").isTrue();
            releaseSave.countDown();
            Thread completer = requireNonNull(storage.savingThread, "savingThread");
            // Event 3 has ended without waiting for the cancel, or the completion waits on the monitor the held cancel owns
            await().atMost(TIMEOUT).until(() -> declinedDelivered.isDone() || isBlockedOnQuietSaves(completer));
        } finally {
            releaseSave.countDown();
            releaseCanceller.countDown();
            canceller.join(TIMEOUT.toMillis());
        }
        declinedDelivered.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Then
        assertThat(canceller.isAlive()).as("the cancelling thread has finished").isFalse();
        assertThat(wrapped.beforeReading(SUBSCRIPTION_ID)).as("save offered while the latest event is one the predicate declined").isNull();
    }

    @Test
    void a_delivery_that_is_still_under_way_blocks_quiet_saves_even_when_another_delivery_was_stored() {
        // Given
        QuietPositionReportingModel wrapped = new QuietPositionReportingModel();
        HoldableStorage storage = new HoldableStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage, quickToSave(1));
        Sinks.Empty<Void> finishFirst = Sinks.empty();
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(STARTS_AT), event -> event.getId().equals("1") ? finishFirst.asMono() : Mono.empty());
        Function<CloudEvent, Mono<Void>> action = wrapped.actions.getFirst();
        CompletableFuture<Void> first = action.apply(eventAt(1)).toFuture();

        // When
        action.apply(eventAt(2)).block(TIMEOUT);
        Function<Checkpoint, Mono<Void>> offeredWhileTheFirstIsUnderWay = wrapped.beforeReading(SUBSCRIPTION_ID);
        finishFirst.tryEmitEmpty();
        first.join();
        Function<Checkpoint, Mono<Void>> offeredOnceTheFirstIsStored = wrapped.beforeReading(SUBSCRIPTION_ID);

        // Then
        assertThat(offeredWhileTheFirstIsUnderWay).as("save offered while a delivery is under way").isNull();
        assertThat(offeredOnceTheFirstIsStored).as("save offered once every delivery has ended and the latest was stored").isNotNull();
    }

    @Test
    void a_persist_predicate_that_never_stores_has_no_position_saved_for_a_subscription_from_a_start_position_of_its_own() {
        // Given
        QuietPositionReportingModel wrapped = new QuietPositionReportingModel();
        HoldableStorage storage = new HoldableStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage,
                new ReactorDurableSubscriptionModelConfig(__ -> false).saveQuietPositionEvery(SHORTER_THAN_ANY_WAIT));
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(STARTS_AT), __ -> Mono.empty());

        // When the wrapped model reads nothing, delivers events the predicate declines, and reads nothing again
        saveIfOffered(wrapped, QUIET_POSITION);
        wrapped.actions.getFirst().apply(eventAt(1)).block(TIMEOUT);
        wrapped.actions.getFirst().apply(eventAt(2)).block(TIMEOUT);
        saveIfOffered(wrapped, new StringBasedCheckpoint("quiet-position-after-the-events"));

        // Then
        assertThat(storedPosition(storage)).as("checkpoint stored for a subscription from a start position of its own whose predicate never stores").isNull();
    }

    @Test
    void a_subscription_whose_start_position_was_recorded_has_its_quiet_position_saved_before_its_first_event_whatever_the_predicate() {
        // Given
        QuietPositionReportingModel wrapped = new QuietPositionReportingModel();
        HoldableStorage storage = new HoldableStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage,
                new ReactorDurableSubscriptionModelConfig(__ -> false).saveQuietPositionEvery(SHORTER_THAN_ANY_WAIT));
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty()).waitUntilStarted(TIMEOUT).block();

        // When
        Function<Checkpoint, Mono<Void>> offeredBeforeAnyEvent = wrapped.beforeReading(SUBSCRIPTION_ID);
        if (offeredBeforeAnyEvent != null) {
            offeredBeforeAnyEvent.apply(QUIET_POSITION).block(TIMEOUT);
        }
        wrapped.actions.getFirst().apply(eventAt(1)).block(TIMEOUT);
        Function<Checkpoint, Mono<Void>> offeredAfterADeclinedEvent = wrapped.beforeReading(SUBSCRIPTION_ID);

        // Then
        assertThat(offeredBeforeAnyEvent).as("save offered before the first event of a subscription whose start position was recorded").isNotNull();
        assertThat(storedPosition(storage)).as("checkpoint stored").isEqualTo(QUIET_POSITION.asString());
        assertThat(offeredAfterADeclinedEvent).as("save offered while the latest event is one the predicate declined").isNull();
    }

    @Test
    void no_save_is_offered_before_the_interval_has_passed() {
        // Given
        QuietPositionReportingModel wrapped = new QuietPositionReportingModel();
        HoldableStorage storage = new HoldableStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage,
                new ReactorDurableSubscriptionModelConfig(1).saveQuietPositionEvery(Duration.ofHours(1)));
        subscribeFromTheStoredStart(model, storage);

        // When
        Function<Checkpoint, Mono<Void>> offered = wrapped.beforeReading(SUBSCRIPTION_ID);

        // Then
        assertThat(offered).as("save offered long before the interval has passed").isNull();
    }

    @Test
    void a_save_is_offered_again_only_once_the_interval_has_passed_since_the_last_save() {
        // Given
        QuietPositionReportingModel wrapped = new QuietPositionReportingModel();
        HoldableStorage storage = new HoldableStorage();
        Duration interval = Duration.ofSeconds(1);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage,
                new ReactorDurableSubscriptionModelConfig(1).saveQuietPositionEvery(interval));
        subscribeFromTheStoredStart(model, storage);
        await().atMost(TIMEOUT).until(() -> wrapped.beforeReading(SUBSCRIPTION_ID) != null);
        wrapped.beforeReading(SUBSCRIPTION_ID).apply(QUIET_POSITION).block(TIMEOUT);

        // When
        Function<Checkpoint, Mono<Void>> offeredRightAfterTheSave = wrapped.beforeReading(SUBSCRIPTION_ID);

        // Then
        assertThat(offeredRightAfterTheSave).as("save offered right after a save").isNull();
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(wrapped.beforeReading(SUBSCRIPTION_ID)).as("save offered once the interval has passed since the last save").isNotNull());
    }

    @Test
    void the_listener_is_registered_with_the_wrapped_model_and_removed_at_shutdown() {
        // Given
        QuietPositionReportingModel wrapped = new QuietPositionReportingModel();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, new HoldableStorage(), quickToSave(1));
        List<QuietPositionListener> registered = List.copyOf(wrapped.listeners);

        // When
        model.shutdown();

        // Then
        assertThat(registered).as("listeners registered by the durable model").hasSize(1);
        assertThat(wrapped.removedListeners).as("listeners removed at shutdown").containsExactlyElementsOf(registered);
    }

    @Test
    void no_listener_is_registered_when_the_quiet_position_is_never_saved() {
        // Given
        QuietPositionReportingModel wrapped = new QuietPositionReportingModel();

        // When
        new ReactorDurableSubscriptionModel(wrapped, new HoldableStorage(), new ReactorDurableSubscriptionModelConfig(1).neverSaveQuietPosition());

        // Then
        assertThat(wrapped.listeners).as("listeners registered by a durable model that never saves the quiet position").isEmpty();
    }

    private static ReactorDurableSubscriptionModelConfig quickToSave(int persistPositionForEveryNCloudEvent) {
        return new ReactorDurableSubscriptionModelConfig(persistPositionForEveryNCloudEvent).saveQuietPositionEvery(SHORTER_THAN_ANY_WAIT);
    }

    // A subscribe from the model default that finds STARTS_AT stored, as after an earlier run stored a position, and
    // returns once the subscription is handed to the wrapped model, since the subscribe returns before it reads storage
    private static void subscribeFromTheStoredStart(ReactorDurableSubscriptionModel model, CheckpointStorage storage) {
        storage.save(SUBSCRIPTION_ID, STARTS_AT).block(TIMEOUT);
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty()).waitUntilStarted(TIMEOUT).block();
    }

    // What a wrapped model does with the function offered for a read that returned nothing
    private static void saveIfOffered(QuietPositionReportingModel wrapped, Checkpoint quietPosition) {
        @Nullable Function<Checkpoint, Mono<Void>> save = wrapped.beforeReading(SUBSCRIPTION_ID);
        if (save != null) {
            save.apply(quietPosition).block(TIMEOUT);
        }
    }

    private static Function<Checkpoint, Mono<Void>> saveFunctionFor(QuietPositionReportingModel wrapped) {
        Function<Checkpoint, Mono<Void>> save = wrapped.beforeReading(SUBSCRIPTION_ID);
        assertThat(save).as("save offered for the read of the subscription").isNotNull();
        return save;
    }

    private static CloudEvent eventAt(int position) {
        CloudEvent event = CloudEventBuilder.v1().withId(String.valueOf(position)).withSource(URI.create("urn:test")).withType("Something").build();
        return new CheckpointAwareCloudEvent(event, new StringBasedCheckpoint("event-" + position));
    }

    private static @Nullable String storedPosition(CheckpointStorage storage) {
        return storage.read(SUBSCRIPTION_ID).map(Checkpoint::asString).block(TIMEOUT);
    }

    // A named wrapped model that reports quiet positions to its listeners, and answers what they offer to the caller
    private static final class QuietPositionReportingModel extends NamedRecordingSubscriptionModel implements QuietPositionReportingSubscriptions {
        private final List<QuietPositionListener> listeners = new CopyOnWriteArrayList<>();
        private final List<QuietPositionListener> removedListeners = new CopyOnWriteArrayList<>();

        private QuietPositionReportingModel() {
            super("global");
        }

        @Override
        public void addQuietPositionListener(QuietPositionListener listener) {
            listeners.add(listener);
        }

        @Override
        public void removeQuietPositionListener(QuietPositionListener listener) {
            listeners.remove(listener);
            removedListeners.add(listener);
        }

        // What the function the listener offers for a read is, or null when it offers none
        private @Nullable Function<Checkpoint, Mono<Void>> beforeReading(String subscriptionId) {
            assertThat(listeners).as("listeners registered with the wrapped model").hasSize(1);
            return listeners.getFirst().beforeReading(subscriptionId).block(TIMEOUT);
        }
    }

    // A raw subscriber, since BaseSubscriber ignores a cancel that comes after the completion
    private static final class CancellingOnCompleteSubscriber implements CoreSubscriber<Void> {
        private final CompletableFuture<Void> completed = new CompletableFuture<>();
        private volatile @Nullable Subscription subscription;

        @Override
        public void onSubscribe(Subscription subscription) {
            this.subscription = subscription;
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(Void ignored) {
        }

        @Override
        public void onError(Throwable error) {
            completed.completeExceptionally(error);
        }

        @Override
        public void onComplete() {
            Subscription kept = subscription;
            if (kept == null) {
                completed.completeExceptionally(new IllegalStateException("No subscription was received in onSubscribe"));
                return;
            }
            kept.cancel();
            completed.complete(null);
        }
    }

    // A raw subscriber that runs what it is given on completion and can be cancelled from another thread, since
    // BaseSubscriber ignores a completion that comes after a cancel
    private static final class CancellableSubscriber implements CoreSubscriber<Void> {
        private final Runnable onComplete;
        private volatile @Nullable Subscription subscription;

        private CancellableSubscriber(Runnable onComplete) {
            this.onComplete = onComplete;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            this.subscription = subscription;
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(Void ignored) {
        }

        @Override
        public void onError(Throwable error) {
        }

        @Override
        public void onComplete() {
            onComplete.run();
        }

        private void cancel() {
            requireNonNull(subscription, "subscription").cancel();
        }
    }

    private static boolean isBlockedOnQuietSaves(Thread thread) {
        ThreadInfo info = ManagementFactory.getThreadMXBean().getThreadInfo(thread.threadId());
        return info != null && info.getThreadState() == Thread.State.BLOCKED
               && info.getLockName() != null && info.getLockName().contains("ReactorDurableSubscriptionModel$QuietSaves");
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    // Holds a save on a latch until released and fails the next one on request, both on a thread of their own
    private static final class HoldableStorage implements CheckpointStorage {
        private final InMemoryCheckpointStorage backing = new InMemoryCheckpointStorage();
        private final CountDownLatch saveEntered = new CountDownLatch(1);
        private volatile @Nullable CountDownLatch releaseSave;
        private volatile @Nullable RuntimeException failNextSave;
        private volatile @Nullable Thread savingThread;

        private CountDownLatch holdSaves() {
            CountDownLatch release = new CountDownLatch(1);
            releaseSave = release;
            return release;
        }

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return backing.read(subscriptionId);
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            return Mono.defer(() -> {
                RuntimeException failure = failNextSave;
                if (failure != null) {
                    failNextSave = null;
                    return Mono.<Checkpoint>error(failure);
                }
                CountDownLatch release = releaseSave;
                if (release != null) {
                    savingThread = Thread.currentThread();
                    saveEntered.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return Mono.<Checkpoint>error(e);
                    }
                }
                return backing.save(subscriptionId, checkpoint, condition);
            }).subscribeOn(Schedulers.boundedElastic());
        }

        @Override
        public Mono<Long> writeVersion(String subscriptionId) {
            return backing.writeVersion(subscriptionId);
        }

        @Override
        public Mono<Void> delete(String subscriptionId) {
            return backing.delete(subscriptionId);
        }

        @Override
        public Mono<Void> delete(String subscriptionId, CheckpointWriteCondition condition) {
            return backing.delete(subscriptionId, condition);
        }

        @Override
        public boolean evaluatesWriteConditions() {
            return backing.evaluatesWriteConditions();
        }

        @Override
        public boolean evaluatesDeleteConditions() {
            return backing.evaluatesDeleteConditions();
        }

        @Override
        public Mono<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate) {
            return backing.resolveFirstCheckpointRace(subscriptionId, candidate);
        }
    }
}
