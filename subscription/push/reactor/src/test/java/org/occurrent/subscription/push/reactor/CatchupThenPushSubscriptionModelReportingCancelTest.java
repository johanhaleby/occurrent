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

package org.occurrent.subscription.push.reactor;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

class CatchupThenPushSubscriptionModelReportingCancelTest {

    /**
     * The cancel is not deferred to whoever subscribes to what the method returns. The returned signal only reports
     * how the marker delete went, so a caller that ignores it still gets a cancelled subscription and a deleted marker.
     */
    @Test
    void the_cancel_takes_effect_before_anything_subscribes_to_what_it_returns() {
        // Given
        MarkerStorage marker = new MarkerStorage();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(historyOf("1", "2"), new PushSubscriptionModel(), marker);
        caughtUp(model, marker);

        // When
        model.cancelSubscriptionReportingCompletion("sub");

        // Then
        assertThat(model.isRunning("sub")).as("subscription running right after the call, with nothing subscribed to the returned Mono").isFalse();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(hasMarker(marker)).as("catch-up marker in storage after the call, with nothing subscribed to the returned Mono").isFalse());
    }

    @Test
    void completes_only_once_the_marker_delete_has_succeeded() throws Exception {
        // Given
        CountDownLatch deleteEntered = new CountDownLatch(1);
        CountDownLatch releaseDelete = new CountDownLatch(1);
        MarkerStorage marker = new MarkerStorage();
        marker.beforeDelete = Mono.<Void>fromRunnable(() -> {
            deleteEntered.countDown();
            awaitLatch(releaseDelete);
        }).subscribeOn(Schedulers.boundedElastic());
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(historyOf("1", "2"), new PushSubscriptionModel(), marker);
        caughtUp(model, marker);

        try {
            // When
            CompletableFuture<Void> cancelled = model.cancelSubscriptionReportingCompletion("sub").toFuture();
            assertThat(deleteEntered.await(5, TimeUnit.SECONDS)).as("the delete has reached the storage").isTrue();

            // Then
            assertThat(cancelled.isDone()).as("cancel reported as complete while the storage is still deleting the marker").isFalse();

            releaseDelete.countDown();
            cancelled.get(5, TimeUnit.SECONDS);
            assertThat(hasMarker(marker)).as("catch-up marker in storage at the moment the cancel reported completion").isFalse();
        } finally {
            releaseDelete.countDown();
        }
    }

    /**
     * The cancel arrives while the marker write is already running. The delete queues behind that write, so the
     * completion has to wait for the write and then for the delete, or it would report a marker that is about to appear.
     */
    @Test
    void completes_after_a_marker_write_that_was_running_when_the_cancel_landed_with_the_marker_gone() throws Exception {
        // Given
        CountDownLatch saveEntered = new CountDownLatch(1);
        CountDownLatch releaseSave = new CountDownLatch(1);
        MarkerStorage marker = new MarkerStorage();
        marker.beforeSave = Mono.fromRunnable(() -> {
            saveEntered.countDown();
            awaitLatch(releaseSave);
        });
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(historyOf("1", "2"), new PushSubscriptionModel(), marker);
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), e -> Mono.empty());
        assertThat(saveEntered.await(5, TimeUnit.SECONDS)).as("the catch-up has read its history and is writing the marker").isTrue();

        try {
            // When
            CompletableFuture<Void> cancelled = model.cancelSubscriptionReportingCompletion("sub").toFuture();

            // Then
            assertThat(cancelled.isDone()).as("cancel reported as complete while the marker write that was running is still in flight").isFalse();

            releaseSave.countDown();
            cancelled.get(5, TimeUnit.SECONDS);
            assertThat(hasMarker(marker)).as("catch-up marker in storage at the moment the cancel reported completion").isFalse();
        } finally {
            releaseSave.countDown();
        }
    }

    @Test
    void fails_when_the_marker_delete_fails_and_calling_it_again_deletes_the_marker() {
        // Given
        AtomicInteger failuresLeft = new AtomicInteger(1);
        MarkerStorage marker = new MarkerStorage();
        marker.beforeDelete = Mono.defer(() -> failuresLeft.getAndDecrement() > 0 ? Mono.error(new RuntimeException("The marker store is unavailable")) : Mono.empty());
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(historyOf("1", "2"), new PushSubscriptionModel(), marker);
        caughtUp(model, marker);

        // When
        Mono<Void> firstCancel = model.cancelSubscriptionReportingCompletion("sub");

        // Then
        Throwable failure = catchThrowable(() -> firstCancel.block(Duration.ofSeconds(5)));
        assertThat(failure).as("failure reported by the cancel whose marker delete failed").isNotNull();
        assertThat(failure).hasStackTraceContaining("The marker store is unavailable");
        assertThat(hasMarker(marker)).as("catch-up marker in storage after the delete failed").isTrue();

        // When
        model.cancelSubscriptionReportingCompletion("sub").block(Duration.ofSeconds(5));

        // Then
        assertThat(hasMarker(marker)).as("catch-up marker in storage after the second cancel completed").isFalse();
    }

    /**
     * The process ends after the call and before the delete reached the storage, so the caller never saw completion
     * and the marker survives the crash. A restarted process knows nothing of the subscription, and calling the method
     * there for the same id is how the caller finishes the cancel.
     */
    @Test
    void a_cancel_the_process_did_not_live_to_complete_is_finished_by_calling_it_again_after_a_restart() throws Exception {
        // Given
        PositionOrderedReader history = historyOf("1", "2");
        InMemoryCheckpointStorage durable = new InMemoryCheckpointStorage();
        CountDownLatch deleteEntered = new CountDownLatch(1);
        MarkerStorage dyingProcessStorage = new MarkerStorage(durable);
        dyingProcessStorage.beforeDelete = Mono.<Void>fromRunnable(deleteEntered::countDown).then(Mono.never());
        CatchupThenPushSubscriptionModel dyingProcess = new CatchupThenPushSubscriptionModel(history, new PushSubscriptionModel(), dyingProcessStorage);
        caughtUp(dyingProcess, durable);

        // When
        CompletableFuture<Void> cancelledBeforeTheCrash = dyingProcess.cancelSubscriptionReportingCompletion("sub").toFuture();
        assertThat(deleteEntered.await(5, TimeUnit.SECONDS)).as("the delete was started before the process ended").isTrue();

        // Then
        assertThat(cancelledBeforeTheCrash.isDone()).as("cancel reported as complete by a process that ended before its delete reached the storage").isFalse();
        assertThat(hasMarker(durable)).as("catch-up marker in the durable storage left by the crash").isTrue();

        // When
        PushSubscriptionModel restartedFeed = new PushSubscriptionModel();
        CatchupThenPushSubscriptionModel restartedProcess = new CatchupThenPushSubscriptionModel(history, restartedFeed, durable);
        restartedProcess.cancelSubscriptionReportingCompletion("sub").toFuture().get(5, TimeUnit.SECONDS);

        // Then
        assertThat(hasMarker(durable)).as("catch-up marker in the durable storage after the restarted process completed the cancel").isFalse();

        // When
        List<String> delivered = subscribe(restartedProcess);
        restartedFeed.accept(event("live")).block(Duration.ofSeconds(5));

        // Then
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(delivered).contains("live"));
        assertThat(delivered).as("events delivered to the subscription made after the restarted process completed the cancel").containsExactly("1", "2", "live");
    }

    private static void caughtUp(CatchupThenPushSubscriptionModel model, CheckpointStorage marker) {
        subscribe(model);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(hasMarker(marker)).as("catch-up marker written").isTrue());
    }

    private static boolean hasMarker(CheckpointStorage storage) {
        return Boolean.TRUE.equals(storage.read("sub").hasElement().block(Duration.ofSeconds(5)));
    }

    private static List<String> subscribe(CatchupThenPushSubscriptionModel model) {
        List<String> delivered = new CopyOnWriteArrayList<>();
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), e -> Mono.fromRunnable(() -> delivered.add(e.getId()))).waitUntilStarted().block(Duration.ofSeconds(5));
        return delivered;
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("The latch was never released");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static PositionOrderedReader historyOf(String... ids) {
        return new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return Flux.fromArray(ids).map(CatchupThenPushSubscriptionModelReportingCancelTest::event);
            }

            @Override
            public Mono<Long> currentPosition() {
                return Mono.just((long) ids.length);
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:occurrent:test")).withType("Created").build();
    }

    private static class MarkerStorage implements CheckpointStorage {
        private final CheckpointStorage backing;
        private volatile Mono<Void> beforeSave = Mono.empty();
        private volatile Mono<Void> beforeDelete = Mono.empty();

        private MarkerStorage() {
            this(new InMemoryCheckpointStorage());
        }

        private MarkerStorage(CheckpointStorage backing) {
            this.backing = backing;
        }

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return backing.read(subscriptionId);
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition writeCondition) {
            return Mono.defer(() -> beforeSave.then(Mono.defer(() -> backing.save(subscriptionId, checkpoint, writeCondition))));
        }

        @Override
        public Mono<Long> writeVersion(String subscriptionId) {
            return backing.writeVersion(subscriptionId);
        }

        @Override
        public Mono<Void> delete(String subscriptionId) {
            return Mono.defer(() -> beforeDelete.then(Mono.defer(() -> backing.delete(subscriptionId))));
        }
    }
}
