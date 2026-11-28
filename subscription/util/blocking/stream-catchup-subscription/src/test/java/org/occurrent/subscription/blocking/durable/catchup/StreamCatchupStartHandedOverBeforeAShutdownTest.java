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

package org.occurrent.subscription.blocking.durable.catchup;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.IntrospectableSubscriptions;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A {@code start(true)} an action handed over runs once the action has returned. When a {@code shutdown()} has begun by
 * then, its turn would change nothing, so it reads no checkpoint for its resumes either, which includes the one the live delegate reads to prepare a resume.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupStartHandedOverBeforeAShutdownTest {

    private static final String A = "a";
    private static final String X = "x";

    private final CountDownLatch aHandedTheStartOver = new CountDownLatch(1);
    private final CountDownLatch releaseA = new CountDownLatch(1);
    private final CountDownLatch releaseTheLiveShutdown = new CountDownLatch(1);
    private final AtomicBoolean aIsInItsAction = new AtomicBoolean();
    private InMemorySubscriptionModel inMemory;
    private RepositionableHookedModel wrapped;
    private InMemoryEventStore eventStore;
    private CountingStorage storage;
    private StreamCatchupSubscriptionModel model;

    @BeforeEach
    void create_a_started_model_with_a_paused_x_and_an_a_whose_action_hands_a_start_over_and_blocks() {
        inMemory = new InMemorySubscriptionModel();
        eventStore = new InMemoryEventStore(inMemory).withoutStreamPosition();
        storage = new CountingStorage();
        wrapped = new RepositionableHookedModel(inMemory);
        wrapped.beforeShutdown = () -> awaitUninterruptibly(releaseTheLiveShutdown);
        model = new StreamCatchupSubscriptionModel(new DurableSubscriptionModel(wrapped, storage), eventStore, new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage)));
        model.subscribe(X, null, StartAt.now(), event -> {
        });
        model.pauseSubscription(X);
        model.subscribe(A, null, StartAt.now(), event -> {
            if (aIsInItsAction.compareAndSet(false, true)) {
                model.start(true);
                aHandedTheStartOver.countDown();
                awaitUninterruptibly(releaseA);
            }
        });
    }

    @AfterEach
    void release_everything_and_shut_down() throws Exception {
        releaseA.countDown();
        releaseTheLiveShutdown.countDown();
        CompletableFuture.runAsync(() -> {
            model.shutdown();
            inMemory.shutdown();
        }, daemonExecutor("shutdown-of-the-model")).get(30, TimeUnit.SECONDS);
    }

    @Test
    @Timeout(90)
    void a_start_an_action_handed_over_reads_no_checkpoint_once_a_shutdown_made_while_the_action_runs_has_begun() throws Exception {
        // Given the action of a has handed a start(true) over and blocks
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        assertThat(aHandedTheStartOver.await(10, TimeUnit.SECONDS)).as("whether the action of a handed its start over").isTrue();

        // When a shutdown is made from another thread, and begins before the action returns, and its shutdown of the live delegate is held
        CompletableFuture<Void> shutdown = CompletableFuture.runAsync(model::shutdown, daemonExecutor("shutdown-under-test"));
        await().atMost(25, TimeUnit.SECONDS).untilAsserted(() -> assertThat(model.shuttingDown).as("whether the shutdown has begun").isTrue());
        storage.counting = true;

        // And the action returns, so the start it handed over runs
        releaseA.countDown();
        Thread.sleep(2_000);
        releaseTheLiveShutdown.countDown();
        shutdown.handle((ignored, failure) -> null).get(40, TimeUnit.SECONDS);

        // Then the start read no checkpoint for x
        assertThat(storage.readsOfX.get()).as("the reads of the checkpoint of x from the storage of the live delegate after the shutdown had begun").isZero();
    }

    private static CloudEvent cloudEvent() {
        return CloudEventBuilder.v1().withId(UUID.randomUUID().toString()).withSource(URI.create("urn:test")).withType("test.event").withTime(OffsetDateTime.now()).build();
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static Executor daemonExecutor(String name) {
        return runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            thread.start();
        };
    }

    // A live delegate that lists x, which the model lists as one to read a checkpoint for once a shutdown has cleared its own record of x,
    // and that a DurableSubscriptionModel resumes at a stored position, so its prepared resume reads the storage
    private static final class RepositionableHookedModel extends HookedSubscriptionModel implements RepositionableSubscriptions, IntrospectableSubscriptions {
        RepositionableHookedModel(InMemorySubscriptionModel delegate) {
            super(delegate);
        }

        @Override
        public Set<String> subscriptionIds() {
            return Set.of(X);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            return resumeSubscription(subscriptionId);
        }
    }

    private static final class CountingStorage extends InMemoryCheckpointStorage {
        private final AtomicInteger readsOfX = new AtomicInteger();
        private volatile boolean counting;

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            if (counting && X.equals(subscriptionId)) {
                readsOfX.incrementAndGet();
            }
            return super.read(subscriptionId);
        }
    }
}
