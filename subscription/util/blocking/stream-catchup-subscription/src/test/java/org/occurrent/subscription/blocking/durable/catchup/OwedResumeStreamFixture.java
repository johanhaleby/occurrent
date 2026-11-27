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
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.subscription.internal.CatchupCheckpoints;

import java.lang.reflect.Field;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A stream model over an in-memory live delegate, whose storage holds the first read of x. A {@code start(true)} made on
 * a thread of its own then waits ten seconds for that read, ends its turn, and owes x the resume that runs once the
 * read answers. That start returns once the resume has run, so a test knows the resume ran when the start returns.
 */
final class OwedResumeStreamFixture {

    static final String X = "x";
    static final long BOUND_SECONDS = 25;
    static final String OWED_RESUME_THREAD_PREFIX = "occurrent-catchup-start-resume";
    private static final Checkpoint LIVE_START = new StringBasedCheckpoint("token-1");
    // Replays from position 1, after the beginning
    private static final Checkpoint STORED = new StringBasedCheckpoint(CatchupCheckpoints.format(1, 1, 3, LIVE_START));

    final AtomicBoolean xResumesFromTheBeginning = new AtomicBoolean();
    final CopyOnWriteArrayList<String> idsDeliveredToX = new CopyOnWriteArrayList<>();
    final InMemorySubscriptionModel inMemory = new InMemorySubscriptionModel();
    final RepositionableHookedModel wrapped = new RepositionableHookedModel(inMemory);
    final InMemoryEventStore eventStore = new InMemoryEventStore(inMemory);
    final StorageThatHoldsTheFirstReadOfX storage = new StorageThatHoldsTheFirstReadOfX();
    final StreamCatchupSubscriptionModel model;

    OwedResumeStreamFixture() {
        model = new StreamCatchupSubscriptionModel(wrapped, eventStore, new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        for (int i = 0; i < 3; i++) {
            write();
        }
    }

    // Whether the calling thread is one that runs the resume a start owes after its turn
    static boolean onAnOwedResumeThread() {
        return Thread.currentThread().getName().startsWith(OWED_RESUME_THREAD_PREFIX);
    }

    String write() {
        CloudEvent event = cloudEvent();
        eventStore.write(UUID.randomUUID().toString(), List.of(event));
        return event.getId();
    }

    void givenAPausedX(StartAt startAt) {
        model.subscribe(X, null, startAt, event -> idsDeliveredToX.add(event.getId()));
        model.pauseSubscription(X);
    }

    // X starts live, so it gets no history when it is subscribed. A resume of it replays from the stored position
    void givenXWhoseResumeReplaysFromItsStoredPosition() {
        storage.save(X, STORED);
        xResumesFromTheBeginning.set(true);
    }

    // Runs the callback on its first ask made on a thread that runs an owed resume, which is not the thread that
    // reads before the turn, and replays from the beginning once x is told to
    StartAt startAtThatRunsOnTheFirstAskOfAnOwedResume(Runnable callback) {
        AtomicBoolean asked = new AtomicBoolean();
        return StartAt.dynamic(() -> {
            if (onAnOwedResumeThread() && asked.compareAndSet(false, true)) {
                callback.run();
            }
            return xResumesFromTheBeginning.get() ? StartAtTime.beginningOfTime() : null;
        });
    }

    // A start(true) on a thread of its own, whose read of x is held past its turn so that it owes x the resume
    CompletableFuture<Void> aStartThatOwesTheResumeOfX() throws Exception {
        storage.holds.set(true);
        CompletableFuture<Void> start = CompletableFuture.runAsync(() -> model.start(true), daemonExecutor("start-of-the-model"));
        assertThat(storage.entered.await(BOUND_SECONDS, TimeUnit.SECONDS)).as("whether the start read the storage for x").isTrue();
        await().atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(theResumeOwedFor(X)).as("whether the start owes x the resume it left until after its turn").isTrue());
        assertThat(start).as("the start while the read of x is held").isNotDone();
        return start;
    }

    boolean theResumeOwedFor(String subscriptionId) throws Exception {
        Field owedResumes = AbstractCatchupSubscriptionModel.class.getDeclaredField("owedResumes");
        owedResumes.setAccessible(true);
        return ((Map<?, ?>) owedResumes.get(model)).containsKey(subscriptionId);
    }

    void releaseTheReadAndShutDown() throws Exception {
        storage.release.countDown();
        CompletableFuture.runAsync(() -> {
            model.shutdown();
            inMemory.shutdown();
        }, daemonExecutor("shutdown-of-the-model")).get(30, TimeUnit.SECONDS);
    }

    static CloudEvent cloudEvent() {
        return CloudEventBuilder.v1().withId(UUID.randomUUID().toString()).withSource(URI.create("urn:test")).withType("test.event").withTime(OffsetDateTime.now()).build();
    }

    static Executor daemonExecutor(String name) {
        return runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            thread.start();
        };
    }

    static final class StorageThatHoldsTheFirstReadOfX extends InMemoryCheckpointStorage {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicBoolean holds = new AtomicBoolean();

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            if (X.equals(subscriptionId) && holds.compareAndSet(true, false)) {
                entered.countDown();
                try {
                    release.await(80, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return super.read(subscriptionId);
        }
    }

    // A live delegate a catch-up can reposition, so a subscription whose replay has ended resumes in it
    static final class RepositionableHookedModel extends HookedSubscriptionModel implements RepositionableSubscriptions {
        RepositionableHookedModel(InMemorySubscriptionModel delegate) {
            super(delegate);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            return resumeSubscription(subscriptionId);
        }
    }
}
