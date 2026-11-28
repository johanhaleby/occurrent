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

package org.occurrent.subscription.blocking.durable.catchup.lifecycle;

import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelConfig;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;
import static org.occurrent.subscription.blocking.durable.catchup.lifecycle.GatedHistory.cloudEvent;

/**
 * The dispatcher over a controllable live model, whose storage holds the first read of x. A {@code start(true)} on a thread
 * of its own, made with the model stopped, waits ten seconds for that read, ends its turn, and owes x the resume that
 * runs once the read answers. That start returns once the resume has run, so a test knows the resume ran when the start returns.
 */
final class OwedResumeDispatcherFixture {

    static final String X = "x";
    static final long BOUND_SECONDS = 25;
    static final String OWED_RESUME_THREAD_PREFIX = "occurrent-catchup-start-resume";

    final StorageThatHoldsTheFirstReadOfX storage = new StorageThatHoldsTheFirstReadOfX();
    final ControllableLiveModel live = new ControllableLiveModel();
    final GatedHistory history = new GatedHistory(cloudEvent("1"));
    final CatchupSubscriptionModel model = new CatchupSubscriptionModel(new DurableSubscriptionModel(live, storage), history,
            new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage)));
    final List<String> delivered = new CopyOnWriteArrayList<>();

    static boolean onAnOwedResumeThread() {
        return Thread.currentThread().getName().startsWith(OWED_RESUME_THREAD_PREFIX);
    }

    void givenAPausedX(StartAt startAt) {
        model.subscribe(X, null, startAt, event -> delivered.add(event.getId()));
        model.pauseSubscription(X);
        assertThat(model.isPaused(X)).as("whether x is paused before the test starts").isTrue();
    }

    // A start(true) on a thread of its own with the model stopped, whose read of x is held past its turn so that it owes x the resume
    CompletableFuture<Void> aStartThatOwesTheResumeOfX() throws Exception {
        model.stop();
        storage.holds.set(true);
        CompletableFuture<Void> start = CompletableFuture.runAsync(() -> model.start(true), daemonExecutor("start-of-the-model"));
        assertThat(storage.entered.await(BOUND_SECONDS, TimeUnit.SECONDS)).as("whether the start read the storage for x").isTrue();
        await().atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(theResumeOwedFor(X)).as("whether the start owes x the resume it left until after its turn").isTrue());
        assertThat(start).as("the start while the read of x is held").isNotDone();
        return start;
    }

    boolean theResumeOwedFor(String subscriptionId) throws Exception {
        Method anyCatchupModel = CatchupSubscriptionModel.class.getDeclaredMethod("anyCatchupModel");
        anyCatchupModel.setAccessible(true);
        Object child = anyCatchupModel.invoke(model);
        Field owedResumes = abstractCatchupModelOf(child).getDeclaredField("owedResumes");
        owedResumes.setAccessible(true);
        return ((Map<?, ?>) owedResumes.get(child)).containsKey(subscriptionId);
    }

    // Whether any catch-up child of the dispatcher holds a replay of the subscription parked
    boolean hasParkedReplay(String subscriptionId) throws Exception {
        Method presentCatchupModels = CatchupSubscriptionModel.class.getDeclaredMethod("presentCatchupModels");
        presentCatchupModels.setAccessible(true);
        for (Object child : ((Stream<?>) presentCatchupModels.invoke(model)).toList()) {
            Method hasParkedReplay = abstractCatchupModelOf(child).getDeclaredMethod("hasParkedReplay", String.class);
            hasParkedReplay.setAccessible(true);
            if ((boolean) hasParkedReplay.invoke(child, subscriptionId)) {
                return true;
            }
        }
        return false;
    }

    private static Class<?> abstractCatchupModelOf(Object child) {
        Class<?> type = child.getClass();
        while (type != null && !type.getSimpleName().equals("AbstractCatchupSubscriptionModel")) {
            type = type.getSuperclass();
        }
        assertThat(type).as("the abstract catch-up model a child of the dispatcher extends").isNotNull();
        return type;
    }

    void publish(String eventId) {
        CheckpointAwareCloudEvent event = new CheckpointAwareCloudEvent(cloudEvent(eventId), new StringBasedCheckpoint("checkpoint-" + eventId));
        CompletableFuture.runAsync(() -> live.publish(event), daemonExecutor("live-delivery-" + eventId));
    }

    void releaseTheReadAndShutDown() throws Exception {
        storage.release.countDown();
        CompletableFuture.runAsync(model::shutdown, daemonExecutor("shutdown-of-the-model")).get(30, TimeUnit.SECONDS);
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
}
