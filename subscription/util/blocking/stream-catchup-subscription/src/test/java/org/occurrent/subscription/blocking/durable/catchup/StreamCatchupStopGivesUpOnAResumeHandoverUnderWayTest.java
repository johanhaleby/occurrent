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
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.subscription.internal.CatchupCheckpoints;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A replay a resume started stays in the attempts a stop parks while it hands its subscription over inside the live
 * delegate's {@code resumeSubscription}, and holds the subscription's handover lock there. The stop waits for that lock
 * once, as a park does, and gives up after the lifecycle wait of 10 s with the model stopped all the same.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class StreamCatchupStopGivesUpOnAResumeHandoverUnderWayTest {

    private static final String X = "x";
    private static final String Y = "y";
    private static final Checkpoint LIVE_START = new StringBasedCheckpoint("token-1");
    // Replays from position 1, after the beginning
    private static final Checkpoint STORED = new StringBasedCheckpoint(CatchupCheckpoints.format(1, 1, 3, LIVE_START));

    private final CountDownLatch inTheLiveResume = new CountDownLatch(1);
    private final CountDownLatch releaseTheLiveResume = new CountDownLatch(1);
    private final CountDownLatch releaseTheLiveSubscribeOfY = new CountDownLatch(1);
    private InMemorySubscriptionModel inMemory;
    private RepositionableHookedModel wrapped;
    private InMemoryCheckpointStorage storage;
    private StreamCatchupSubscriptionModel model;

    @BeforeEach
    void create_a_model_over_a_live_delegate_whose_resume_can_be_held() {
        inMemory = new InMemorySubscriptionModel();
        wrapped = new RepositionableHookedModel(inMemory);
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemory);
        storage = new InMemoryCheckpointStorage();
        model = new StreamCatchupSubscriptionModel(wrapped, eventStore, new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage)));
        for (int i = 0; i < 3; i++) {
            eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        }
    }

    @AfterEach
    void release_everything_and_shut_down() {
        releaseTheLiveResume.countDown();
        releaseTheLiveSubscribeOfY.countDown();
        CompletableFuture<Void> shutdown = CompletableFuture.runAsync(() -> {
            model.shutdown();
            inMemory.shutdown();
        }, daemonExecutor("shutdown-of-the-model"));
        try {
            shutdown.get(10, TimeUnit.SECONDS);
        } catch (Exception ignored) {
            // A shutdown that hangs is left on its daemon thread, the assertions of the test have already reported it
        }
    }

    @Test
    void a_stop_made_while_the_replay_of_a_resume_is_held_in_the_live_delegates_resume_gives_up_within_the_lifecycle_wait_with_the_model_stopped_all_the_same() throws Exception {
        // Given the replay of a resume held inside the live delegate's resume of x
        givenTheReplayOfAResumeOfXIsHeldInTheLiveResume();

        // When a stop is made from another thread, and the resume is held past the 10 s a park waits
        CompletableFuture<@Nullable Throwable> stop = CompletableFuture.supplyAsync(() -> {
            try {
                model.stop();
                return null;
            } catch (Throwable e) {
                return e;
            }
        }, daemonExecutor("stop-of-the-model"));
        boolean endedWhileTheResumeWasHeld = true;
        Throwable thrown;
        try {
            thrown = stop.get(15, TimeUnit.SECONDS);
        } catch (TimeoutException stillWaiting) {
            // Released after 15 s at the latest, well before a latch gives up on its own, so the test ends whether or not the stop depends on it
            endedWhileTheResumeWasHeld = false;
            releaseTheLiveResume.countDown();
            thrown = stop.get(15, TimeUnit.SECONDS);
        }

        // Then the stop gave up within 15 s and threw
        assertThat(endedWhileTheResumeWasHeld).as("whether the stop ended within 15 s although the resume was held").isTrue();
        assertThat(thrown).as("what the stop threw once it gave up on the handover").isInstanceOf(IllegalStateException.class);
        assertThat(model.isRunning()).as("whether the model runs right after the stop gave up").isFalse();
    }

    @Test
    void a_stop_whose_park_of_a_held_resume_gives_up_still_waits_for_the_handover_of_another_subscription_so_it_reads_as_paused_once_the_stop_returned() throws Exception {
        // Given the replay of a resume of x held in the live delegate's resume, and the replay of y held in the live delegate's subscribe
        givenTheReplayOfAResumeOfXIsHeldInTheLiveResume();
        CountDownLatch inTheLiveSubscribeOfY = new CountDownLatch(1);
        wrapped.beforeSubscribe = id -> {
            if (Y.equals(id)) {
                inTheLiveSubscribeOfY.countDown();
                HookedSubscriptionModel.awaitLatch(releaseTheLiveSubscribeOfY);
            }
        };
        model.subscribe(Y, StartAtTime.beginningOfTime(), event -> {
        });
        assertThat(inTheLiveSubscribeOfY.await(10, TimeUnit.SECONDS)).as("whether the replay of y reached the live delegate's subscribe").isTrue();

        // When a stop is made from another thread, which records what it reads of y right after it returned
        AtomicBoolean yPausedAfterTheStop = new AtomicBoolean();
        CompletableFuture<@Nullable Throwable> stop = CompletableFuture.supplyAsync(() -> {
            try {
                model.stop();
                return null;
            } catch (Throwable e) {
                return e;
            } finally {
                yPausedAfterTheStop.set(model.isPaused(Y));
            }
        }, daemonExecutor("stop-of-the-model"));
        // The park of x gives up after 10 s. y is released at 12 s, and x at 25 s, so the test ends whether or not the stop depends on either
        Throwable thrown;
        try {
            thrown = stop.get(12, TimeUnit.SECONDS);
        } catch (TimeoutException yStillHeld) {
            releaseTheLiveSubscribeOfY.countDown();
            try {
                thrown = stop.get(13, TimeUnit.SECONDS);
            } catch (TimeoutException xStillHeld) {
                releaseTheLiveResume.countDown();
                thrown = stop.get(10, TimeUnit.SECONDS);
            }
        }

        // Then the stop threw, the model doesn't run, and y reads as paused right after the stop returned
        assertThat(thrown).as("what the stop threw once its park of x gave up").isInstanceOf(IllegalStateException.class);
        assertThat(model.isRunning()).as("whether the model runs right after the stop returned").isFalse();
        assertThat(yPausedAfterTheStop).as("whether y read as paused right after the stop returned").isTrue();
    }

    private void givenTheReplayOfAResumeOfXIsHeldInTheLiveResume() throws Exception {
        // X starts live and is paused, and replays from a stored position behind the history once resumed
        AtomicBoolean xResumesFromTheBeginning = new AtomicBoolean();
        model.subscribe(X, null, StartAt.dynamic(() -> xResumesFromTheBeginning.get() ? StartAtTime.beginningOfTime() : null), event -> {
        });
        model.pauseSubscription(X);
        xResumesFromTheBeginning.set(true);
        storage.save(X, STORED);
        wrapped.beforeResume = () -> {
            inTheLiveResume.countDown();
            HookedSubscriptionModel.awaitLatch(releaseTheLiveResume);
        };
        model.resumeSubscription(X);
        assertThat(inTheLiveResume.await(10, TimeUnit.SECONDS)).as("whether the replay reached the live delegate's resume of x").isTrue();
    }

    private static CloudEvent cloudEvent() {
        return CloudEventBuilder.v1().withId(UUID.randomUUID().toString()).withSource(URI.create("urn:test")).withType("test.event").withTime(OffsetDateTime.now()).build();
    }

    private static Executor daemonExecutor(String name) {
        return runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            thread.start();
        };
    }

    // A live delegate a catch-up can reposition, so a subscription whose replay has ended resumes in it
    private static final class RepositionableHookedModel extends HookedSubscriptionModel implements RepositionableSubscriptions {
        RepositionableHookedModel(InMemorySubscriptionModel delegate) {
            super(delegate);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            return resumeSubscription(subscriptionId);
        }
    }
}
