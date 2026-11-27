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
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.Duration;
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
import static org.awaitility.Awaitility.await;

/**
 * A replay whose handover to the live delegate is under way has left the attempts a stop parks, since it read the model
 * as not stopped before the stop began. A {@code stop()} must still wait for that handover, so that the live delegate
 * holds the subscription when the stop stops the delegate. Right after the stop returned the subscription then reads as
 * paused, never as neither paused nor running, and the next start runs it.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class StreamCatchupStopWaitsForAHandoverUnderWayTest {

    private static final String ID = "subscription";

    private final CountDownLatch inTheLiveSubscribe = new CountDownLatch(1);
    private final CountDownLatch releaseTheLiveSubscribe = new CountDownLatch(1);
    private InMemorySubscriptionModel inMemory;
    private InMemoryEventStore eventStore;
    private StreamCatchupSubscriptionModel model;

    @BeforeEach
    void create_a_model_over_a_live_delegate_whose_subscribe_for_the_subscription_can_be_held() {
        inMemory = new InMemorySubscriptionModel();
        HookedSubscriptionModel wrapped = new HookedSubscriptionModel(inMemory);
        wrapped.beforeSubscribe = id -> {
            if (ID.equals(id)) {
                inTheLiveSubscribe.countDown();
                HookedSubscriptionModel.awaitLatch(releaseTheLiveSubscribe);
            }
        };
        eventStore = new InMemoryEventStore(inMemory).withoutStreamPosition();
        model = new StreamCatchupSubscriptionModel(wrapped, eventStore, new CatchupSubscriptionModelConfig(100));
    }

    @AfterEach
    void release_everything_and_shut_down() {
        releaseTheLiveSubscribe.countDown();
        try {
            model.shutdown();
        } finally {
            inMemory.shutdown();
        }
    }

    @Test
    void a_stop_made_while_the_handover_of_a_replay_is_held_in_the_live_delegates_subscribe_leaves_the_subscription_paused_once_it_returns_and_the_next_start_runs_it() throws Exception {
        // Given a replay that has read the model as not stopped and is held inside the live delegate's subscribe
        write();
        model.subscribe(ID, StartAtTime.beginningOfTime(), event -> {
        });
        assertThat(inTheLiveSubscribe.await(10, TimeUnit.SECONDS)).as("whether the replay reached the live delegate's subscribe").isTrue();

        // When a stop is made from another thread, which records what it reads right after it returned
        AtomicBoolean runsAfterTheStop = new AtomicBoolean(true);
        AtomicBoolean pausedAfterTheStop = new AtomicBoolean();
        CompletableFuture<Void> stop = CompletableFuture.runAsync(() -> {
            model.stop();
            runsAfterTheStop.set(model.isRunning());
            pausedAfterTheStop.set(model.isPaused(ID));
        }, daemonExecutor("stop-of-the-model"));
        // A stop that waits for the handover has not returned by now, one that does not has. The handover is held until the first, so the test ends either way
        try {
            stop.get(1, TimeUnit.SECONDS);
        } catch (TimeoutException ignored) {
            // The stop is waiting for the handover
        }
        releaseTheLiveSubscribe.countDown();
        stop.get(10, TimeUnit.SECONDS);

        // Then
        assertThat(runsAfterTheStop).as("whether the model runs right after the stop returned").isFalse();
        assertThat(pausedAfterTheStop).as("whether the subscription reads as paused right after the stop returned").isTrue();

        // And the next start runs the subscription
        model.start();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(model.isRunning()).as("whether the model runs within 10 s of the next start()").isTrue();
            assertThat(model.isRunning(ID)).as("whether the subscription runs within 10 s of the next start()").isTrue();
            assertThat(model.isPaused(ID)).as("whether the subscription is still paused within 10 s of the next start()").isFalse();
        });
    }

    @Test
    void a_stop_made_while_the_handover_of_a_replay_is_held_in_the_live_delegates_subscribe_for_longer_than_a_park_waits_gives_up_with_the_model_stopped_all_the_same() throws Exception {
        // Given a replay that has read the model as not stopped and is held inside the live delegate's subscribe
        write();
        model.subscribe(ID, StartAtTime.beginningOfTime(), event -> {
        });
        assertThat(inTheLiveSubscribe.await(10, TimeUnit.SECONDS)).as("whether the replay reached the live delegate's subscribe").isTrue();

        // When a stop is made from another thread, and the handover is held past the 10 s a park waits
        StopOutcome stop = stopFromAnotherThreadWhileTheHandoverIsHeldFor15SecondsAtMost(releaseTheLiveSubscribe);

        // Then the stop gave up within 15 s and threw, and the model doesn't run
        assertThat(stop.endedWhileTheHandoverWasHeld).as("whether the stop ended within 15 s although the handover was held").isTrue();
        assertThat(stop.thrown).as("what the stop threw once it gave up on the handover").isInstanceOf(IllegalStateException.class);
        assertThat(model.isRunning()).as("whether the model runs right after the stop gave up").isFalse();

        // And the next start runs the model and the subscription, once the handover has ended
        releaseTheLiveSubscribe.countDown();
        model.start();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(model.isRunning()).as("whether the model runs within 10 s of the next start()").isTrue();
            assertThat(model.isRunning(ID)).as("whether the subscription runs within 10 s of the next start()").isTrue();
        });
    }

    // The handover is released after 15 s at the latest, well before a latch gives up on its own, so the test ends
    // whether or not the stop depends on it. A stop that gives up on the handover does so after 10 s
    private StopOutcome stopFromAnotherThreadWhileTheHandoverIsHeldFor15SecondsAtMost(CountDownLatch releaseTheHandover) throws Exception {
        CompletableFuture<@Nullable Throwable> stop = CompletableFuture.supplyAsync(() -> {
            try {
                model.stop();
                return null;
            } catch (Throwable e) {
                return e;
            }
        }, daemonExecutor("stop-of-the-model"));
        try {
            return new StopOutcome(true, stop.get(15, TimeUnit.SECONDS));
        } catch (TimeoutException stillWaiting) {
            releaseTheHandover.countDown();
            return new StopOutcome(false, stop.get(15, TimeUnit.SECONDS));
        }
    }

    private record StopOutcome(boolean endedWhileTheHandoverWasHeld, @Nullable Throwable thrown) {
    }

    private void write() {
        CloudEvent event = CloudEventBuilder.v1().withId(UUID.randomUUID().toString()).withSource(URI.create("urn:test")).withType("test.event").withTime(OffsetDateTime.now()).build();
        eventStore.write(UUID.randomUUID().toString(), List.of(event));
    }

    private static Executor daemonExecutor(String name) {
        return runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            thread.start();
        };
    }
}
