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
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A {@code start(true)} nested inside another start runs on a thread that holds the model lock, either from the live
 * delegate's own {@code start(..)} or from a {@code StartAt.dynamic} function the outer start asks, which stops the model
 * and starts it again. It resumes the subscriptions the live delegate holds paused itself, one after another, since a
 * thread of its own would wait for the lock its caller holds. The nested start must return, then the outer one, and each
 * subscription must run and get the events written afterwards.
 * <p>
 * Each test asserts with a bounded wait on a start run off the test thread, so a start that never returns fails on its
 * assertion rather than on a timeout. The model is shut down from a daemon thread with a bounded wait, since a model
 * whose start holds its lock for good would hold a shutdown up too.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupNestedStartResumesPausedSubscriptionsTest {

    private static final List<String> IDS = List.of("first", "second");

    private InMemorySubscriptionModel inMemory;
    private HookedSubscriptionModel wrapped;
    private InMemoryEventStore eventStore;
    private StreamCatchupSubscriptionModel model;
    private final AtomicBoolean acted = new AtomicBoolean();
    private final AtomicReference<@Nullable Throwable> nestedStartThrew = new AtomicReference<>();
    private final CountDownLatch nestedStartReturned = new CountDownLatch(1);
    private final CountDownLatch outerStartReturned = new CountDownLatch(1);
    private final Map<String, List<String>> delivered = new ConcurrentHashMap<>();

    @BeforeEach
    void create_a_model_over_a_wrapped_model() {
        inMemory = new InMemorySubscriptionModel();
        wrapped = new HookedSubscriptionModel(inMemory);
        eventStore = new InMemoryEventStore(inMemory).withoutStreamPosition();
        model = new StreamCatchupSubscriptionModel(wrapped, eventStore, new CatchupSubscriptionModelConfig(100));
    }

    @AfterEach
    void shut_down_without_waiting_for_a_start_that_never_returned() {
        shutDownOnADaemonThread(model::shutdown);
        shutDownOnADaemonThread(inMemory::shutdown);
    }

    @Test
    @Timeout(60)
    void a_start_nested_in_the_start_of_the_live_delegate_returns_and_resumes_each_subscription_the_live_delegate_holds_paused() throws Exception {
        // Given two paused subscriptions in a stopped model, and a live delegate whose start starts the model
        givenTwoPausedSubscriptionsInAStoppedModel(() -> {
        });
        wrapped.beforeStart = () -> {
            if (acted.compareAndSet(false, true)) {
                nestedStart();
            }
        };

        // When the model is started
        startOffTheTestThread();

        // Then both starts return, and each subscription runs and gets what is written afterwards
        assertThatBothStartsReturnedAndEachSubscriptionResumed();
    }

    @Test
    @Timeout(60)
    void a_start_nested_in_a_start_at_function_the_outer_start_asks_returns_and_resumes_each_subscription_the_live_delegate_holds_paused() throws Exception {
        // Given two paused subscriptions in a stopped model, the first of which has a function that stops the model and starts it
        givenTwoPausedSubscriptionsInAStoppedModel(() -> {
            if (acted.compareAndSet(false, true)) {
                model.stop();
                nestedStart();
            }
        });

        // When the model is started
        startOffTheTestThread();

        // Then both starts return, and each subscription runs and gets what is written afterwards
        assertThatBothStartsReturnedAndEachSubscriptionResumed();
    }

    private void givenTwoPausedSubscriptionsInAStoppedModel(Runnable onAskedByTheStart) {
        AtomicBoolean armed = new AtomicBoolean();
        for (String id : IDS) {
            model.subscribe(id, null, StartAt.dynamic(() -> {
                if (armed.get() && id.equals("first")) {
                    onAskedByTheStart.run();
                }
                return null;
            }), event -> delivered.computeIfAbsent(id, key -> new CopyOnWriteArrayList<>()).add(event.getId()));
            model.pauseSubscription(id);
            assertThat(model.isPaused(id)).as("whether " + id + " is paused before the start").isTrue();
        }
        model.stop();
        armed.set(true);
    }

    private void nestedStart() {
        try {
            model.start(true);
        } catch (Throwable e) {
            nestedStartThrew.set(e);
        } finally {
            nestedStartReturned.countDown();
        }
    }

    private void startOffTheTestThread() {
        CompletableFuture.runAsync(() -> {
            try {
                model.start(true);
            } finally {
                outerStartReturned.countDown();
            }
        }, daemonExecutor("outer-start"));
    }

    private void assertThatBothStartsReturnedAndEachSubscriptionResumed() throws Exception {
        assertThat(nestedStartReturned.await(15, TimeUnit.SECONDS)).as("whether the nested start returned").isTrue();
        assertThat(outerStartReturned.await(15, TimeUnit.SECONDS)).as("whether the outer start returned").isTrue();
        assertThat(nestedStartThrew.get()).as("what the nested start threw").isNull();
        for (String id : IDS) {
            assertThat(model.isPaused(id)).as("whether " + id + " is paused once the starts returned").isFalse();
        }
        String event = write();
        for (String id : IDS) {
            await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> assertThat(delivered.getOrDefault(id, List.of())).as("the events " + id + " got after the starts returned").contains(event));
        }
    }

    private String write() {
        CloudEvent event = CloudEventBuilder.v1().withId(UUID.randomUUID().toString()).withSource(URI.create("urn:test")).withType("test.event").withTime(OffsetDateTime.now()).build();
        eventStore.write(UUID.randomUUID().toString(), List.of(event));
        return event.getId();
    }

    private static Executor daemonExecutor(String name) {
        return runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            thread.start();
        };
    }

    private static void shutDownOnADaemonThread(Runnable shutdown) {
        try {
            CompletableFuture.runAsync(shutdown, daemonExecutor("shutdown")).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            // A start that never returned holds the lock, and the daemon threads end with the test JVM
        }
    }
}
