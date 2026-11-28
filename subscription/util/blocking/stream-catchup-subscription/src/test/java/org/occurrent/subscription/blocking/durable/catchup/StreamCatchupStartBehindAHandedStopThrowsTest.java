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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A {@code start(..)} that waits on the lifecycle thread behind a {@code stop()} a live action handed over throws what
 * the wrapped model's {@code start(..)} threw, as it does when it runs on the calling thread.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupStartBehindAHandedStopThrowsTest {

    private static final String ID = "subscription";
    private static final String THE_FAILURE = "the wrapped model failed to start";

    private InMemorySubscriptionModel inMemory;
    private HookedSubscriptionModel hooked;
    private InMemoryEventStore eventStore;
    private StreamCatchupSubscriptionModel model;
    private final List<CallOnAThread> calls = new ArrayList<>();
    private final List<CountDownLatch> latches = new ArrayList<>();

    @BeforeEach
    void create_a_model_over_a_wrapped_model_whose_start_can_be_made_to_fail() {
        inMemory = new InMemorySubscriptionModel();
        hooked = new HookedSubscriptionModel(inMemory);
        eventStore = new InMemoryEventStore(inMemory).withoutStreamPosition();
        model = new StreamCatchupSubscriptionModel(hooked, eventStore, new CatchupSubscriptionModelConfig(100));
    }

    @AfterEach
    void release_everything_and_shut_down() {
        try {
            hooked.beforeStart = () -> {
            };
            latches.forEach(CountDownLatch::countDown);
            calls.forEach(CallOnAThread::awaitEnded);
        } finally {
            model.shutdown();
            inMemory.shutdown();
        }
    }

    @Test
    @Timeout(40)
    void a_start_from_another_thread_that_waits_behind_a_stop_from_a_live_action_throws_what_the_wrapped_start_threw() throws InterruptedException {
        // Given
        CountDownLatch inTheAction = newLatch();
        CountDownLatch releaseTheAction = newLatch();
        model.subscribe(ID, null, StartAt.now(), event -> {
            model.stop();
            inTheAction.countDown();
            awaitLatch(releaseTheAction);
        });
        calls.add(new CallOnAThread(() -> eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent("1")))));
        assertThat(inTheAction.await(5, TimeUnit.SECONDS)).as("the live call is inside the action, after it handed the stop over").isTrue();
        hooked.beforeStart = () -> {
            throw new IllegalStateException(THE_FAILURE);
        };
        AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();
        CountDownLatch startEnded = newLatch();
        CallOnAThread start = new CallOnAThread(() -> {
            try {
                model.start(false);
            } catch (Throwable e) {
                thrown.set(e);
            } finally {
                startEnded.countDown();
            }
        });
        calls.add(start);
        start.awaitReturnedOrWaiting();

        // When
        releaseTheAction.countDown();

        // Then
        assertThat(startEnded.await(10, TimeUnit.SECONDS)).as("whether the start ended once the action had returned and the handed stop had run").isTrue();
        assertThat(thrown.get()).as("what the start that waited behind the handed stop threw").isInstanceOf(IllegalStateException.class).hasMessage(THE_FAILURE);
    }

    private CountDownLatch newLatch() {
        CountDownLatch latch = new CountDownLatch(1);
        latches.add(latch);
        return latch;
    }

    private static void awaitLatch(CountDownLatch latch) {
        HookedSubscriptionModel.awaitLatch(latch);
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("test.event").withTime(OffsetDateTime.now()).build();
    }
}
