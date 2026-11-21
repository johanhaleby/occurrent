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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A {@code shutdown()} from an ordinary thread doesn't wait for the calls queued on the lifecycle thread. A
 * {@code start(..)} from another thread that waits there behind a {@code stop()} a live action handed over must then
 * not call the wrapped model's {@code start(..)} once it runs, since the shutdown has shut that model down.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupShutdownSkipsAStartQueuedBehindAHandedStopTest {

    private static final String ID = "subscription";

    private InMemorySubscriptionModel inMemory;
    private HookedSubscriptionModel hooked;
    private InMemoryEventStore eventStore;
    private StreamCatchupSubscriptionModel model;
    private final List<CallOnAThread> calls = new ArrayList<>();
    private final List<CountDownLatch> latches = new ArrayList<>();

    @BeforeEach
    void create_a_model_over_a_wrapped_model_that_counts_its_starts() {
        inMemory = new InMemorySubscriptionModel();
        hooked = new HookedSubscriptionModel(inMemory);
        eventStore = new InMemoryEventStore(inMemory).withoutStreamPosition();
        model = new StreamCatchupSubscriptionModel(hooked, eventStore, new CatchupSubscriptionModelConfig(100));
    }

    @AfterEach
    void release_everything_and_shut_down() {
        try {
            hooked.shutsTheDelegateDown = true;
            latches.forEach(CountDownLatch::countDown);
            calls.forEach(CallOnAThread::awaitEnded);
        } finally {
            model.shutdown();
            inMemory.shutdown();
        }
    }

    @Test
    @Timeout(40)
    void a_shutdown_while_a_start_waits_behind_a_stop_from_a_live_action_returns_and_the_start_never_reaches_the_wrapped_model() throws InterruptedException {
        // Given
        CountDownLatch inTheAction = newLatch();
        CountDownLatch releaseTheAction = newLatch();
        model.subscribe(ID, null, StartAt.now(), event -> {
            model.stop();
            inTheAction.countDown();
            HookedSubscriptionModel.awaitLatch(releaseTheAction);
        });
        calls.add(new CallOnAThread(() -> eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent("1")))));
        assertThat(inTheAction.await(5, TimeUnit.SECONDS)).as("the live call is inside the action, after it handed the stop over").isTrue();
        hooked.shutsTheDelegateDown = false;
        CountDownLatch startEnded = newLatch();
        CallOnAThread start = new CallOnAThread(() -> {
            try {
                model.start(false);
            } finally {
                startEnded.countDown();
            }
        });
        calls.add(start);
        start.awaitReturnedOrWaiting();
        CountDownLatch shutdownReturned = newLatch();
        CallOnAThread shutdown = new CallOnAThread(() -> {
            model.shutdown();
            shutdownReturned.countDown();
        });
        calls.add(shutdown);

        // When
        boolean shutdownReturnedWhileTheActionBlocks = shutdownReturned.await(5, TimeUnit.SECONDS);
        int startsAfterTheShutdown = hooked.starts.get();
        releaseTheAction.countDown();
        boolean startEndedAfterTheRelease = startEnded.await(10, TimeUnit.SECONDS);

        // Then
        assertThat(shutdownReturnedWhileTheActionBlocks).as("whether the shutdown returned within 5 seconds while the action still blocked").isTrue();
        assertThat(startEndedAfterTheRelease).as("whether the start ended once the action had returned and the handed stop had run").isTrue();
        assertThat(hooked.starts.get()).as("the wrapped model's starts once the queued start had returned, against its starts when the shutdown returned").isEqualTo(startsAfterTheShutdown);
    }

    private CountDownLatch newLatch() {
        CountDownLatch latch = new CountDownLatch(1);
        latches.add(latch);
        return latch;
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("test.event").withTime(OffsetDateTime.now()).build();
    }
}
