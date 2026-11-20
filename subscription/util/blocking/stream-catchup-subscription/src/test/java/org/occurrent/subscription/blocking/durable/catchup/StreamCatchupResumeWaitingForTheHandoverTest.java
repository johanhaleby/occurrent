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
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * A resume that waits for the handover lock of a replay whose subscription was paused while the replay still read the
 * history takes effect after the handover, as the serial order subscribe, pause, resume says. The handover applies the pause
 * then, so the resume finds the subscription paused and resumes it, and it neither throws nor ends with it running and
 * the pause lost.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class StreamCatchupResumeWaitingForTheHandoverTest {

    private static final String A = "a";
    private static final Duration WAIT = Duration.ofSeconds(10);

    private InMemorySubscriptionModel inMemory;
    private InMemoryEventStore eventStore;
    private HookedSubscriptionModel hooked;
    private StreamCatchupSubscriptionModel model;

    @BeforeEach
    void create_a_started_model_over_an_in_memory_model() {
        inMemory = new InMemorySubscriptionModel();
        eventStore = new InMemoryEventStore(inMemory).withoutStreamPosition();
        hooked = new HookedSubscriptionModel(inMemory);
        model = new StreamCatchupSubscriptionModel(hooked, eventStore, new CatchupSubscriptionModelConfig(100));
    }

    @AfterEach
    void release_everything_and_shut_down() throws Exception {
        CompletableFuture.runAsync(() -> {
            model.shutdown();
            inMemory.shutdown();
        }).get(10, TimeUnit.SECONDS);
    }

    @Test
    void a_resume_that_waits_for_the_handover_lock_of_a_replay_paused_while_it_read_the_history_resumes_the_subscription_the_handover_paused() throws Exception {
        // Given a replay of a that is held in its action, and a pause of a made while it reads the history
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        CountDownLatch inTheAction = new CountDownLatch(1);
        CountDownLatch leaveTheAction = new CountDownLatch(1);
        CountDownLatch inTheHandover = new CountDownLatch(1);
        CountDownLatch leaveTheHandover = new CountDownLatch(1);
        hooked.beforeSubscribe = id -> {
            if (A.equals(id)) {
                inTheHandover.countDown();
                HookedSubscriptionModel.awaitLatch(leaveTheHandover);
            }
        };
        model.subscribe(A, StartAtTime.beginningOfTime(), event -> {
            inTheAction.countDown();
            HookedSubscriptionModel.awaitLatch(leaveTheAction);
        });
        assertThat(inTheAction.await(10, TimeUnit.SECONDS)).as("whether the replay of a reached its action").isTrue();
        model.pauseSubscription(A);

        // And the replay is in the handover, blocked in the live model's subscribe under the handover lock of a
        leaveTheAction.countDown();
        assertThat(inTheHandover.await(10, TimeUnit.SECONDS)).as("whether the replay of a reached the subscribe of the live model").isTrue();

        // And a resume of a made from another thread waits for that lock
        AtomicReference<Thread> resumingThread = new AtomicReference<>();
        CompletableFuture<Subscription> resume = CompletableFuture.supplyAsync(() -> {
            resumingThread.set(Thread.currentThread());
            return model.resumeSubscription(A);
        });
        await().atMost(WAIT).until(() -> resumingThread.get() != null && Arrays.stream(resumingThread.get().getStackTrace()).anyMatch(frame -> frame.getMethodName().equals("lockHandover")));

        // When the handover goes on
        leaveTheHandover.countDown();

        // Then the outcome is that of subscribe, pause and resume in that order, so the resume returns, its handle starts, and a runs and is not paused
        AtomicReference<Subscription> handleOfTheResume = new AtomicReference<>();
        assertThat(catchThrowable(() -> handleOfTheResume.set(resume.get(10, TimeUnit.SECONDS)))).as("what resumeSubscription(a) threw").isNull();
        assertThat(handleOfTheResume.get().waitUntilStarted(WAIT)).as("whether the handle of resumeSubscription(a) answers started").isTrue();
        assertThat(model.isPaused(A)).as("whether a is paused after the subscribe, the pause and the resume ran in that order").isFalse();
        assertThat(model.isRunning(A)).as("whether a runs after the subscribe, the pause and the resume ran in that order").isTrue();
    }

    private static CloudEvent cloudEvent() {
        return CloudEventBuilder.v1().withId(UUID.randomUUID().toString()).withSource(URI.create("urn:test")).withType("test.event").withTime(OffsetDateTime.now()).build();
    }
}
