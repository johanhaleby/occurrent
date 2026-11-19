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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.StartAt;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A subscribe that runs while a shutdown is between marking the records it forgets and forgetting them is never
 * forgotten by that shutdown without the mark, so a live call of that subscription reaches the action instead of
 * throwing AttemptNotMade.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class StreamCatchupSubscribeDuringShutdownSweepTest {

    private static final String ID = "live";

    private final KeepsTheLiveActionSubscriptionModel live = new KeepsTheLiveActionSubscriptionModel();
    private final List<String> received = new CopyOnWriteArrayList<>();
    private final CountDownLatch sweepBetweenMarksAndClear = new CountDownLatch(1);
    private final CountDownLatch releaseTheSweep = new CountDownLatch(1);
    private final StreamCatchupSubscriptionModel catchup = new StreamCatchupSubscriptionModel(live, new InMemoryEventStore(), new CatchupSubscriptionModelConfig(100));

    @AfterEach
    void release_the_sweep() {
        releaseTheSweep.countDown();
    }

    @Test
    void a_subscribe_made_while_the_shutdown_is_between_marking_the_records_and_forgetting_them_gets_its_live_event_to_the_action() throws InterruptedException {
        // Given a shutdown that holds between marking the records and forgetting them
        catchup.betweenShutdownMarksAndClear = () -> {
            sweepBetweenMarksAndClear.countDown();
            try {
                releaseTheSweep.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
        Thread shutdown = new Thread(catchup::markShuttingDown);
        shutdown.start();
        assertThat(sweepBetweenMarksAndClear.await(5, TimeUnit.SECONDS)).as("the shutdown is between marking the records and forgetting them").isTrue();

        // When an id is subscribed meanwhile, and the shutdown then goes on
        Thread subscribe = new Thread(() -> catchup.subscribe(ID, null, StartAt.now(), event -> received.add(event.getId())));
        subscribe.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> !subscribe.isAlive() || subscribe.getState() == Thread.State.BLOCKED);
        releaseTheSweep.countDown();
        subscribe.join(5000);
        shutdown.join(5000);
        live.callTheKeptAction(ID);

        // Then
        assertAll(
                () -> assertThat(live.thrownByTheAction()).as("what the live call threw").isEmpty(),
                () -> assertThat(received).as("the events the action received").containsExactly("live1"));
    }
}
