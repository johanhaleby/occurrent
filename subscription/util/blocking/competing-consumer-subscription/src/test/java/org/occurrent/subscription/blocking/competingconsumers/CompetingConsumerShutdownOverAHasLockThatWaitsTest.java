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

package org.occurrent.subscription.blocking.competingconsumers;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A lease strategy of your own whose {@code hasLock} waits until the strategy is shut down, under a wrapped model whose
 * own {@code shutdown()} waits for the event it is handing over to reach the action. {@code shutdown()} shuts the lease
 * strategy down before the wrapped model, which ends that {@code hasLock}, so the event reaches the action and
 * {@code shutdown()} returns.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class CompetingConsumerShutdownOverAHasLockThatWaitsTest {

    private static final Duration EVENTUALLY = Duration.ofSeconds(5);
    // Long enough that a shutdown() that has not returned within EVENTUALLY is still waiting here
    private static final Duration WRAPPED_MODEL_WAITS_FOR_THE_EVENT_AT_MOST = Duration.ofSeconds(15);

    private final CountDownLatch e2Delivered = new CountDownLatch(1);
    private final CountDownLatch inHasLockForE2 = new CountDownLatch(1);
    private final CountDownLatch strategyShutDown = new CountDownLatch(1);
    private final List<String> received = new CopyOnWriteArrayList<>();
    private final WaitsForE2BeforeItShutsDown wrapped = new WaitsForE2BeforeItShutsDown(e2Delivered);
    private final HasLockWaitsForShutdown strategy = new HasLockWaitsForShutdown(inHasLockForE2, strategyShutDown);
    private final CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);

    @AfterEach
    void releaseEveryWait() {
        strategyShutDown.countDown();
        e2Delivered.countDown();
    }

    @Test
    void shutdown_returns_when_an_event_waits_in_a_has_lock_that_waits_for_the_lease_strategy_to_shut_down() throws Exception {
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), e -> {
            received.add(e.getId());
            if (e.getId().equals("e2")) {
                e2Delivered.countDown();
            }
        }).waitUntilStarted();
        wrapped.accept(List.of(event("e1")));
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(received).as("[events s1 received before hasLock waits]").containsExactly("e1"));
        strategy.waitsForShutdown = true;
        wrapped.accept(List.of(event("e2")));
        assertThat(inHasLockForE2.await(EVENTUALLY.toSeconds(), SECONDS)).as("hasLock is asked for e2 and waits").isTrue();

        CountDownLatch shutdownReturned = new CountDownLatch(1);
        Thread.ofPlatform().start(() -> {
            model.shutdown();
            shutdownReturned.countDown();
        });

        assertThat(shutdownReturned.await(EVENTUALLY.toSeconds(), SECONDS)).as("shutdown() returns while hasLock waits for the lease strategy to shut down").isTrue();
        assertThat(received).as("[events s1 received once shutdown() has returned]").containsExactly("e1", "e2");
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("Tested").build();
    }

    // Waits in its own shutdown() for e2 to reach the action, as a model that waits for the action it is running does
    private static final class WaitsForE2BeforeItShutsDown extends InMemorySubscriptionModel {
        private final CountDownLatch e2Delivered;

        private WaitsForE2BeforeItShutsDown(CountDownLatch e2Delivered) {
            super(RetryStrategy.none());
            this.e2Delivered = e2Delivered;
        }

        @Override
        public void shutdown() {
            try {
                e2Delivered.await(WRAPPED_MODEL_WAITS_FOR_THE_EVENT_AT_MOST.toSeconds(), SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            super.shutdown();
        }
    }

    // Grants every lease, and reports it held until told to wait. From then on hasLock waits until this strategy is
    // shut down, and then reports the lease not held.
    private static final class HasLockWaitsForShutdown implements CompetingConsumerStrategy {
        private final CountDownLatch inHasLock;
        private final CountDownLatch shutDown;
        private volatile boolean waitsForShutdown;

        private HasLockWaitsForShutdown(CountDownLatch inHasLock, CountDownLatch shutDown) {
            this.inHasLock = inHasLock;
            this.shutDown = shutDown;
        }

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            return true;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            if (!waitsForShutdown) {
                return true;
            }
            inHasLock.countDown();
            try {
                shutDown.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return false;
        }

        @Override
        public void addListener(CompetingConsumerListener listenerConsumer) {
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {
        }

        @Override
        public void shutdown() {
            shutDown.countDown();
        }
    }
}
