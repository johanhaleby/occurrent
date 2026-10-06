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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * An event that waits for the lease is delivered once the lease is back, or once this model pauses, stops or starts
 * the subscription, and is never lost, also over an {@code InMemorySubscriptionModel} that does not retry an action
 * that throws, and also when the lease strategy throws an {@link Error} while the event waits.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class CompetingConsumerLosesNoHeldEventTest {

    private static final Duration EVENTUALLY = Duration.ofSeconds(5);
    // Longer than the 200 ms an event waits at most between two looks at the lease
    private static final Duration AWAY_FOR = Duration.ofMillis(500);

    private final FenceStrategy strategy = new FenceStrategy();
    private final CountDownLatch inE1 = new CountDownLatch(1);
    private final CountDownLatch releaseE1 = new CountDownLatch(1);
    private final List<String> received = new CopyOnWriteArrayList<>();
    private @Nullable CompetingConsumerSubscriptionModel model;

    @AfterEach
    void shutdown() {
        releaseE1.countDown();
        if (model != null) {
            model.shutdown();
        }
    }

    @Test
    void an_event_waiting_for_the_lease_when_it_moves_to_another_node_and_back_is_delivered_by_a_model_that_does_not_retry() throws Exception {
        theLeaseMovesAwayAndBackWhileAnEventWaits(RetryStrategy.none());
    }

    @Test
    void an_event_waiting_for_the_lease_when_it_moves_to_another_node_and_back_is_delivered_by_a_model_that_retries_only_other_exceptions() throws Exception {
        theLeaseMovesAwayAndBackWhileAnEventWaits(RetryStrategy.fixed(200).retryIf(e -> !(e instanceof IllegalStateException)));
    }

    @Test
    void an_event_waiting_for_the_lease_while_this_model_is_stopped_is_delivered_once_it_starts_by_a_model_that_does_not_retry() throws Exception {
        InMemorySubscriptionModel inMemory = new InMemorySubscriptionModel(RetryStrategy.none());
        model = new CompetingConsumerSubscriptionModel(inMemory, strategy);
        subscribeAndBlockInE1(inMemory);

        model.stop();
        releaseE1.countDown();
        assertThat(strategy.askedWithoutTheLease.await(5, SECONDS)).as("e2 waits for the lease while this model is stopped").isTrue();
        model.start(true);
        await().atMost(EVENTUALLY).until(() -> model.isRunning("s1"));
        inMemory.accept(List.of(event("e3")));

        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(received).as("[events s1 received once this model was started again]").containsExactly("e1", "e2", "e3"));
    }

    @Test
    void an_event_for_which_the_lease_strategy_throws_an_error_waits_and_is_delivered_once_it_answers_by_a_model_that_does_not_retry() throws Exception {
        InMemorySubscriptionModel inMemory = new InMemorySubscriptionModel(RetryStrategy.none());
        model = new CompetingConsumerSubscriptionModel(inMemory, strategy);
        subscribeAndBlockInE1(inMemory);

        strategy.errorOnTheDeliveringThread = new Error("hasLock failed");
        releaseE1.countDown();
        assertThat(strategy.threwAnError.await(5, SECONDS)).as("hasLock throws an Error for e2").isTrue();
        // Long enough for e2 to ask more than once
        await().pollDelay(AWAY_FOR).atMost(AWAY_FOR.multipliedBy(2)).dontCatchUncaughtExceptions().until(() -> true);
        strategy.errorOnTheDeliveringThread = null;
        inMemory.accept(List.of(event("e3")));

        await().atMost(EVENTUALLY).dontCatchUncaughtExceptions().untilAsserted(() -> assertThat(received).as("[events s1 received after hasLock threw an Error for e2]").containsExactly("e1", "e2", "e3"));
    }

    // e1 runs while the lease closes without anyone being told, as the MongoDB lease strategies close it, so e2 waits for
    // it. The lease then goes to another node for a while and back.
    private void theLeaseMovesAwayAndBackWhileAnEventWaits(RetryStrategy retryStrategy) throws Exception {
        InMemorySubscriptionModel inMemory = new InMemorySubscriptionModel(retryStrategy);
        model = new CompetingConsumerSubscriptionModel(inMemory, strategy);
        subscribeAndBlockInE1(inMemory);

        strategy.fenced = true;
        releaseE1.countDown();
        assertThat(strategy.askedWithoutTheLease.await(5, SECONDS)).as("e2 waits for the lease").isTrue();
        strategy.transfer("s1", "node", "other-node");
        // An event lost on a thread of the wrapped model that dies of it shows in the events received, not as the exception
        await().pollDelay(AWAY_FOR).atMost(AWAY_FOR.multipliedBy(2)).dontCatchUncaughtExceptions().until(() -> true);
        strategy.fenced = false;
        strategy.transfer("s1", "other-node", "node");
        await().atMost(EVENTUALLY).dontCatchUncaughtExceptions().until(() -> inMemory.isRunning("s1"));
        inMemory.accept(List.of(event("e3")));

        await().atMost(EVENTUALLY).dontCatchUncaughtExceptions().untilAsserted(() -> assertThat(received).as("[events s1 received once the lease was back]").containsExactly("e1", "e2", "e3"));
    }

    private void subscribeAndBlockInE1(InMemorySubscriptionModel inMemory) throws InterruptedException {
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), e -> {
            if (e.getId().equals("e1")) {
                strategy.deliveringThread = Thread.currentThread();
                inE1.countDown();
                try {
                    releaseE1.await();
                } catch (InterruptedException x) {
                    Thread.currentThread().interrupt();
                }
            }
            received.add(e.getId());
        }).waitUntilStarted();
        inMemory.accept(List.of(event("e1"), event("e2")));
        assertThat(inE1.await(5, SECONDS)).as("e1 runs").isTrue();
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("Tested").build();
    }

    // Reports the lease of a subscription held by the node it went to, unless fenced, which closes it without telling
    // anyone. A transfer tells this node, on the calling thread, of the loss and then of the grant. Records when the
    // thread that delivered e1 asks without the lease, which is e2 waiting for it. Throws errorOnTheDeliveringThread, when
    // set, to the thread that delivered e1.
    static final class FenceStrategy implements CompetingConsumerStrategy {
        private final Map<String, String> holders = new ConcurrentHashMap<>();
        private final List<CompetingConsumerListener> listeners = new CopyOnWriteArrayList<>();
        final CountDownLatch askedWithoutTheLease = new CountDownLatch(1);
        final CountDownLatch threwAnError = new CountDownLatch(1);
        volatile @Nullable Error errorOnTheDeliveringThread;
        volatile boolean fenced;
        volatile @Nullable Thread deliveringThread;

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            String holder = holders.putIfAbsent(subscriptionId, subscriberId);
            return holder == null || holder.equals(subscriberId);
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            holders.remove(subscriptionId, subscriberId);
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            holders.remove(subscriptionId, subscriberId);
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            Error error = errorOnTheDeliveringThread;
            if (error != null && Thread.currentThread() == deliveringThread) {
                threwAnError.countDown();
                throw error;
            }
            boolean held = !fenced && subscriberId.equals(holders.get(subscriptionId));
            if (!held && Thread.currentThread() == deliveringThread) {
                askedWithoutTheLease.countDown();
            }
            return held;
        }

        @Override
        public void addListener(CompetingConsumerListener listenerConsumer) {
            listeners.add(listenerConsumer);
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {
            listeners.remove(listenerConsumer);
        }

        // A listener that throws is skipped, as the MongoDB lease strategies log it and tell the next one
        void transfer(String subscriptionId, String from, String to) {
            holders.put(subscriptionId, to);
            listeners.forEach(listener -> told(() -> listener.onConsumeProhibited(subscriptionId, from)));
            listeners.forEach(listener -> told(() -> listener.onConsumeGranted(subscriptionId, to)));
        }

        private static void told(Runnable callback) {
            try {
                callback.run();
            } catch (RuntimeException e) {
                LoggerFactory.getLogger(FenceStrategy.class).warn("A listener of the lease strategy threw", e);
            }
        }
    }
}
