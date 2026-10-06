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

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * A node whose wrapped model throws from its own {@code shutdown()} may go on delivering, so {@code shutdown()} doesn't
 * shut its lease strategy down. The node keeps its lease, and delivers while it holds it. Once the lease has expired
 * and gone to another node, the node delivers nothing, however many events its wrapped model hands over.
 * <p>
 * The two nodes share leases that expire three ticks after their holder last refreshed them. A tick refreshes the
 * leases of each node whose lease strategy still runs and is not stalled, and then expires the rest, which go to a node
 * that waits for them. A stalled node is told nothing of the loss, as a node cut off from the lease store is not.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class CompetingConsumerShutdownOverAFailingWrappedModelTest {

    private static final Duration EVENTUALLY = Duration.ofSeconds(5);
    // Far longer than an event the wrapped model hands over takes to reach the action
    private static final Duration AFTERWARDS = Duration.ofMillis(500);
    // Well past the three ticks a lease lives without being refreshed
    private static final int PAST_EXPIRY = 10;

    private final ExpiringLeases leases = new ExpiringLeases();
    private final IllegalStateException wrappedModelFailure = new IllegalStateException("wrapped model shutdown failed");
    private final StopsDeliveringOnlyWhenTold wrapped1 = new StopsDeliveringOnlyWhenTold(wrappedModelFailure);
    private final InMemorySubscriptionModel wrapped2 = new InMemorySubscriptionModel(RetryStrategy.none());
    private final CompetingConsumerSubscriptionModel node1 = new CompetingConsumerSubscriptionModel(wrapped1, leases.strategyOf("node-1"));
    private final CompetingConsumerSubscriptionModel node2 = new CompetingConsumerSubscriptionModel(wrapped2, leases.strategyOf("node-2"));
    private final List<String> receivedByNode1 = new CopyOnWriteArrayList<>();
    private final List<String> receivedByNode2 = new CopyOnWriteArrayList<>();

    @AfterEach
    void shutdown() {
        wrapped1.failing = false;
        // Lets an event node 1 still holds go, so the wrapped model need not wait for it to shut down
        leases.give("s1", "node-1");
        node1.shutdown();
        node2.shutdown();
    }

    @Test
    void a_node_whose_wrapped_model_throws_from_its_own_shutdown_keeps_its_lease_and_delivers_under_it() throws Exception {
        node1WinsTheLeaseAndReceivesE1();

        Throwable thrown = catchThrowable(node1::shutdown);
        leases.tick(PAST_EXPIRY);
        wrapped1.accept(List.of(event("e2")));

        assertThat(thrown).as("[what shutdown() threw]").isSameAs(wrappedModelFailure);
        assertThat(leases.holderOf("s1")).as("[the node holding the lease of s1 well past the lease time]").isEqualTo("node-1");
        assertThat(node2.isRunning("s1")).as("node 2 runs s1").isFalse();
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(receivedByNode1).as("[events node 1 received]").containsExactly("e1", "e2"));
    }

    @Test
    void a_node_whose_wrapped_model_threw_from_its_own_shutdown_delivers_nothing_once_its_lease_has_gone_to_another_node() throws Exception {
        node1WinsTheLeaseAndReceivesE1();

        Throwable thrown = catchThrowable(node1::shutdown);
        leases.stall("node-1");
        leases.tick(PAST_EXPIRY);
        await().atMost(EVENTUALLY).until(() -> node2.isRunning("s1"));
        wrapped1.accept(List.of(event("e2")));
        wrapped2.accept(List.of(event("e2")));
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(receivedByNode2).as("[events node 2 received]").containsExactly("e2"));
        Thread.sleep(AFTERWARDS.toMillis());

        assertThat(receivedByNode1).as("[events node 1 received after its lease went to node 2]").containsExactly("e1");
        assertThat(leases.holderOf("s1")).as("[the node holding the lease of s1]").isEqualTo("node-2");
        assertThat(thrown).as("[what shutdown() threw]").isSameAs(wrappedModelFailure);
    }

    private void node1WinsTheLeaseAndReceivesE1() {
        node1.subscribe("node-1", "s1", null, StartAt.subscriptionModelDefault(), e -> receivedByNode1.add(e.getId())).waitUntilStarted();
        node2.subscribe("node-2", "s1", null, StartAt.subscriptionModelDefault(), e -> receivedByNode2.add(e.getId()));
        wrapped1.accept(List.of(event("e1")));
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(receivedByNode1).as("[events node 1 received before shutdown()]").containsExactly("e1"));
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("Tested").build();
    }

    // Throws from its own shutdown() without stopping, so it goes on delivering, until told not to
    private static final class StopsDeliveringOnlyWhenTold extends InMemorySubscriptionModel {
        private final IllegalStateException failure;
        private volatile boolean failing = true;

        private StopsDeliveringOnlyWhenTold(IllegalStateException failure) {
            super(RetryStrategy.none());
            this.failure = failure;
        }

        @Override
        public void shutdown() {
            if (failing) {
                throw failure;
            }
            super.shutdown();
        }
    }

    private static final class ExpiringLeases {
        private static final int LEASE_TICKS = 3;

        private final Map<String, Lease> leases = new HashMap<>();
        private final List<Waiting> waiting = new ArrayList<>();
        private final Map<String, NodeStrategy> nodes = new ConcurrentHashMap<>();
        private long now;

        NodeStrategy strategyOf(String subscriberId) {
            return nodes.computeIfAbsent(subscriberId, NodeStrategy::new);
        }

        synchronized @Nullable String holderOf(String subscriptionId) {
            Lease lease = leases.get(subscriptionId);
            return lease == null ? null : lease.holder;
        }

        synchronized void give(String subscriptionId, String subscriberId) {
            leases.put(subscriptionId, new Lease(subscriberId, now + LEASE_TICKS));
        }

        void stall(String subscriberId) {
            strategyOf(subscriberId).stalled = true;
        }

        void tick(int ticks) {
            for (int i = 0; i < ticks; i++) {
                tick().forEach(Runnable::run);
            }
        }

        private synchronized List<Runnable> tick() {
            now++;
            leases.values().forEach(lease -> {
                if (nodes.get(lease.holder).refreshes()) {
                    lease.expiresAt = now + LEASE_TICKS;
                }
            });
            List<Runnable> grants = new ArrayList<>();
            leases.entrySet().removeIf(entry -> {
                if (entry.getValue().expiresAt > now) {
                    return false;
                }
                String subscriptionId = entry.getKey();
                waiting.stream().filter(candidate -> candidate.subscriptionId.equals(subscriptionId)).findFirst().ifPresent(candidate -> {
                    waiting.remove(candidate);
                    grants.add(() -> {
                        give(subscriptionId, candidate.subscriberId);
                        nodes.get(candidate.subscriberId).granted(subscriptionId);
                    });
                });
                return true;
            });
            return grants;
        }

        private synchronized boolean register(String subscriptionId, String subscriberId) {
            Lease lease = leases.get(subscriptionId);
            if (lease == null) {
                leases.put(subscriptionId, new Lease(subscriberId, now + LEASE_TICKS));
                return true;
            }
            if (lease.holder.equals(subscriberId)) {
                return true;
            }
            waiting.add(new Waiting(subscriptionId, subscriberId));
            return false;
        }

        private synchronized boolean holds(String subscriptionId, String subscriberId) {
            Lease lease = leases.get(subscriptionId);
            return lease != null && lease.holder.equals(subscriberId) && lease.expiresAt > now;
        }

        private synchronized void giveUp(String subscriptionId, String subscriberId) {
            Lease lease = leases.get(subscriptionId);
            if (lease != null && lease.holder.equals(subscriberId)) {
                leases.remove(subscriptionId);
            }
            waiting.removeIf(candidate -> candidate.subscriptionId.equals(subscriptionId) && candidate.subscriberId.equals(subscriberId));
        }

        private static final class Lease {
            private final String holder;
            private long expiresAt;

            private Lease(String holder, long expiresAt) {
                this.holder = holder;
                this.expiresAt = expiresAt;
            }
        }

        private record Waiting(String subscriptionId, String subscriberId) {
        }

        // What one node's model talks to. Its shutdown() stops the refresh of the leases of the node.
        private final class NodeStrategy implements CompetingConsumerStrategy {
            private final String subscriberId;
            private volatile @Nullable CompetingConsumerListener listener;
            private volatile boolean shutDown;
            private volatile boolean stalled;

            private NodeStrategy(String subscriberId) {
                this.subscriberId = subscriberId;
            }

            boolean refreshes() {
                return !shutDown && !stalled;
            }

            void granted(String subscriptionId) {
                CompetingConsumerListener told = listener;
                if (told != null) {
                    told.onConsumeGranted(subscriptionId, subscriberId);
                }
            }

            @Override
            public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
                return register(subscriptionId, subscriberId);
            }

            @Override
            public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
                giveUp(subscriptionId, subscriberId);
            }

            @Override
            public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
                giveUp(subscriptionId, subscriberId);
            }

            @Override
            public boolean hasLock(String subscriptionId, String subscriberId) {
                return holds(subscriptionId, subscriberId);
            }

            @Override
            public void addListener(CompetingConsumerListener listener) {
                this.listener = listener;
            }

            @Override
            public void removeListener(CompetingConsumerListener listener) {
                this.listener = null;
            }

            @Override
            public void shutdown() {
                shutDown = true;
            }
        }
    }
}
