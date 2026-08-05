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

package org.occurrent.tck.subscription.blocking;

import org.jspecify.annotations.NullMarked;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A strategy that honours the whole contract, so that every test in {@link CompetingConsumerStrategyConformance} can be
 * seen passing as well as failing.
 * <p>
 * Two mechanisms enforce the ban on skipping and neither subsumes the other, per ADR 94. Running the suite against
 * {@link NoopCompetingConsumerStrategy} shows every test asserts something, but every test dies on its first call there,
 * so nothing further down a test body is ever reached. Running it green against this reaches every line, so an
 * {@code Assumptions} call anywhere in the suite would show up as a skipped test.
 * <p>
 * <strong>It coordinates nothing like Occurrent's own strategies do, and that is the point.</strong> Occurrent's two
 * implementations keep a lease in MongoDB. This one keeps candidates in a shared map, elects the oldest live one, and
 * treats a candidate that has stopped heart-beating as gone. If a suite written against a lease could only be satisfied
 * by a lease, it would be a description of MongoDB rather than a contract, and a second implementation is the cheapest
 * way to find that out. It is a design of its own rather than a copy of a published class, so there is nothing here for
 * a production change to drift away from.
 */
@NullMarked
final class WorkingCompetingConsumerStrategy implements CompetingConsumerStrategy {

    /**
     * How often an instance stamps the candidates it still wants and re-elects. Short, because the suite waits on the
     * outcome and a fixture using this declares a {@code timeToConverge()} well above it.
     */
    private static final Duration ROUND = Duration.ofMillis(25);

    /**
     * How long a candidate survives without being stamped. Several rounds, so an instance that is merely busy does not
     * lose a lock a rival then has to hand back.
     */
    private static final Duration LIVENESS = Duration.ofMillis(250);

    /**
     * The shared storage two strategies contend over. What Occurrent's implementations put in a MongoDB collection.
     */
    static final class Storage {
        private final Map<String, List<Candidate>> candidatesPerSubscription = new ConcurrentHashMap<>();

        private synchronized void add(Candidate candidate) {
            List<Candidate> candidates = candidatesPerSubscription.computeIfAbsent(candidate.subscriptionId, __ -> new ArrayList<>());
            if (candidates.stream().noneMatch(candidate::isSameAs)) {
                candidates.add(candidate);
            }
        }

        private synchronized void remove(Candidate candidate) {
            candidatesPerSubscription.getOrDefault(candidate.subscriptionId, List.of())
                    .removeIf(candidate::isSameAs);
        }

        /**
         * Puts a candidate behind everybody already waiting, so releasing hands the lock over rather than handing it
         * straight back to whoever gave it up.
         */
        private synchronized void moveToBack(Candidate candidate) {
            List<Candidate> candidates = candidatesPerSubscription.getOrDefault(candidate.subscriptionId, List.of());
            if (candidates.removeIf(candidate::isSameAs)) {
                candidates.add(candidate);
            }
        }

        /**
         * Who holds the lock for a subscription: the longest-standing candidate that is still alive and not yielding.
         */
        private synchronized boolean holds(Candidate candidate, long now) {
            return candidatesPerSubscription.getOrDefault(candidate.subscriptionId, List.of()).stream()
                    .filter(other -> other.isEligible(now))
                    .findFirst()
                    .filter(candidate::isSameAs)
                    .isPresent();
        }
    }

    private static final class Candidate {
        private final String instanceId;
        private final String subscriptionId;
        private final String subscriberId;
        private volatile long lastSeen;
        private volatile long yieldingUntil;

        private Candidate(String instanceId, String subscriptionId, String subscriberId, long now) {
            this.instanceId = instanceId;
            this.subscriptionId = subscriptionId;
            this.subscriberId = subscriberId;
            this.lastSeen = now;
            this.yieldingUntil = now;
        }

        private boolean isSameAs(Candidate other) {
            return instanceId.equals(other.instanceId) && subscriptionId.equals(other.subscriptionId)
                    && subscriberId.equals(other.subscriberId);
        }

        private boolean isEligible(long now) {
            return now - lastSeen < LIVENESS.toNanos() && now - yieldingUntil >= 0;
        }
    }

    private final String instanceId = UUID.randomUUID().toString();
    private final Storage storage;
    private final Map<String, Candidate> mine = new ConcurrentHashMap<>();
    private final Map<String, Boolean> lastReported = new ConcurrentHashMap<>();
    private final Set<CompetingConsumerListener> listeners = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final ScheduledExecutorService coordinator =
            Executors.newSingleThreadScheduledExecutor(runnable -> Thread.ofPlatform().daemon().unstarted(runnable));

    WorkingCompetingConsumerStrategy(Storage storage) {
        this.storage = storage;
        coordinator.scheduleWithFixedDelay(this::coordinate, ROUND.toMillis(), ROUND.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
        Candidate candidate = new Candidate(instanceId, subscriptionId, subscriberId, System.nanoTime());
        mine.put(key(subscriptionId, subscriberId), candidate);
        // A consumer that never had the lock has not been prohibited from anything, so seeding what the listeners were
        // last told keeps a registration that lost from reporting a change that did not happen.
        lastReported.putIfAbsent(key(subscriptionId, subscriberId), false);
        storage.add(candidate);
        return reportWhetherItHoldsTheLock(candidate);
    }

    @Override
    public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
        Candidate candidate = mine.remove(key(subscriptionId, subscriberId));
        if (candidate != null) {
            storage.remove(candidate);
            reportWhetherItHoldsTheLock(candidate);
            lastReported.remove(key(subscriptionId, subscriberId));
        }
    }

    @Override
    public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
        Candidate candidate = mine.get(key(subscriptionId, subscriberId));
        if (candidate != null) {
            // Still a candidate, but behind everybody already waiting and standing back for a round even when nobody
            // is. That is what makes releasing weaker than unregistering: it hands the lock over when there is a rival
            // and takes it back on its own when there is not, with nobody registering it again.
            candidate.yieldingUntil = System.nanoTime() + ROUND.toNanos();
            storage.moveToBack(candidate);
            reportWhetherItHoldsTheLock(candidate);
        }
    }

    @Override
    public boolean hasLock(String subscriptionId, String subscriberId) {
        Candidate candidate = mine.get(key(subscriptionId, subscriberId));
        return candidate != null && storage.holds(candidate, System.nanoTime());
    }

    @Override
    public void addListener(CompetingConsumerListener listenerConsumer) {
        listeners.add(listenerConsumer);
    }

    @Override
    public void removeListener(CompetingConsumerListener listenerConsumer) {
        listeners.remove(listenerConsumer);
    }

    /**
     * Stops coordinating. Nothing is removed from the shared storage, so this instance's candidates go stale and rivals
     * elect somebody else, which is what a crashed process looks like from the outside.
     */
    @Override
    public void shutdown() {
        coordinator.shutdownNow();
    }

    private void coordinate() {
        long now = System.nanoTime();
        mine.values().forEach(candidate -> {
            candidate.lastSeen = now;
            reportWhetherItHoldsTheLock(candidate);
        });
    }

    /**
     * Tells the listeners whenever the answer for a candidate is not the one they were last told, and never otherwise.
     */
    private boolean reportWhetherItHoldsTheLock(Candidate candidate) {
        boolean holdsIt = storage.holds(candidate, System.nanoTime());
        Boolean reported = lastReported.put(key(candidate.subscriptionId, candidate.subscriberId), holdsIt);
        if (reported == null || reported != holdsIt) {
            listeners.forEach(listener -> {
                if (holdsIt) {
                    listener.onConsumeGranted(candidate.subscriptionId, candidate.subscriberId);
                } else {
                    listener.onConsumeProhibited(candidate.subscriptionId, candidate.subscriberId);
                }
            });
        }
        return holdsIt;
    }

    private static String key(String subscriptionId, String subscriberId) {
        return subscriptionId + " " + subscriberId;
    }
}
