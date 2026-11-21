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

package org.occurrent.subscription.blocking.durable;

import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link QuietPositionReportingModel} that can be paused and resumed, at a position or from its own, and that
 * restarts at any checkpoint, or, from {@link #continuingFromItsOwnPositionForAGlobalCheckpoint()}, at any checkpoint
 * but a {@link GlobalCheckpoint}. A resume at a {@code GlobalCheckpoint} then goes on from its own position and
 * delivers nothing again, as a MongoDB model does.
 * A pause returns at once, as the pause of a wrapped model does that stops waiting for a handler that is still running.
 */
final class RepositionableQuietPositionReportingModel extends QuietPositionReportingModel implements RepositionableSubscriptions {
    private final Set<String> paused = ConcurrentHashMap.newKeySet();
    // Where each resume at a position resumed the subscription, in the order they came
    final List<Checkpoint> resumedAt = new CopyOnWriteArrayList<>();
    final AtomicInteger plainResumes = new AtomicInteger();
    private final boolean continuesFromItsOwnPositionForAGlobalCheckpoint;

    RepositionableQuietPositionReportingModel() {
        this(false);
    }

    private RepositionableQuietPositionReportingModel(boolean continuesFromItsOwnPositionForAGlobalCheckpoint) {
        this.continuesFromItsOwnPositionForAGlobalCheckpoint = continuesFromItsOwnPositionForAGlobalCheckpoint;
    }

    static RepositionableQuietPositionReportingModel continuingFromItsOwnPositionForAGlobalCheckpoint() {
        return new RepositionableQuietPositionReportingModel(true);
    }

    @Override
    public void pauseSubscription(String subscriptionId) {
        paused.add(subscriptionId);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return paused.contains(subscriptionId);
    }

    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        paused.remove(subscriptionId);
        plainResumes.incrementAndGet();
        return subscription(subscriptionId);
    }

    @Override
    public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
        paused.remove(subscriptionId);
        resumedAt.add(((StartAt.StartAtCheckpoint) startAt).checkpoint);
        return subscription(subscriptionId);
    }

    @Override
    public boolean restartsAt(Checkpoint checkpoint) {
        return !continuesFromItsOwnPositionForAGlobalCheckpoint || !isGlobal(checkpoint);
    }

    private static boolean isGlobal(Checkpoint checkpoint) {
        try {
            GlobalCheckpoint.parse(checkpoint);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    private static Subscription subscription(String subscriptionId) {
        return new Subscription() {
            @Override
            public String id() {
                return subscriptionId;
            }

            @Override
            public boolean waitUntilStarted(Duration timeout) {
                return true;
            }
        };
    }
}
