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

package org.occurrent.subscription.mongodb.nativedriver.blocking;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.jspecify.annotations.NullMarked;
import org.occurrent.subscription.CheckpointWriteCondition;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link NativeMongoCheckpointStorage} whose next conditional write for one specific subscription id can be held
 * open until a test releases it, through the package-private {@code persistConditionalCheckpointDocument} hook the
 * production class already exposes for exactly this kind of interception. See
 * {@code org.occurrent.subscription.mongodb.spring.blocking.HoldableSpringMongoCheckpointStorage}, its Spring
 * twin, for the full rationale. This one filters by subscription id because one {@code CompetingConsumerStrategy}
 * node here serves two subscriptions through a single shared storage, and holding indiscriminately would also hold
 * the healthy subscription's own, unrelated writes.
 */
@NullMarked
public class HoldableNativeMongoCheckpointStorage extends NativeMongoCheckpointStorage {

    private static final java.time.Duration HOLD_TIMEOUT = java.time.Duration.ofSeconds(10);

    private final AtomicReference<@org.jspecify.annotations.Nullable String> armedForSubscriptionId = new AtomicReference<>();
    private final AtomicBoolean triggered = new AtomicBoolean(false);
    private final CountDownLatch arrived = new CountDownLatch(1);
    private final CountDownLatch released = new CountDownLatch(1);

    public HoldableNativeMongoCheckpointStorage(MongoCollection<Document> checkpointCollection) {
        super(checkpointCollection);
    }

    /**
     * The next conditional write this storage attempts for {@code subscriptionId} blocks inside
     * {@code persistConditionalCheckpointDocument} until {@link #release()} is called. Every other subscription id's
     * write passes straight through, unheld. Single-shot, arm again only after releasing.
     */
    public void armHold(String subscriptionId) {
        if (!armedForSubscriptionId.compareAndSet(null, subscriptionId)) {
            throw new IllegalStateException("Already armed, this storage holds only one write at a time");
        }
    }

    /**
     * Blocks the calling thread until the held write has arrived at the hold point, so a test can be sure a rival's
     * own write is free to land first.
     */
    public void awaitHeldWriteArrived() {
        awaitUninterruptibly(arrived, "The held write never arrived");
    }

    /**
     * Releases the write {@link #armHold(String)} is holding, letting it proceed against MongoDB.
     */
    public void release() {
        released.countDown();
    }

    @Override
    Document persistConditionalCheckpointDocument(String subscriptionId, Document newCheckpointDocument, CheckpointWriteCondition condition) {
        if (subscriptionId.equals(armedForSubscriptionId.get()) && triggered.compareAndSet(false, true)) {
            arrived.countDown();
            awaitUninterruptibly(released, "Held write was never released");
        }
        return super.persistConditionalCheckpointDocument(subscriptionId, newCheckpointDocument, condition);
    }

    private static void awaitUninterruptibly(CountDownLatch latch, String timeoutMessage) {
        try {
            if (!latch.await(HOLD_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException(timeoutMessage + " within " + HOLD_TIMEOUT);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
