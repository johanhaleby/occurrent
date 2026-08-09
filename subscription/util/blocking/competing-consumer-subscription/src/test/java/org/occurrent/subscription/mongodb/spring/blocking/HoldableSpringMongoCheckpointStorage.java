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

package org.occurrent.subscription.mongodb.spring.blocking;

import org.bson.Document;
import org.jspecify.annotations.NullMarked;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.springframework.data.mongodb.core.MongoOperations;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link SpringMongoCheckpointStorage} whose next conditional write can be held open until a test releases it,
 * through the package-private {@code persistConditionalCheckpointDocument} hook the production class already
 * exposes for exactly this kind of interception (see its javadoc: "so a test can inject a transient failure into
 * it"). Deliberately placed in {@code SpringMongoCheckpointStorage}'s own package, a split package that exists only
 * in test sources, rather than proxying the Mongo driver the way {@code MongoLeaseRaceTest} does. The write this
 * holds is one {@code findOneAndUpdate}, and holding at the method the production class already isolates for
 * testing is simpler than intercepting the driver call beneath it.
 * <p>
 * A stale node's checkpoint write and the node that took its lease over both reach this same document, and which
 * one lands first decides whether the stale write is accepted or refused (ADR 116 names this window explicitly).
 * {@link #armHold()} lets a test hold the stale node's write open until the rival's own write has landed, so an
 * end-to-end test can assert the fence's outcome on a real race instead of hoping for one interleaving over another.
 */
@NullMarked
public class HoldableSpringMongoCheckpointStorage extends SpringMongoCheckpointStorage {

    private static final java.time.Duration HOLD_TIMEOUT = java.time.Duration.ofSeconds(10);

    private final AtomicBoolean armed = new AtomicBoolean(false);
    private final CountDownLatch arrived = new CountDownLatch(1);
    private final CountDownLatch released = new CountDownLatch(1);

    public HoldableSpringMongoCheckpointStorage(MongoOperations mongoOperations, String checkpointCollection) {
        super(mongoOperations, checkpointCollection);
    }

    /**
     * The next conditional write this storage attempts blocks inside {@code persistConditionalCheckpointDocument}
     * until {@link #release()} is called. Single-shot, arm again only after releasing.
     */
    public void armHold() {
        if (!armed.compareAndSet(false, true)) {
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
     * Releases the write {@link #armHold()} is holding, letting it proceed against MongoDB.
     */
    public void release() {
        released.countDown();
    }

    @Override
    @NullMarked
    Document persistConditionalCheckpointDocument(String subscriptionId, Document newCheckpointDocument, CheckpointWriteCondition condition) {
        if (armed.compareAndSet(true, false)) {
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
