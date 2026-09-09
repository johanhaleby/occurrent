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

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StartPositionAlreadyPinnedException;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code recordFirstPositionOrRefuse} reads storage, finds nothing, then writes what the wrapped model answers for
 * {@code globalCheckpoint()}. A checkpoint that lands in storage between those two calls, from another node
 * registering the same subscription id concurrently, must not be silently overwritten by this node's write, the
 * same guarantee {@code ManualStartSubscriptionModel} and {@code ReactorDurableSubscriptionModel} already give.
 * <p>
 * A hand-rolled storage rather than MongoDB, because this is about the order two calls reach storage in, which a
 * real database hides.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelFirstPositionRaceTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";

    @Test
    void a_registration_that_loses_the_first_position_write_to_a_checkpoint_it_did_not_read_is_refused_rather_than_overwriting_it() {
        RaceSimulatingCheckpointStorage storage = new RaceSimulatingCheckpointStorage();
        // Another node's registration for the same subscription id wins the write in between this node's read
        // (which finds nothing) and its own write.
        storage.whenTheFirstReadFindsNothing = () -> storage.delegate.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("landed-during-registration"));
        InMemoryFeed feed = new InMemoryFeed();
        feed.answersCurrentPosition = true;
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage);

        assertThatThrownBy(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }))
                .as("the position stored is not the one this registration read, so starting from it would skip whatever the other node's own subscription is about to see")
                .isInstanceOf(StartPositionAlreadyPinnedException.class);

        assertThat(storage.delegate.read(SUBSCRIPTION_ID).asString())
                .as("the other node's checkpoint must survive this node's losing write untouched")
                .isEqualTo("landed-during-registration");
    }

    @Test
    void a_registration_that_loses_the_write_from_inside_the_dynamic_supplier_adopts_the_winning_position_instead_of_throwing_from_inside_it() {
        // startWhenNoStartPositionCanBeRecorded(true) plus a feed that answers null for globalCheckpoint() only on
        // the eager, outside-the-supplier call lets recordFirstPositionOrRefuse return null without ever writing,
        // so the dynamic supplier's own retry-path branch runs on this registration's very first (and only)
        // evaluation, the same branch a wrapped model's own retry loop could otherwise evaluate more than once.
        RaceSimulatingCheckpointStorage storage = new RaceSimulatingCheckpointStorage();
        storage.whenTheSecondReadFindsNothing = () -> storage.delegate.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("landed-during-registration"));
        InMemoryFeed feed = new InMemoryFeed();
        feed.answersCurrentPosition = true;
        feed.answersNullOnFirstCallOnly = true;
        DurableSubscriptionModel durable = new DurableSubscriptionModel(
                feed, storage, new DurableSubscriptionModelConfig(1).startWhenNoStartPositionCanBeRecorded(true));

        // Must not throw. A StartPositionAlreadyPinnedException surfacing from inside StartAt.dynamic's supplier
        // reaches the wrapped model's own evaluation path instead of this call, which a retry wrapper could catch
        // and re-evaluate forever, telling nobody, exactly what recordFirstPositionOrRefuse's own placement
        // outside the supplier exists to avoid for the eager path.
        assertThatCode(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        })).doesNotThrowAnyException();

        assertThat(storage.delegate.read(SUBSCRIPTION_ID).asString())
                .as("the other node's already-stored position is adopted rather than lost or refused")
                .isEqualTo("landed-during-registration");
    }

    @Test
    void when_the_confirm_read_itself_finds_nothing_the_dynamic_supplier_falls_back_to_the_computed_checkpoint_never_to_the_model_default() {
        // A storage that always refuses ifAbsent() and always reads back empty, standing in for the doubly rare
        // case where the confirm-read behind a lost race finds nothing (the racing checkpoint deleted between the
        // failed write and the read that would have named it). globalCheckpoint is the position this node itself
        // computed and would have started from had the race gone the other way, so falling back to it risks a
        // duplicate delivery against whatever the other node's write actually holds, never a loss, unlike falling
        // through to the caller's model-default fallback, which would skip everything between here and now.
        AlwaysConflictingCheckpointStorage storage = new AlwaysConflictingCheckpointStorage();
        InMemoryFeed feed = new InMemoryFeed();
        feed.answersCurrentPosition = true;
        feed.answersNullOnFirstCallOnly = true;
        DurableSubscriptionModel durable = new DurableSubscriptionModel(
                feed, storage, new DurableSubscriptionModelConfig(1).startWhenNoStartPositionCanBeRecorded(true));

        assertThatCode(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        })).doesNotThrowAnyException();

        assertThat(feed.lastResolvedStartAt)
                .as("the computed checkpoint, not the model-default StartAt this feed answers 'now' for")
                .isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class,
                        checkpoint -> assertThat(checkpoint.checkpoint.asString()).isEqualTo("this-nodes-own-position"));
    }

    private static CloudEvent cloudEvent(String id) {
        return io.cloudevents.core.builder.CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("test.event").build();
    }

    /**
     * Delegates every call to a real {@link InMemoryCheckpointStorage}, except that the first {@link #read} for a
     * subscription id runs {@link #whenTheFirstReadFindsNothing} right after finding nothing, before this node's
     * own write reaches storage. That reproduces the interleaving a real race needs without a second thread.
     */
    private static final class RaceSimulatingCheckpointStorage implements CheckpointStorage {
        final InMemoryCheckpointStorage delegate = new InMemoryCheckpointStorage();
        @Nullable Runnable whenTheFirstReadFindsNothing;
        @Nullable Runnable whenTheSecondReadFindsNothing;
        private int nullReadCount = 0;

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            Checkpoint found = delegate.read(subscriptionId);
            if (found == null) {
                nullReadCount++;
                if (nullReadCount == 1 && whenTheFirstReadFindsNothing != null) {
                    Runnable hook = whenTheFirstReadFindsNothing;
                    whenTheFirstReadFindsNothing = null;
                    hook.run();
                } else if (nullReadCount == 2 && whenTheSecondReadFindsNothing != null) {
                    Runnable hook = whenTheSecondReadFindsNothing;
                    whenTheSecondReadFindsNothing = null;
                    hook.run();
                }
            }
            return found;
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, org.occurrent.subscription.CheckpointWriteCondition condition) {
            return delegate.save(subscriptionId, checkpoint, condition);
        }

        @Override
        public boolean evaluatesWriteConditions() {
            return delegate.evaluatesWriteConditions();
        }

        @Override
        public java.util.OptionalLong writeVersion(String subscriptionId) {
            return delegate.writeVersion(subscriptionId);
        }

        @Override
        public void delete(String subscriptionId) {
            delegate.delete(subscriptionId);
        }

        @Override
        public boolean exists(String subscriptionId) {
            return delegate.exists(subscriptionId);
        }
    }

    /**
     * Always refuses {@code ifAbsent()} and always reads back empty, standing in for a storage where the racing
     * checkpoint behind a lost write is gone again by the time the confirm-read looks for it.
     */
    private static final class AlwaysConflictingCheckpointStorage implements CheckpointStorage {
        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            return null;
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, org.occurrent.subscription.CheckpointWriteCondition condition) {
            if (condition instanceof org.occurrent.subscription.CheckpointWriteCondition.IfAbsent) {
                throw new org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException(subscriptionId, java.util.OptionalLong.empty(), condition);
            }
            return checkpoint;
        }

        @Override
        public boolean evaluatesWriteConditions() {
            return true;
        }

        @Override
        public java.util.OptionalLong writeVersion(String subscriptionId) {
            return java.util.OptionalLong.empty();
        }

        @Override
        public void delete(String subscriptionId) {
        }

        @Override
        public boolean exists(String subscriptionId) {
            return false;
        }
    }

    /**
     * A feed with change-stream mechanics reduced to what this test needs: the model default means the end of what
     * has been published so far, and a subscription is registered but never delivered to.
     */
    private static final class InMemoryFeed implements CheckpointAwareSubscriptionModel {
        final Map<String, Boolean> subscriptions = new LinkedHashMap<>();
        boolean answersCurrentPosition = false;
        // Set only by the test that needs the eager, outside-the-supplier globalCheckpoint() call to answer
        // unanswerable, so recordFirstPositionOrRefuse returns null without writing anything and the dynamic
        // supplier's own retry-path branch is what asks again.
        boolean answersNullOnFirstCallOnly = false;
        private int globalCheckpointCalls = 0;
        @Nullable StartAt lastResolvedStartAt;

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            if (startAt.isDynamic()) {
                lastResolvedStartAt = startAt.get(new SubscriptionModelContext(InMemoryFeed.class));
            }
            subscriptions.put(subscriptionId, true);
            return dummySubscription(subscriptionId);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            globalCheckpointCalls++;
            if (answersNullOnFirstCallOnly && globalCheckpointCalls == 1) {
                return null;
            }
            return answersCurrentPosition ? new StringBasedCheckpoint("this-nodes-own-position") : null;
        }

        @Override
        public void shutdown() {
            subscriptions.clear();
        }

        @Override
        public void stop() {
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return subscriptions.containsKey(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return dummySubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            subscriptions.remove(subscriptionId);
        }

        private static Subscription dummySubscription(String subscriptionId) {
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
}
