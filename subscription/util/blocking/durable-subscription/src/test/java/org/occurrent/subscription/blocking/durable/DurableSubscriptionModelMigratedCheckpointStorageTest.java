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
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.SubscriptionHandle;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A recipe-migrated {@code CheckpointStorage} that only answers {@code any()} is not a compile-time fiction, it is
 * the permanent shape doc/migration/upgrading-to-0.33.0.md documents for a store that cannot evaluate a condition,
 * and it is exactly what the rewrite module's {@code AddCheckpointStorageConditionalWriteStubs} generates. This
 * proves that shape actually works against {@link DurableSubscriptionModel}, which calls the three-argument
 * {@code save} directly (see {@code writeConditionFor}) and stamps it {@code any()} whenever no
 * {@code CheckpointWriteVersionSource} is configured. Before the fix, the generated stub threw for every condition,
 * {@code any()} included, so a class shaped exactly like {@link MigratedCheckpointStorage} below failed on its
 * first checkpoint write; the rewrite module's own recipe test only ever asserted the generated source, never that
 * it worked at runtime.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelMigratedCheckpointStorageTest {

    private static final String SUBSCRIPTION_ID = "subscription";

    @Test
    void a_migrated_storage_that_only_answers_any_accepts_the_write_durable_subscription_model_makes_with_no_write_version_source_configured() {
        MigratedCheckpointStorage storage = new MigratedCheckpointStorage();
        DurableSubscriptionModel model = new DurableSubscriptionModel(oneEventSubscriptionModel(), storage);

        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), event -> {
        }).waitUntilStarted();

        assertThat(storage.exists(SUBSCRIPTION_ID)).isTrue();
    }

    @Test
    void a_migrated_storage_that_only_answers_any_still_refuses_a_stronger_condition_a_configured_write_version_source_asks_for() {
        MigratedCheckpointStorage storage = new MigratedCheckpointStorage();
        DurableSubscriptionModel model = new DurableSubscriptionModel(oneEventSubscriptionModel(), storage, id -> OptionalLong.of(7));

        assertThatThrownBy(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), event -> {
        }).waitUntilStarted())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("only any() is supported");
    }

    /**
     * Delivers a single checkpoint-carrying event synchronously on {@code subscribe} and answers a global checkpoint,
     * which is all {@link DurableSubscriptionModel} needs from the wrapped model to exercise a checkpoint write.
     */
    private static CheckpointAwareSubscriptionModel oneEventSubscriptionModel() {
        return new CheckpointAwareSubscriptionModel() {
            @Override
            public SubscriptionHandle subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
                action.accept(checkpointAwareCloudEvent());
                return new SubscriptionHandle() {
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

            @Override
            public @Nullable Checkpoint globalCheckpoint() {
                return new StringBasedCheckpoint("global");
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
                return true;
            }

            @Override
            public boolean isPaused(String subscriptionId) {
                return false;
            }

            @Override
            public SubscriptionHandle resumeSubscription(String subscriptionId) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void pauseSubscription(String subscriptionId) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void cancelSubscription(String subscriptionId) {
            }
        };
    }

    private static CloudEvent checkpointAwareCloudEvent() {
        CloudEvent cloudEvent = CloudEventBuilder.v1()
                .withId("1")
                .withSource(URI.create("urn:occurrent:test"))
                .withType("Created")
                .build();
        return new CheckpointAwareCloudEvent(cloudEvent, new StringBasedCheckpoint("cp1"));
    }

    /**
     * Exactly the shape {@code AddCheckpointStorageConditionalWriteStubs} generates for a class whose 0.32.0
     * two-argument {@code save} is real, pre-existing behaviour: the three-argument overload delegates {@code any()}
     * to it and refuses anything stronger, {@code writeVersion} answers empty. See
     * doc/migration/upgrading-to-0.33.0.md section 2.
     */
    private static final class MigratedCheckpointStorage implements CheckpointStorage {

        private final Map<String, Checkpoint> checkpoints = new ConcurrentHashMap<>();

        @Override
        public Checkpoint read(String subscriptionId) {
            return checkpoints.get(subscriptionId);
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint) {
            checkpoints.put(subscriptionId, checkpoint);
            return checkpoint;
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            if (!(condition instanceof CheckpointWriteCondition.Any)) {
                throw new UnsupportedOperationException("This storage cannot evaluate " + condition + ", only any() is supported.");
            }
            return save(subscriptionId, checkpoint);
        }

        @Override
        public OptionalLong writeVersion(String subscriptionId) {
            return OptionalLong.empty();
        }

        @Override
        public void delete(String subscriptionId) {
            checkpoints.remove(subscriptionId);
        }

        @Override
        public boolean exists(String subscriptionId) {
            return checkpoints.containsKey(subscriptionId);
        }
    }
}
