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
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionHandle;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.time.Duration;
import java.util.OptionalLong;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A configured {@link org.occurrent.subscription.api.blocking.CheckpointWriteVersionSource} stamps every checkpoint
 * {@link DurableSubscriptionModel} writes with {@code notOlderThan(version)}. No source, or a source answering
 * empty, leaves every write {@code any()}, exactly the behaviour before ADR 116. Run over
 * {@link InMemoryCheckpointStorage}, which evaluates a {@code CheckpointWriteCondition} for real rather than
 * refusing it, so the stored version proves which condition the model actually stamped the write with.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelCheckpointWriteVersionSourceTest {

    private static final String SUBSCRIPTION_ID = "subscription";

    @Test
    void a_configured_source_stamps_the_checkpoint_write_not_older_than_the_version_it_answers() {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel model = new DurableSubscriptionModel(oneEventSubscriptionModel(), storage, id -> OptionalLong.of(7));

        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), event -> {
        }).waitUntilStarted();

        assertThat(storage.writeVersion(SUBSCRIPTION_ID)).hasValue(7L);
    }

    @Test
    void a_source_answering_empty_leaves_the_write_any_the_same_as_no_source() {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel model = new DurableSubscriptionModel(oneEventSubscriptionModel(), storage, id -> OptionalLong.empty());

        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), event -> {
        }).waitUntilStarted();

        assertThat(storage.exists(SUBSCRIPTION_ID)).isTrue();
        assertThat(storage.writeVersion(SUBSCRIPTION_ID)).isEmpty();
    }

    @Test
    void no_source_configured_leaves_the_write_any_unchanged_from_before_adr_116() {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel model = new DurableSubscriptionModel(oneEventSubscriptionModel(), storage);

        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), event -> {
        }).waitUntilStarted();

        assertThat(storage.exists(SUBSCRIPTION_ID)).isTrue();
        assertThat(storage.writeVersion(SUBSCRIPTION_ID)).isEmpty();
    }

    /**
     * Delivers a single checkpoint-carrying event synchronously on {@code subscribe} and answers a global checkpoint,
     * which is all {@link DurableSubscriptionModel} needs from the wrapped model to exercise both checkpoint writes
     * this test cares about.
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
}
