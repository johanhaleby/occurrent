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

package org.occurrent.subscription.push.blocking;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.util.List;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A configured {@link org.occurrent.subscription.api.blocking.CheckpointWriteVersionSource} stamps the one-shot
 * catch-up marker {@link CatchupThenPushSubscriptionModel} writes with {@code notOlderThan(version)}. No source, or
 * a source answering empty, leaves the write {@code any()}, exactly the behaviour before ADR 116. Run over
 * {@link InMemoryCheckpointStorage}, which evaluates a {@code CheckpointWriteCondition} for real rather than
 * refusing it, so the stored version proves which condition the model actually stamped the write with.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupThenPushSubscriptionModelCheckpointWriteVersionSourceTest {

    private static final String SUBSCRIPTION_ID = "proj";

    @Test
    void a_configured_source_stamps_the_marker_write_not_older_than_the_version_it_answers() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        InMemoryEventStore store = new InMemoryEventStore(feed::accept);
        store.write("s1", List.of(cloudEvent("1", "Created")));
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();

        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, marker, id -> OptionalLong.of(5));
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), ce -> {
        }).waitUntilStarted();

        assertThat(marker.writeVersion(SUBSCRIPTION_ID)).hasValue(5L);
    }

    @Test
    void a_source_answering_empty_leaves_the_marker_write_any_the_same_as_no_source() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        InMemoryEventStore store = new InMemoryEventStore(feed::accept);
        store.write("s1", List.of(cloudEvent("1", "Created")));
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();

        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, marker, id -> OptionalLong.empty());
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), ce -> {
        }).waitUntilStarted();

        assertThat(marker.exists(SUBSCRIPTION_ID)).isTrue();
        assertThat(marker.writeVersion(SUBSCRIPTION_ID)).isEmpty();
    }

    @Test
    void no_source_configured_leaves_the_marker_write_any_unchanged_from_before_adr_116() {
        PushSubscriptionModel feed = new PushSubscriptionModel();
        InMemoryEventStore store = new InMemoryEventStore(feed::accept);
        store.write("s1", List.of(cloudEvent("1", "Created")));
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();

        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, marker);
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), ce -> {
        }).waitUntilStarted();

        assertThat(marker.exists(SUBSCRIPTION_ID)).isTrue();
        assertThat(marker.writeVersion(SUBSCRIPTION_ID)).isEmpty();
    }

    private static CloudEvent cloudEvent(String id, String type) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:occurrent:test"))
                .withType(type)
                .build();
    }
}
