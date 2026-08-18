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

package org.occurrent.broker.api.blocking;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.api.EventStoreCloudEventExtensions;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DomainEventForwarderTest {

    private final FakeCheckpointAwareSubscriptionModel wrappedModel = new FakeCheckpointAwareSubscriptionModel();
    private final InMemoryCheckpointStorage checkpointStorage = new InMemoryCheckpointStorage();
    private final DurableSubscriptionModel subscriptionModel = new DurableSubscriptionModel(wrappedModel, checkpointStorage);
    private final FakeDomainEventSink<String> sink = new FakeDomainEventSink<>();
    private final DomainEventForwarder<String> forwarder = new DomainEventForwarder<>(subscriptionModel, new FakeStringCloudEventConverter(), sink);

    @Test
    void decodes_the_event_once_and_publishes_the_domain_event_with_its_metadata() {
        forwarder.forward("subscription1");

        wrappedModel.publish(checkpointAwareEvent("hello", 1));

        assertThat(sink.published()).hasSize(1);
        FakeDomainEventSink.Published<String> published = sink.published().get(0);
        assertThat(published.domainEvent()).isEqualTo("hello");
        assertThat(published.metadata().getStreamId()).isEqualTo("stream1");
        assertThat(published.metadata().getStreamVersion()).isEqualTo(1);
        assertThat(published.metadata().getPosition()).isEqualTo(1L);
        assertThat(published.metadata().<String>get(EventStoreCloudEventExtensions.DCB_TAGS)).isEqualTo("tag1\ntag2");
    }

    @Test
    void advances_the_checkpoint_once_the_sink_returns() {
        forwarder.forward("subscription1");

        wrappedModel.publish(checkpointAwareEvent("hello", 1));

        assertThat(checkpointStorage.read("subscription1")).isEqualTo(GlobalCheckpoint.of(1));
    }

    @Test
    void leaves_the_checkpoint_unmoved_and_propagates_the_failure_when_the_sink_throws() {
        forwarder.forward("subscription1");
        sink.failOnNextPublish();

        assertThatThrownBy(() -> wrappedModel.publish(checkpointAwareEvent("hello", 1)))
                .hasMessage("Simulated publish failure");

        assertThat(checkpointStorage.read("subscription1")).isNull();
    }

    @Test
    void passes_the_subscription_id_filter_and_start_position_through_to_the_wrapped_subscription_model() {
        SubscriptionFilter filter = new SubscriptionFilter() {
        };
        StartAt startAt = StartAt.now();

        forwarder.forward("subscription1", filter, startAt);

        assertThat(wrappedModel.lastSubscriptionId()).isEqualTo("subscription1");
        assertThat(wrappedModel.lastFilter()).isSameAs(filter);
        assertThat(wrappedModel.lastStartAt()).isSameAs(startAt);
    }

    private static CloudEvent checkpointAwareEvent(String data, long position) {
        CloudEvent cloudEvent = CloudEventBuilder.v1()
                .withId("event-" + position)
                .withSource(URI.create("urn:occurrent:test"))
                .withType("SomethingHappened")
                .withData(data.getBytes(StandardCharsets.UTF_8))
                .withExtension(OccurrentCloudEventExtension.occurrent("stream1", position))
                .withExtension(OccurrentCloudEventExtension.POSITION, position)
                .withExtension(EventStoreCloudEventExtensions.DCB_TAGS, "tag1\ntag2")
                .build();
        return new CheckpointAwareCloudEvent(cloudEvent, GlobalCheckpoint.of(position));
    }
}
