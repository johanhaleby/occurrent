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

package org.occurrent.broker.kafka.blocking;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.SubscriptionFilter;

import java.net.URI;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaSharedTopicDestinationResolverTest {

    private final KafkaSharedTopicDestinationResolver resolver = new KafkaSharedTopicDestinationResolver("my-topic");

    private final String eventAType = EventA.class.getName();

    @Test
    void constructor_refuses_a_null_topic() {
        assertThatThrownBy(() -> new KafkaSharedTopicDestinationResolver(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void constructor_refuses_a_topic_name_kafka_itself_would_reject() {
        assertThatThrownBy(() -> new KafkaSharedTopicDestinationResolver("not a legal topic!"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not a legal topic!");
    }

    @Test
    void constructor_refuses_an_empty_topic_name() {
        assertThatThrownBy(() -> new KafkaSharedTopicDestinationResolver(""))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void destinationFor_always_derives_the_one_configured_topic() {
        CloudEvent cloudEvent = cloudEventOfType(eventAType, null);

        KafkaDestination destination = resolver.destinationFor(cloudEvent);

        assertThat(destination.topic()).isEqualTo("my-topic");
        assertThat(destination.headers()).isEmpty();
    }

    @Test
    void destinationFor_keys_by_the_streamid_extension_when_present() {
        CloudEvent cloudEvent = cloudEventOfType(eventAType, "stream-1");

        KafkaDestination destination = resolver.destinationFor(cloudEvent);

        assertThat(destination.key()).isEqualTo("stream-1");
    }

    @Test
    void destinationFor_leaves_the_key_null_when_the_event_has_no_streamid_extension() {
        CloudEvent cloudEvent = cloudEventOfType(eventAType, null);

        KafkaDestination destination = resolver.destinationFor(cloudEvent);

        assertThat(destination.key()).isNull();
    }

    @Test
    void catchAllDestination_returns_the_one_configured_topic() {
        KafkaDestination destination = resolver.catchAllDestination();

        assertThat(destination.topic()).isEqualTo("my-topic");
        assertThat(destination.key()).isNull();
        assertThat(destination.headers()).isEmpty();
        assertThat(destination.topicIsPattern()).isFalse();
    }

    @Test
    void destinationsFor_a_narrowing_filter_still_returns_the_one_configured_topic() {
        SubscriptionFilter filter = AgnosticSubscriptionFilter.filter(Filter.type(eventAType));

        Optional<Set<KafkaDestination>> destinations = resolver.destinationsFor(filter);

        assertThat(destinations).contains(Set.of(KafkaDestination.of("my-topic")));
    }

    @Test
    void destinationsFor_a_non_narrowing_filter_also_returns_the_one_configured_topic() {
        SubscriptionFilter filter = AgnosticSubscriptionFilter.filter(Filter.subject("some-subject"));

        Optional<Set<KafkaDestination>> destinations = resolver.destinationsFor(filter);

        assertThat(destinations).contains(Set.of(KafkaDestination.of("my-topic")));
    }

    @Test
    void destinationsFor_a_subscription_filter_this_resolver_does_not_understand_still_returns_the_one_configured_topic() {
        SubscriptionFilter filter = new SubscriptionFilter() {
        };

        Optional<Set<KafkaDestination>> destinations = resolver.destinationsFor(filter);

        assertThat(destinations).contains(Set.of(KafkaDestination.of("my-topic")));
    }

    private CloudEvent cloudEventOfType(String type, String streamId) {
        CloudEventBuilder builder = CloudEventBuilder.v1().withId("id").withSource(URI.create("urn:test")).withType(type);
        if (streamId != null) {
            builder.withExtension("streamid", streamId);
        }
        return builder.build();
    }
}
