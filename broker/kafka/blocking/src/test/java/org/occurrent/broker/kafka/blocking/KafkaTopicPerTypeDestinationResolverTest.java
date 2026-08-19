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
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.condition.Condition;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.SubscriptionFilter;

import java.net.URI;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaTopicPerTypeDestinationResolverTest {

    private final CloudEventTypeMapper<TestEvent> typeMapper = ReflectionCloudEventTypeMapper.qualified();
    private final KafkaTopicPerTypeDestinationResolver resolver = new KafkaTopicPerTypeDestinationResolver("my-topic-", typeMapper);

    private final String eventAType = EventA.class.getName();
    private final String eventBType = EventB.class.getName();

    @Test
    void destinationFor_derives_the_topic_from_the_prefix_and_the_cloud_event_type_through_the_type_mapper() {
        CloudEvent cloudEvent = cloudEventOfType(eventAType, null);

        KafkaDestination destination = resolver.destinationFor(cloudEvent);

        assertThat(destination.topic()).isEqualTo("my-topic-" + eventAType);
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
    void destinationFor_throws_whatever_the_type_mapper_throws_for_an_unrecognised_type() {
        CloudEvent cloudEvent = cloudEventOfType("not.a.real.Class", null);

        assertThatThrownBy(() -> resolver.destinationFor(cloudEvent)).isInstanceOf(RuntimeException.class);
    }

    @Test
    void catchAllDestination_returns_a_topic_pattern_covering_the_prefix() {
        KafkaDestination destination = resolver.catchAllDestination();

        assertThat(destination.topic()).isEqualTo("\\Qmy-topic-\\E.*");
        assertThat(destination.key()).isNull();
        assertThat(destination.headers()).isEmpty();
    }

    @Test
    void destinationsFor_an_equality_type_filter_resolves_to_a_single_destination() {
        SubscriptionFilter filter = AgnosticSubscriptionFilter.filter(Filter.type(eventAType));

        Optional<Set<KafkaDestination>> destinations = resolver.destinationsFor(filter);

        assertThat(destinations).contains(Set.of(KafkaDestination.of("my-topic-" + eventAType)));
    }

    @Test
    void destinationsFor_an_in_condition_type_filter_resolves_to_one_destination_per_value() {
        SubscriptionFilter filter = AgnosticSubscriptionFilter.filter(Filter.type(Condition.in(eventAType, eventBType)));

        Optional<Set<KafkaDestination>> destinations = resolver.destinationsFor(filter);

        assertThat(destinations).contains(Set.of(
                KafkaDestination.of("my-topic-" + eventAType),
                KafkaDestination.of("my-topic-" + eventBType)));
    }

    @Test
    void destinationsFor_an_or_of_two_type_filters_unions_the_destinations() {
        SubscriptionFilter filter = AgnosticSubscriptionFilter.filter(Filter.type(eventAType).or(Filter.type(eventBType)));

        Optional<Set<KafkaDestination>> destinations = resolver.destinationsFor(filter);

        assertThat(destinations).contains(Set.of(
                KafkaDestination.of("my-topic-" + eventAType),
                KafkaDestination.of("my-topic-" + eventBType)));
    }

    @Test
    void destinationsFor_an_or_with_one_unconstrained_branch_cannot_narrow() {
        SubscriptionFilter filter = AgnosticSubscriptionFilter.filter(Filter.type(eventAType).or(Filter.streamId("some-stream")));

        Optional<Set<KafkaDestination>> destinations = resolver.destinationsFor(filter);

        assertThat(destinations).isEmpty();
    }

    @Test
    void destinationsFor_an_and_narrows_to_the_type_conjunct_even_though_the_other_conjunct_is_not_a_type_filter() {
        SubscriptionFilter filter = AgnosticSubscriptionFilter.filter(Filter.type(eventAType).and(Filter.streamId("some-stream")));

        Optional<Set<KafkaDestination>> destinations = resolver.destinationsFor(filter);

        assertThat(destinations).contains(Set.of(KafkaDestination.of("my-topic-" + eventAType)));
    }

    @Test
    void destinationsFor_a_filter_on_an_unrelated_field_cannot_narrow() {
        SubscriptionFilter filter = AgnosticSubscriptionFilter.filter(Filter.subject("some-subject"));

        Optional<Set<KafkaDestination>> destinations = resolver.destinationsFor(filter);

        assertThat(destinations).isEmpty();
    }

    @Test
    void destinationsFor_a_subscription_filter_this_resolver_does_not_understand_cannot_narrow() {
        SubscriptionFilter filter = new SubscriptionFilter() {
        };

        Optional<Set<KafkaDestination>> destinations = resolver.destinationsFor(filter);

        assertThat(destinations).isEmpty();
    }

    /**
     * The case Kafka's own topic naming rule exists to catch: {@link Class#getName()} writes a nested class's
     * enclosing class separator as {@code $}, which {@code [a-zA-Z0-9._-]} does not allow.
     */
    @Test
    void destinationFor_refuses_a_type_that_resolves_to_an_illegal_kafka_topic_name() {
        CloudEventTypeMapper<NestedEvent> nestedTypeMapper = ReflectionCloudEventTypeMapper.qualified();
        KafkaTopicPerTypeDestinationResolver nestedResolver = new KafkaTopicPerTypeDestinationResolver("my-topic-", nestedTypeMapper);
        CloudEvent cloudEvent = cloudEventOfType(NestedEvent.class.getName(), null);

        assertThatThrownBy(() -> nestedResolver.destinationFor(cloudEvent))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("$")
                .hasMessageContaining(NestedEvent.class.getName());
    }

    private CloudEvent cloudEventOfType(String type, String streamId) {
        CloudEventBuilder builder = CloudEventBuilder.v1().withId("id").withSource(URI.create("urn:test")).withType(type);
        if (streamId != null) {
            builder.withExtension("streamid", streamId);
        }
        return builder.build();
    }

    /**
     * Nested only for this one negative test, since a nested class's qualified name is exactly what
     * {@code destinationFor_refuses_a_type_that_resolves_to_an_illegal_kafka_topic_name} needs to be illegal.
     * {@link EventA} and {@link EventB} above are top-level instead, precisely so the rest of this file's fixtures
     * stay legal Kafka topic names.
     */
    private static final class NestedEvent {
    }
}

interface TestEvent {
}

final class EventA implements TestEvent {
}

final class EventB implements TestEvent {
}
