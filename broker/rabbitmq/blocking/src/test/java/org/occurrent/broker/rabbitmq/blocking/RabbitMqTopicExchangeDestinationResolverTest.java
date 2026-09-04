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

package org.occurrent.broker.rabbitmq.blocking;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.condition.Condition;
import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.SubscriptionFilter;

import java.net.URI;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RabbitMqTopicExchangeDestinationResolverTest {

    private final CloudEventTypeMapper<TestEvent> typeMapper = ReflectionCloudEventTypeMapper.qualified();
    private final RabbitMqTopicExchangeDestinationResolver resolver = new RabbitMqTopicExchangeDestinationResolver("my-exchange", typeMapper);

    private final String eventAType = EventA.class.getName();
    private final String eventBType = EventB.class.getName();

    @Test
    void destinationFor_derives_the_routing_key_from_the_cloud_event_type_through_the_type_mapper() {
        CloudEvent cloudEvent = cloudEventOfType(eventAType);

        RabbitMqDestination destination = resolver.destinationFor(cloudEvent);

        assertThat(destination.exchange()).isEqualTo("my-exchange");
        assertThat(destination.routingKey()).isEqualTo(eventAType);
        assertThat(destination.headers()).isEmpty();
    }

    @Test
    void destinationFor_throws_whatever_the_type_mapper_throws_for_an_unrecognised_type() {
        CloudEvent cloudEvent = cloudEventOfType("not.a.real.Class");

        assertThatThrownBy(() -> resolver.destinationFor(cloudEvent)).isInstanceOf(RuntimeException.class);
    }

    @Test
    void catchAllDestination_binds_the_topic_exchange_wildcard() {
        RabbitMqDestination destination = resolver.catchAllDestination();

        assertThat(destination.exchange()).isEqualTo("my-exchange");
        assertThat(destination.routingKey()).isEqualTo("#");
    }

    @Test
    void destinationsFor_an_equality_type_filter_resolves_to_a_single_destination() {
        SubscriptionFilter filter = AgnosticSubscriptionFilter.filter(Filter.type(eventAType));

        Optional<Set<RabbitMqDestination>> destinations = resolver.destinationsFor(filter);

        assertThat(destinations).contains(Set.of(RabbitMqDestination.of("my-exchange", eventAType)));
    }

    @Test
    void destinationsFor_an_in_condition_type_filter_resolves_to_one_destination_per_value() {
        SubscriptionFilter filter = AgnosticSubscriptionFilter.filter(Filter.type(Condition.in(eventAType, eventBType)));

        Optional<Set<RabbitMqDestination>> destinations = resolver.destinationsFor(filter);

        assertThat(destinations).contains(Set.of(
                RabbitMqDestination.of("my-exchange", eventAType),
                RabbitMqDestination.of("my-exchange", eventBType)));
    }

    @Test
    void destinationsFor_an_or_of_two_type_filters_unions_the_destinations() {
        SubscriptionFilter filter = AgnosticSubscriptionFilter.filter(Filter.type(eventAType).or(Filter.type(eventBType)));

        Optional<Set<RabbitMqDestination>> destinations = resolver.destinationsFor(filter);

        assertThat(destinations).contains(Set.of(
                RabbitMqDestination.of("my-exchange", eventAType),
                RabbitMqDestination.of("my-exchange", eventBType)));
    }

    @Test
    void destinationsFor_an_or_with_one_unconstrained_branch_cannot_narrow() {
        SubscriptionFilter filter = AgnosticSubscriptionFilter.filter(Filter.type(eventAType).or(Filter.streamId("some-stream")));

        Optional<Set<RabbitMqDestination>> destinations = resolver.destinationsFor(filter);

        assertThat(destinations).isEmpty();
    }

    @Test
    void destinationsFor_an_and_narrows_to_the_type_conjunct_even_though_the_other_conjunct_is_not_a_type_filter() {
        SubscriptionFilter filter = AgnosticSubscriptionFilter.filter(Filter.type(eventAType).and(Filter.streamId("some-stream")));

        Optional<Set<RabbitMqDestination>> destinations = resolver.destinationsFor(filter);

        assertThat(destinations).contains(Set.of(RabbitMqDestination.of("my-exchange", eventAType)));
    }

    @Test
    void destinationsFor_a_filter_on_an_unrelated_field_cannot_narrow() {
        SubscriptionFilter filter = AgnosticSubscriptionFilter.filter(Filter.subject("some-subject"));

        Optional<Set<RabbitMqDestination>> destinations = resolver.destinationsFor(filter);

        assertThat(destinations).isEmpty();
    }

    @Test
    void destinationsFor_a_subscription_filter_this_resolver_does_not_understand_cannot_narrow() {
        SubscriptionFilter filter = new SubscriptionFilter() {
        };

        Optional<Set<RabbitMqDestination>> destinations = resolver.destinationsFor(filter);

        assertThat(destinations).isEmpty();
    }

    @Test
    void destinationsFor_the_subscription_filter_overload_is_the_one_this_resolver_inherits() throws NoSuchMethodException {
        Class<?> declaringClass = RabbitMqTopicExchangeDestinationResolver.class
                .getMethod("destinationsFor", SubscriptionFilter.class).getDeclaringClass();

        assertThat(declaringClass).isEqualTo(DestinationResolver.class);
    }

    private CloudEvent cloudEventOfType(String type) {
        return CloudEventBuilder.v1().withId("id").withSource(URI.create("urn:test")).withType(type).build();
    }

    private interface TestEvent {
    }

    private static final class EventA implements TestEvent {
    }

    private static final class EventB implements TestEvent {
    }
}
