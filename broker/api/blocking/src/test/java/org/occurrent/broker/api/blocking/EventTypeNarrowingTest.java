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

import org.junit.jupiter.api.Test;
import org.occurrent.condition.Condition;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.SubscriptionFilter;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Both {@code RabbitMqTopicExchangeDestinationResolver} and {@code KafkaTopicPerTypeDestinationResolver} call
 * {@link EventTypeNarrowing#narrow(SubscriptionFilter)} directly, so this tests the shared walk once here rather
 * than duplicating the same filter-tree cases in each transport module.
 */
class EventTypeNarrowingTest {

    private static final String EVENT_A_TYPE = "com.example.EventA";
    private static final String EVENT_B_TYPE = "com.example.EventB";
    private static final String EVENT_C_TYPE = "com.example.EventC";

    @Test
    void an_equality_type_filter_resolves_to_a_single_type() {
        SubscriptionFilter filter = AgnosticSubscriptionFilter.filter(Filter.type(EVENT_A_TYPE));

        Optional<Set<String>> types = EventTypeNarrowing.narrow(filter);

        assertThat(types).contains(Set.of(EVENT_A_TYPE));
    }

    @Test
    void an_in_condition_type_filter_resolves_to_every_value() {
        SubscriptionFilter filter = AgnosticSubscriptionFilter.filter(Filter.type(Condition.in(EVENT_A_TYPE, EVENT_B_TYPE)));

        Optional<Set<String>> types = EventTypeNarrowing.narrow(filter);

        assertThat(types).contains(Set.of(EVENT_A_TYPE, EVENT_B_TYPE));
    }

    @Test
    void an_or_of_two_type_filters_unions_the_types() {
        SubscriptionFilter filter = AgnosticSubscriptionFilter.filter(Filter.type(EVENT_A_TYPE).or(Filter.type(EVENT_B_TYPE)));

        Optional<Set<String>> types = EventTypeNarrowing.narrow(filter);

        assertThat(types).contains(Set.of(EVENT_A_TYPE, EVENT_B_TYPE));
    }

    @Test
    void an_or_with_one_unconstrained_branch_cannot_narrow() {
        SubscriptionFilter filter = AgnosticSubscriptionFilter.filter(Filter.type(EVENT_A_TYPE).or(Filter.streamId("some-stream")));

        Optional<Set<String>> types = EventTypeNarrowing.narrow(filter);

        assertThat(types).isEmpty();
    }

    @Test
    void an_and_narrows_to_the_type_conjunct_even_though_the_other_conjunct_is_not_a_type_filter() {
        SubscriptionFilter filter = AgnosticSubscriptionFilter.filter(Filter.type(EVENT_A_TYPE).and(Filter.streamId("some-stream")));

        Optional<Set<String>> types = EventTypeNarrowing.narrow(filter);

        assertThat(types).contains(Set.of(EVENT_A_TYPE));
    }

    @Test
    void an_and_of_two_type_filters_intersects_them() {
        SubscriptionFilter filter = AgnosticSubscriptionFilter.filter(
                Filter.type(Condition.in(EVENT_A_TYPE, EVENT_B_TYPE)).and(Filter.type(EVENT_A_TYPE)));

        Optional<Set<String>> types = EventTypeNarrowing.narrow(filter);

        assertThat(types).contains(Set.of(EVENT_A_TYPE));
    }

    @Test
    void an_in_condition_with_no_values_cannot_narrow() {
        SubscriptionFilter filter = AgnosticSubscriptionFilter.filter(Filter.type(Condition.in(List.of())));

        Optional<Set<String>> types = EventTypeNarrowing.narrow(filter);

        assertThat(types).isEmpty();
    }

    @Test
    void an_and_of_two_type_filters_with_no_type_in_common_cannot_narrow() {
        SubscriptionFilter filter = AgnosticSubscriptionFilter.filter(
                Filter.type(Condition.in(EVENT_A_TYPE, EVENT_B_TYPE)).and(Filter.type(EVENT_C_TYPE)));

        Optional<Set<String>> types = EventTypeNarrowing.narrow(filter);

        assertThat(types).isEmpty();
    }

    @Test
    void a_filter_on_an_unrelated_field_cannot_narrow() {
        SubscriptionFilter filter = AgnosticSubscriptionFilter.filter(Filter.subject("some-subject"));

        Optional<Set<String>> types = EventTypeNarrowing.narrow(filter);

        assertThat(types).isEmpty();
    }

    @Test
    void a_subscription_filter_this_resolver_does_not_understand_cannot_narrow() {
        SubscriptionFilter filter = new SubscriptionFilter() {
        };

        Optional<Set<String>> types = EventTypeNarrowing.narrow(filter);

        assertThat(types).isEmpty();
    }
}
