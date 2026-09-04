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
import org.junit.jupiter.api.Test;
import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.SubscriptionFilter;

import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaTopologyTest {

    private static final KafkaDestination CATCH_ALL = KafkaDestination.of("catch-all-topic");

    @Test
    void an_explicit_empty_bindings_set_is_refused_rather_than_treated_as_an_unsubscribe() {
        assertThatThrownBy(() -> KafkaTopology.topicsToSubscribe(null, null, Set.of()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("bindings(...)");
    }

    @Test
    void an_explicit_non_empty_bindings_set_is_still_passed_through_unchanged() {
        KafkaDestination destination = KafkaDestination.of("my-topic");

        Set<KafkaDestination> result = KafkaTopology.topicsToSubscribe(null, null, Set.of(destination));

        assertThat(result).containsExactly(destination);
    }

    @Test
    void a_resolver_that_cannot_narrow_the_bindingFilter_still_falls_back_to_the_catchAllDestination() {
        DestinationResolver<KafkaDestination> resolver = new DestinationResolver<>() {
            @Override
            public KafkaDestination destinationFor(CloudEvent cloudEvent) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Optional<Set<KafkaDestination>> destinationsFor(Filter filter) {
                return Optional.empty();
            }

            @Override
            public KafkaDestination catchAllDestination() {
                return CATCH_ALL;
            }
        };
        SubscriptionFilter bindingFilter = AgnosticSubscriptionFilter.filter(Filter.type("SomeEvent"));

        Set<KafkaDestination> result = KafkaTopology.topicsToSubscribe(resolver, bindingFilter, null);

        assertThat(result).containsExactly(CATCH_ALL);
    }
}
