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
import org.junit.jupiter.api.Test;
import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.SubscriptionFilter;

import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RabbitMqTopologyTest {

    private static final RabbitMqDestination CATCH_ALL = RabbitMqDestination.of("catch-all-exchange", "#");

    @Test
    void an_explicit_empty_bindings_set_is_refused_rather_than_bound_to_nothing() {
        assertThatThrownBy(() -> RabbitMqTopology.destinationsToBind(null, null, Set.of()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("declareTopology(false)");
    }

    @Test
    void an_explicit_non_empty_bindings_set_is_still_passed_through_unchanged() {
        RabbitMqDestination destination = RabbitMqDestination.of("my-exchange", "my.routing.key");

        Set<RabbitMqDestination> result = RabbitMqTopology.destinationsToBind(null, null, Set.of(destination));

        assertThat(result).containsExactly(destination);
    }

    @Test
    void a_resolver_that_cannot_narrow_the_bindingFilter_still_falls_back_to_the_catchAllDestination() {
        DestinationResolver<RabbitMqDestination> resolver = new DestinationResolver<>() {
            @Override
            public RabbitMqDestination destinationFor(CloudEvent cloudEvent) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Optional<Set<RabbitMqDestination>> destinationsFor(SubscriptionFilter filter) {
                return Optional.empty();
            }

            @Override
            public RabbitMqDestination catchAllDestination() {
                return CATCH_ALL;
            }
        };
        SubscriptionFilter bindingFilter = AgnosticSubscriptionFilter.filter(Filter.type("SomeEvent"));

        Set<RabbitMqDestination> result = RabbitMqTopology.destinationsToBind(resolver, bindingFilter, null);

        assertThat(result).containsExactly(CATCH_ALL);
    }
}
