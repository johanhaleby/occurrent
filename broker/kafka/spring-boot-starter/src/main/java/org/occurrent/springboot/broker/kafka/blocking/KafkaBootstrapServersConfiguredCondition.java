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

package org.occurrent.springboot.broker.kafka.blocking;

import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.boot.context.properties.bind.BindResult;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

import java.util.List;

/**
 * Whether {@code occurrent.broker.kafka.bootstrap-servers} is configured, checked by binding it rather than by a
 * plain {@code @ConditionalOnProperty(name = ...)}. A property-presence check only recognizes the comma-separated
 * scalar form ({@code bootstrap-servers: host1:9092,host2:9092}). The indexed YAML list form
 * ({@code bootstrap-servers: [host1:9092, host2:9092]}, which flattens to keys like
 * {@code bootstrap-servers[0]}) has no property literally named {@code bootstrap-servers} at all, so that check
 * silently misses it. Binding to {@code List<String>} and asking whether anything bound handles both forms the
 * same way {@link KafkaBrokerProperties#getBootstrapServers()} itself does.
 */
class KafkaBootstrapServersConfiguredCondition extends SpringBootCondition {

    static final String PROPERTY = "occurrent.broker.kafka.bootstrap-servers";

    @Override
    public ConditionOutcome getMatchOutcome(ConditionContext context, AnnotatedTypeMetadata metadata) {
        BindResult<List<String>> bound = Binder.get(context.getEnvironment())
                .bind(PROPERTY, Bindable.listOf(String.class));
        if (bound.isBound() && !bound.get().isEmpty()) {
            return ConditionOutcome.match("\"" + PROPERTY + "\" is configured");
        }
        return ConditionOutcome.noMatch("\"" + PROPERTY + "\" is not configured");
    }
}
