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

/**
 * Whether {@code occurrent.broker.kafka.topic} is configured, checked by binding it rather than by a plain
 * {@code @ConditionalOnProperty(name = ...)}. That check is boolean-oriented, it fails to match the literal value
 * {@code false} and matches an empty value, and both readings are wrong for a resource name. {@code false} is a
 * legal topic name, and an empty topic builds a resolver that later fails when a bridge tries to subscribe to it.
 * Binding to {@code String} and asking whether anything nonblank bound sidesteps both cases.
 */
class KafkaTopicConfiguredCondition extends SpringBootCondition {

    static final String PROPERTY = "occurrent.broker.kafka.topic";

    @Override
    public ConditionOutcome getMatchOutcome(ConditionContext context, AnnotatedTypeMetadata metadata) {
        BindResult<String> bound = Binder.get(context.getEnvironment())
                .bind(PROPERTY, Bindable.of(String.class));
        if (bound.isBound() && !bound.get().isBlank()) {
            return ConditionOutcome.match("\"" + PROPERTY + "\" is configured");
        }
        return ConditionOutcome.noMatch("\"" + PROPERTY + "\" is not configured");
    }
}
