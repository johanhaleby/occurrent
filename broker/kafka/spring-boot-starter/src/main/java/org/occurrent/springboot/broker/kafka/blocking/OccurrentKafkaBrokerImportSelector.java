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

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.bind.BindResult;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.DeferredImportSelector;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotationMetadata;

import java.util.List;

/**
 * A plain {@code @Import(OccurrentKafkaAutoConfiguration.class)} on {@link EnableOccurrentKafkaBroker} would let
 * the domain sink's {@code @ConditionalOnBean(CloudEventConverter.class)} be evaluated before the application's
 * own {@code @Bean CloudEventConverter} method registers, since {@code @Import} carries no ordering guarantee
 * against the importing class's own sibling {@code @Configuration} classes. A {@link DeferredImportSelector}
 * processes its selected classes only after every regular configuration class in the context is registered, the
 * same mechanism Spring Boot's own {@code @EnableAutoConfiguration} uses for exactly this reason, so the
 * condition sees the application's {@code CloudEventConverter} bean regardless of where it declares it.
 * <p>
 * Also where the missing-prerequisite warning lives, rather than on {@link KafkaBootstrapServersConfiguredCondition}
 * itself. That condition's own no-match is invisible without {@code --debug}: activation here is an explicit
 * opt-in, {@link EnableOccurrentKafkaBroker}, so "you asked for the broker and got nothing" is worth a plain
 * {@code WARN}, not a condition-evaluation report nobody reads.
 */
class OccurrentKafkaBrokerImportSelector implements DeferredImportSelector, EnvironmentAware {

    private static final Logger log = LoggerFactory.getLogger(OccurrentKafkaBrokerImportSelector.class);

    private @Nullable Environment environment;

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    @Override
    public String[] selectImports(AnnotationMetadata importingClassMetadata) {
        if (environment != null && !bootstrapServersConfigured(environment)) {
            log.warn("@EnableOccurrentKafkaBroker is active but \"{}\" is not configured. " +
                    "OccurrentKafkaAutoConfiguration registers nothing without it, no CloudEventSink, no bridge " +
                    "factory, silently. Set it to your cluster's bootstrap servers.",
                    KafkaBootstrapServersConfiguredCondition.PROPERTY);
        }
        return new String[]{OccurrentKafkaAutoConfiguration.class.getName()};
    }

    // Mirrors KafkaBootstrapServersConfiguredCondition's own binding, so this warning and that condition can never
    // disagree about what counts as configured. See that condition's own javadoc for why a plain property-presence
    // check is not enough.
    private static boolean bootstrapServersConfigured(Environment environment) {
        BindResult<List<String>> bound = Binder.get(environment)
                .bind(KafkaBootstrapServersConfiguredCondition.PROPERTY, Bindable.listOf(String.class));
        return bound.isBound() && bound.get().stream().anyMatch(value -> !value.isBlank());
    }
}
