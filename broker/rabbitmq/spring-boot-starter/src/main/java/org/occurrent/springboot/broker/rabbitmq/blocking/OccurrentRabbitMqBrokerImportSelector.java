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

package org.occurrent.springboot.broker.rabbitmq.blocking;

import org.springframework.context.annotation.DeferredImportSelector;
import org.springframework.core.type.AnnotationMetadata;

/**
 * A plain {@code @Import(OccurrentRabbitMqAutoConfiguration.class)} on {@link EnableOccurrentRabbitMqBroker} would
 * let {@code @ConditionalOnBean(Connection.class)} be evaluated before the application's own {@code @Bean
 * Connection} method registers, since {@code @Import} carries no ordering guarantee against the importing
 * class's own sibling {@code @Configuration} classes. A {@link DeferredImportSelector} processes its selected
 * classes only after every regular configuration class in the context is registered, the same mechanism Spring
 * Boot's own {@code @EnableAutoConfiguration} uses for exactly this reason, so the condition sees the
 * application's {@code Connection} bean regardless of where it declares it.
 */
class OccurrentRabbitMqBrokerImportSelector implements DeferredImportSelector {

    @Override
    public String[] selectImports(AnnotationMetadata importingClassMetadata) {
        return new String[]{OccurrentRabbitMqAutoConfiguration.class.getName()};
    }
}
