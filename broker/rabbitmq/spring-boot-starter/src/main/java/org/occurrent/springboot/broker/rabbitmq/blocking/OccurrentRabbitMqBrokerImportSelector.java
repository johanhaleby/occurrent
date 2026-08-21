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

import com.rabbitmq.client.Connection;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.ListableBeanFactory;
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
 * <p>
 * Also where the missing-prerequisite warning lives, rather than on the condition itself.
 * {@code @ConditionalOnBean(Connection.class)}'s own no-match is invisible without {@code --debug}: activation here
 * is an explicit opt-in, {@link EnableOccurrentRabbitMqBroker}, so "you asked for the broker and got nothing" is
 * worth a plain {@code WARN}, not a condition-evaluation report nobody reads.
 */
class OccurrentRabbitMqBrokerImportSelector implements DeferredImportSelector, BeanFactoryAware {

    private static final Logger log = LoggerFactory.getLogger(OccurrentRabbitMqBrokerImportSelector.class);

    private @Nullable ListableBeanFactory beanFactory;

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        if (beanFactory instanceof ListableBeanFactory listableBeanFactory) {
            this.beanFactory = listableBeanFactory;
        }
    }

    @Override
    public String[] selectImports(AnnotationMetadata importingClassMetadata) {
        if (beanFactory != null && beanFactory.getBeanNamesForType(Connection.class, true, false).length == 0) {
            log.warn("@EnableOccurrentRabbitMqBroker is active but no com.rabbitmq.client.Connection bean was " +
                    "found. OccurrentRabbitMqAutoConfiguration registers nothing without one, no CloudEventSink, " +
                    "no bridge factory, silently. spring-boot-starter-amqp, if present on the classpath, supplies " +
                    "a Spring AMQP ConnectionFactory, which does not satisfy this. Declare your own " +
                    "@Bean Connection; see EnableOccurrentRabbitMqBroker's javadoc for the snippet.");
        }
        return new String[]{OccurrentRabbitMqAutoConfiguration.class.getName()};
    }
}
