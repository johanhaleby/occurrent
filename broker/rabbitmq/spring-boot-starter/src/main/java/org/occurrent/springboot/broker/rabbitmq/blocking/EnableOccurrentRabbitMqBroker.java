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

import org.springframework.context.annotation.Import;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Enables the RabbitMQ broker auto-configuration, the same {@code @Import}-based activation
 * {@code EnableOccurrent} uses for the MongoDB starter. Imports through
 * {@link OccurrentRabbitMqBrokerImportSelector} rather than the configuration class directly, so
 * {@code @ConditionalOnBean(Connection.class)} is evaluated after every regular configuration class in the
 * context, the application's own {@code Connection} bean included, is registered.
 * <p>
 * <strong>Requires a {@code com.rabbitmq.client.Connection} bean, which this starter never builds itself.</strong>
 * {@code spring-boot-starter-amqp}, if it happens to be on the classpath too, does not supply one either, it
 * registers a Spring AMQP {@code ConnectionFactory} instead, a different type this condition does not recognize.
 * Declare the connection yourself, for example:
 * <pre>{@code
 * @Bean
 * Connection rabbitMqConnection() throws IOException, TimeoutException {
 *     ConnectionFactory connectionFactory = new ConnectionFactory();
 *     connectionFactory.setUri("amqp://guest:guest@localhost:5672");
 *     return connectionFactory.newConnection();
 * }
 * }</pre>
 * Activating this annotation with no such bean present registers nothing and logs a {@code WARN} naming the
 * missing bean, see {@link OccurrentRabbitMqBrokerImportSelector}.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@Import(OccurrentRabbitMqBrokerImportSelector.class)
public @interface EnableOccurrentRabbitMqBroker {
}
