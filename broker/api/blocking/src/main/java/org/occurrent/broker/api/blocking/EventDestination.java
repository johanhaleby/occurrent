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

/**
 * Where an event goes on a broker. Each transport contributes its own record implementing this, because an
 * exchange and a routing key are not a topic and a partition key. {@code RabbitMqDestination} carries an exchange,
 * a routing key and headers. {@code KafkaDestination} carries a topic, a nullable message key and headers.
 * <p>
 * A destination means slightly different things depending on direction. Publishing uses every component of it.
 * A consumer declaring a binding uses only the routing components, an exchange and routing key on RabbitMQ or a
 * topic on Kafka, so a {@link DestinationResolver#destinationsFor(org.occurrent.filter.Filter)}
 * result leaves the per-message components empty.
 */
public interface EventDestination {
}
