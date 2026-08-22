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

package org.occurrent.example.broker.rabbitmq;

/**
 * The two-event order lifecycle the broker example forwards, bridges and projects. The permitted types are
 * top-level, not nested, so the reflection-based {@code CloudEventTypeMapper} can resolve each one from its simple
 * name.
 * <p>
 * {@code eventId} is a field on the event itself, not left to the CloudEvent wrapping it, because
 * {@code DomainEventFeed}'s catch-up-to-live de-dup key is a {@code Function<OrderEvent, String>} with no CloudEvent
 * in reach. It has to survive the JSON round trip a catch-up replay and a live redelivery each do independently, so
 * a value derived only at conversion time, a random one say, would not do. The two paths would compute two
 * different keys for what is otherwise the same delivered event.
 */
public sealed interface OrderEvent permits OrderPlaced, OrderShipped {
    String eventId();

    String orderId();
}

record OrderPlaced(String eventId, String orderId, String product) implements OrderEvent {
}

record OrderShipped(String eventId, String orderId) implements OrderEvent {
}
