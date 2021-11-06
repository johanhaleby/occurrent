/*
 *
 *  Copyright 2021 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.application.converter.typemapper;

/**
 * A cloud event type mapper is component whose purpose it is to get the <a href="https://occurrent.org/documentation#cloudevents">cloud event type</a> from a
 * class or instance of your domain event type and vice versa. It is typically used as a helper to {@code CloudEventConverter}'s.
 *
 * @param <T> The base type of your domain events
 */
public interface CloudEventTypeMapper<T> extends CloudEventTypeGetter<T>, DomainEventTypeGetter<T> {
}
