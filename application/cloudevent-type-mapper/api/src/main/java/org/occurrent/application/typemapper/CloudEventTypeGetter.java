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

package org.occurrent.application.typemapper;

import java.util.Objects;

@FunctionalInterface
public interface CloudEventTypeGetter<T> {

    /**
     * Get the cloud event type from a Java class.
     *
     * @param type The java class that represents a specific domain event type
     * @return The cloud event type of the domain event (cannot be {@code null})
     */
    String getCloudEventType(Class<? extends T> type);

    /**
     * Get the cloud event type from a Java instance.
     *
     * @param event An instance of a domain event
     * @return The cloud event type of the domain event (cannot be {@code null})
     */
    default String getCloudEventType(T event) {
        Objects.requireNonNull(event, "Domain event cannot be null");
        //noinspection unchecked
        return getCloudEventType((Class<T>) event.getClass());
    }
}
