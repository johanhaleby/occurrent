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

@FunctionalInterface
public interface DomainEventTypeGetter<T> {

    /**
     * Get the domain event class from a cloud event type.
     *
     * @param cloudEventType The type as defined by the cloud event
     * @return The java class that represents a specific cloud event type
     */
    <E extends T> Class<E> getDomainEventType(String cloudEventType);
}
