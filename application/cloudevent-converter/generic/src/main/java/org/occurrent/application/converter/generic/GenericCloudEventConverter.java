/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.application.converter.generic;

import io.cloudevents.CloudEvent;
import org.occurrent.application.converter.CloudEventConverter;

import java.util.function.Function;

/**
 * A generic implementation of a {@link CloudEventConverter} that takes two functions,
 * that converts between a cloud event and a domain event and vice versa.
 *
 * @param <T> The type of the domain event
 */
public class GenericCloudEventConverter<T> implements CloudEventConverter<T> {

    private final Function<CloudEvent, T> convertToDomainEvent;
    private final Function<T, CloudEvent> convertToCloudEvent;

    public GenericCloudEventConverter(Function<CloudEvent, T> convertToDomainEvent, Function<T, CloudEvent> convertToCloudEvent) {
        this.convertToDomainEvent = convertToDomainEvent;
        this.convertToCloudEvent = convertToCloudEvent;
    }

    @Override
    public CloudEvent toCloudEvent(T domainEvent) {
        return convertToCloudEvent.apply(domainEvent);
    }

    @Override
    public T toDomainEvent(CloudEvent cloudEvent) {
        return convertToDomainEvent.apply(cloudEvent);
    }
}
