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

import io.cloudevents.CloudEvent;
import org.occurrent.application.converter.CloudEventConverter;

import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;

/**
 * A minimal {@link CloudEventConverter} for a {@code String} domain event, so a forwarder test can exercise real
 * decoding without pulling in a JSON mapper. The domain event is the event's data, decoded as UTF-8.
 */
class FakeStringCloudEventConverter implements CloudEventConverter<String> {

    @Override
    public CloudEvent toCloudEvent(String domainEvent) {
        throw new UnsupportedOperationException("Not needed by DomainEventForwarder, which only decodes");
    }

    @Override
    public String toDomainEvent(CloudEvent cloudEvent) {
        return new String(requireNonNull(cloudEvent.getData()).toBytes(), StandardCharsets.UTF_8);
    }

    @Override
    public String getCloudEventType(Class<? extends String> type) {
        return "String";
    }
}
