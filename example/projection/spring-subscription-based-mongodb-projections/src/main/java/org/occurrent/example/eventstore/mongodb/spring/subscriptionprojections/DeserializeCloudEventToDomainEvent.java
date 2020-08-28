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

package org.occurrent.example.eventstore.mongodb.spring.subscriptionprojections;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.occurrent.domain.DomainEvent;
import org.springframework.stereotype.Component;

import static org.occurrent.functional.CheckedFunction.unchecked;

@Component
public class DeserializeCloudEventToDomainEvent {

    private final ObjectMapper objectMapper;

    public DeserializeCloudEventToDomainEvent(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public DomainEvent deserialize(CloudEvent cloudEvent) {
        return unchecked((CloudEvent e) -> (DomainEvent) objectMapper.readValue(e.getData(), Class.forName(e.getType()))).apply(cloudEvent);
    }

}
