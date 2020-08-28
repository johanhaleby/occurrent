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

import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.Name;
import org.occurrent.eventstore.api.blocking.EventStream;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
public class NameApplicationService {

    private final DomainEventStore eventStore;

    public NameApplicationService(DomainEventStore eventStore) {
        this.eventStore = eventStore;
    }

    public void defineName(UUID id, LocalDateTime time, String name) {
        List<DomainEvent> events = Name.defineName(UUID.randomUUID().toString(), time, name);
        eventStore.append(id, events);
    }

    public void changeName(UUID id, LocalDateTime time, String name) {
        EventStream<DomainEvent> eventStream = eventStore.loadEventStream(id);
        List<DomainEvent> events = eventStream.eventList();

        List<DomainEvent> newEvents = Name.changeName(events, UUID.randomUUID().toString(), time, name);

        eventStore.append(id, newEvents);
    }
}