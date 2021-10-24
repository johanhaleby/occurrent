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

package org.occurrent.example.eventstore.mongodb.spring.reactor.transactional;

import io.vavr.API;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.Name;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static io.vavr.API.Match;
import static io.vavr.Predicates.instanceOf;

@Service
@Transactional
public class NameApplicationService {

    private final DomainEventStore eventStore;
    private CurrentNameProjection currentNameProjection;

    public NameApplicationService(DomainEventStore eventStore, CurrentNameProjection currentNameProjection) {
        this.eventStore = eventStore;
        this.currentNameProjection = currentNameProjection;
    }

    public Mono<Void> defineName(UUID id, LocalDateTime time, String name) {
        List<DomainEvent> events = Name.defineTheName(id.toString(), time, name);
        return eventStore.append(id, 0, events)
                .then(currentNameProjection.save(buildProjectionFromEvents(id, events)))
                .then();
    }

    public Mono<Void> changeName(UUID id, LocalDateTime time, String name) {
        return eventStore.loadEventStream(id)
                .flatMap(eventStream -> eventStream.eventList().flatMap(events -> {
                    List<DomainEvent> newEvents = Name.changeName(events, UUID.randomUUID().toString(), time, name);

                    return eventStore.append(id, eventStream.version(), newEvents)
                            .then(currentNameProjection.save(buildProjectionFromEvents(id, append(events, newEvents))))
                            .then();
                }));
    }

    private CurrentName buildProjectionFromEvents(UUID id, List<DomainEvent> domainEvents) {
        return io.vavr.collection.List.ofAll(domainEvents).foldLeft(new CurrentName(id.toString()), (currentName, domainEvent) ->
                Match(domainEvent).of(
                        API.Case(API.$(instanceOf(NameDefined.class)), e -> currentName.changeName(e.getName())),
                        API.Case(API.$(instanceOf(NameWasChanged.class)), e -> currentName.changeName(e.getName()))
                )
        );
    }

    private static <T> List<T> append(List<T> list, List<T> list2) {
        ArrayList<T> ts1 = new ArrayList<>(list);
        ts1.addAll(list2);
        return Collections.unmodifiableList(ts1);
    }

    // For testing purposes
    void setCurrentNameProjection(CurrentNameProjection currentNameProjection) {
        this.currentNameProjection = currentNameProjection;
    }
}