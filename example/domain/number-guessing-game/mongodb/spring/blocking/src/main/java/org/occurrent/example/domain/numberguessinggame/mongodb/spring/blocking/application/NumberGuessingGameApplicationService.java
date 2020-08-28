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

package org.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.application;

import io.cloudevents.CloudEvent;
import org.occurrent.eventstore.api.WriteConditionNotFulfilledException;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStream;
import org.occurrent.example.domain.numberguessinggame.model.domainevents.GameEvent;
import org.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.infrastructure.Serialization;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

@Service
public class NumberGuessingGameApplicationService {

    private final EventStore eventStore;
    private final Serialization serialization;

    public NumberGuessingGameApplicationService(EventStore eventStore, Serialization serialization) {
        this.eventStore = eventStore;
        this.serialization = serialization;
    }

    @Retryable(include = WriteConditionNotFulfilledException.class, maxAttempts = 5, backoff = @Backoff(delay = 100, multiplier = 2, maxDelay = 1000))
    public void play(UUID gameId, Function<Stream<GameEvent>, Stream<GameEvent>> domainFn) {
        EventStream<CloudEvent> eventStream = eventStore.read(gameId.toString());
        Stream<GameEvent> persistedGameEvents = eventStream.events().map(serialization::deserialize);

        Stream<GameEvent> newGameEvents = domainFn.apply(persistedGameEvents);

        eventStore.write(gameId.toString(), eventStream.version(), newGameEvents.map(serialization::serialize));
    }
}