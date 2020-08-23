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

package org.occurrent.example.eventstore.mongodb.spring.projections.adhoc;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEventAttributes;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.functional.CheckedFunction;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.stream.Stream;

import static java.time.ZoneOffset.UTC;
import static java.util.stream.Collectors.groupingBy;

@Component
public class WorkoutRecorder {

    private final EventStore eventStore;
    private final ObjectMapper objectMapper;

    public WorkoutRecorder(EventStore eventStore, ObjectMapper objectMapper) {
        this.eventStore = eventStore;
        this.objectMapper = objectMapper;
    }

    public void recordWorkoutCompleted(WorkoutWasCompleted... events) {
        Stream.of(events)
                .map(e -> CloudEventBuilder.v1()
                        .withId(e.getEventId().toString())
                        .withType(e.getClass().getName())
                        .withSubject(e.getWorkoutId().toString())
                        .withSource(URI.create("http://source"))
                        .withTime(e.getCompletedAt().atZone(UTC))
                        .withData(CheckedFunction.unchecked(objectMapper::writeValueAsBytes).apply(e))
                        .build())
                .collect(groupingBy(CloudEventAttributes::getSubject))
                .forEach((streamId, cloudEvents) -> eventStore.write(streamId, 0, cloudEvents.stream()));
    }
}