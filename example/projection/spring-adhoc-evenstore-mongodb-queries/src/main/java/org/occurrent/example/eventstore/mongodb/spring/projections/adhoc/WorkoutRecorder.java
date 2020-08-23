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