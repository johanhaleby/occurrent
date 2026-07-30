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

package org.occurrent.tck;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;

/**
 * CloudEvents for the conformance suites to write and read back.
 * <p>
 * These are deliberately self-contained. The TCK is published, so it cannot depend on the unpublished
 * {@code test-support} module, and it does not pull in a JSON library just to build a payload: the data is a small
 * hand-written JSON object. Nothing here is MongoDB-specific or Occurrent-specific beyond the CloudEvents
 * specification itself.
 * <p>
 * Every event carries an {@code id}, a {@code source}, a {@code type}, a {@code subject}, a {@code time}, a
 * {@code datacontenttype} of {@code application/json} and a body with a single {@code name} field. That is the
 * smallest shape the suites can filter, sort and assert on, and it is what an event store contract is defined over.
 * A suite that needs a wider shape should say so by taking the attribute as a parameter rather than by growing this
 * class.
 */
@NullMarked
public final class ConformanceEvents {

    /**
     * The {@code source} every event built here carries. A store must not interpret it, but {@code deleteEvent} and
     * {@code updateEvent} are addressed by {@code (id, source)} so a suite needs a stable value to hand back.
     */
    public static final URI SOURCE = URI.create("urn:occurrent:tck");

    /**
     * The instant every event built here carries unless one is supplied. Fixed rather than "now" so a failure is
     * reproducible and so two events built in the same test are only distinguishable by the attributes a suite set
     * on purpose.
     */
    public static final OffsetDateTime TIME = OffsetDateTime.of(2026, 7, 28, 12, 0, 0, 0, ZoneOffset.UTC);

    /**
     * The {@code datacontenttype} every event built here carries unless one is supplied.
     */
    public static final String CONTENT_TYPE = "application/json";

    private ConformanceEvents() {
    }

    /**
     * An event with a generated id, whose subject and name are both derived from the type. Use this when a test cares
     * that events are distinct but not how.
     */
    public static CloudEvent event(String type) {
        return event(UUID.randomUUID().toString(), type);
    }

    /**
     * An event with an explicit id, which is what the duplicate-detection and delete-by-id suites need.
     */
    public static CloudEvent event(String id, String type) {
        return event(id, type, type, TIME);
    }

    /**
     * An event with an explicit id and subject. {@code subject} is the attribute the query suites filter and sort on
     * when they need an ordering that is independent of insertion order.
     */
    public static CloudEvent event(String id, String type, String subject) {
        return event(id, type, subject, TIME);
    }

    /**
     * An event with an explicit time, for the suites that assert time-based filtering and sorting.
     */
    public static CloudEvent eventAt(String id, String type, OffsetDateTime time) {
        return event(id, type, type, time);
    }

    /**
     * The fully explicit form. The body is {@code {"name":"<subject>"}}, so a suite can assert on the payload without
     * a JSON parser by comparing the raw bytes to {@link #dataFor(String)}.
     */
    public static CloudEvent event(String id, String type, String subject, OffsetDateTime time) {
        requireNonNull(id, "id cannot be null");
        requireNonNull(type, "type cannot be null");
        requireNonNull(subject, "subject cannot be null");
        requireNonNull(time, "time cannot be null");
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(SOURCE)
                .withType(type)
                .withSubject(subject)
                .withTime(time)
                .withDataContentType(CONTENT_TYPE)
                .withData(dataFor(subject))
                .build();
    }

    /**
     * An event carrying a {@code dataschema}, which the events built by the other factories deliberately leave unset so
     * a filter on it can tell them apart.
     */
    public static CloudEvent eventWithDataSchema(String id, String type, URI dataSchema) {
        requireNonNull(dataSchema, "dataSchema cannot be null");
        return CloudEventBuilder.v1(event(id, type)).withDataSchema(dataSchema).build();
    }

    /**
     * An event carrying an explicit {@code datacontenttype} and matching body. The other factories always say
     * {@value #CONTENT_TYPE}, so a suite filtering on the content type needs one event that says something else.
     */
    public static CloudEvent eventWithDataContentType(String id, String type, String dataContentType, byte[] data) {
        requireNonNull(dataContentType, "dataContentType cannot be null");
        requireNonNull(data, "data cannot be null");
        return CloudEventBuilder.v1(event(id, type)).withDataContentType(dataContentType).withData(data).build();
    }

    /**
     * The exact bytes {@link #event(String, String, String, OffsetDateTime)} uses as the event body, so a suite can
     * assert that a store round-trips a payload byte for byte.
     */
    public static byte[] dataFor(String name) {
        requireNonNull(name, "name cannot be null");
        return ("{\"name\":\"" + name + "\"}").getBytes(StandardCharsets.UTF_8);
    }

    /**
     * The ids of the supplied events, in order. Almost every assertion in the suites is "these events, in this
     * order", and comparing ids says that without depending on how a store rebuilds the rest of the event.
     */
    public static List<String> idsOf(Iterable<CloudEvent> events) {
        requireNonNull(events, "events cannot be null");
        return StreamSupport.stream(events.spliterator(), false).map(CloudEvent::getId).toList();
    }

    /**
     * The ids of the supplied events, in order.
     * <p>
     * Closes the stream once it has been read. {@code EventStoreQueries.query(..)} can hand back a stream sitting on a
     * database cursor, so a suite calling {@code idsOf(query(..))} would otherwise leak one per assertion.
     */
    public static List<String> idsOf(Stream<CloudEvent> events) {
        requireNonNull(events, "events cannot be null");
        try (Stream<CloudEvent> toRead = events) {
            return toRead.map(CloudEvent::getId).toList();
        }
    }

    /**
     * The ids of the supplied events, in order.
     */
    public static List<String> idsOf(CloudEvent... events) {
        requireNonNull(events, "events cannot be null");
        return Stream.of(events).map(CloudEvent::getId).toList();
    }

    /**
     * The value of a CloudEvent extension, or {@code null} when the event does not carry it. Saves every suite
     * writing the {@code getExtension} plus {@code toString} dance when asserting on the Occurrent stream
     * extensions a store adds on write.
     */
    public static @Nullable String extension(CloudEvent event, String name) {
        requireNonNull(event, "event cannot be null");
        requireNonNull(name, "name cannot be null");
        Object value = event.getExtension(name);
        return value == null ? null : value.toString();
    }
}
