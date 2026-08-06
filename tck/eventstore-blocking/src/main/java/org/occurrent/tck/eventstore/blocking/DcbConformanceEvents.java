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

package org.occurrent.tck.eventstore.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.tck.ConformanceEvents;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * DCB-tagged CloudEvents for the DCB conformance suites, built by stamping tags onto a
 * {@link ConformanceEvents} event.
 * <p>
 * This lives here rather than in {@code occurrent-tck-common} on purpose. That module depends on
 * {@code cloudevents-core} and {@code jspecify} and nothing else, and the subscription TCK modules will depend on it
 * too, so putting a DCB tag helper there would hand the DCB API to every module that only wanted a CloudEvent. This
 * module already depends on the DCB API, because {@link EventStoreFixture} hands back a
 * {@link org.occurrent.eventstore.api.dcb.DcbEventStore}, so the dependency is paid for here and nowhere else.
 * <p>
 * Tags are supplied in their string form ({@code "course:c1"}, or a value-less marker such as {@code "premium"}) so a
 * suite reads as the DCB specification writes them rather than as a chain of {@link Tag#of(String, String)} calls.
 */
@NullMarked
public final class DcbConformanceEvents {

    private DcbConformanceEvents() {
    }

    /**
     * An event with a generated id and the supplied DCB tags. Use this when a test cares that events are distinct and
     * which boundary they belong to, but not what their ids are.
     */
    public static CloudEvent taggedEvent(String type, String... tags) {
        return DcbCloudEvents.withTags(ConformanceEvents.event(type), tagsOf(tags));
    }

    /**
     * An event with an explicit id and the supplied DCB tags, which the duplicate-detection and delete-by-id
     * assertions need so they can hand the same id back.
     * <p>
     * Named apart from {@link #taggedEvent(String, String...)} rather than overloading it. With a trailing varargs of
     * the same type, {@code taggedEvent("NameDefined", "name:1")} would match both and the compiler would say so at
     * every call site.
     */
    public static CloudEvent taggedEventWithId(String id, String type, String... tags) {
        return DcbCloudEvents.withTags(ConformanceEvents.event(id, type), tagsOf(tags));
    }

    /**
     * An event with the supplied DCB tags and the supplied JSON body verbatim, so a suite can put something in the
     * payload that a store must not mistake for DCB metadata.
     */
    public static CloudEvent taggedEventWithJsonData(String type, String json, String... tags) {
        return DcbCloudEvents.withTags(
                ConformanceEvents.eventWithJsonData(UUID.randomUUID().toString(), type, json), tagsOf(tags));
    }

    /**
     * An event carrying the DCB tags extension with no tags in it.
     * <p>
     * A DCB append stamps the extension even for an empty tag set, which is what
     * {@link DcbCloudEvents#isDcbEvent(CloudEvent)} keys on, so an untagged DCB event is a real shape a store has to
     * handle rather than a degenerate one. It is reachable only through a type-scoped criteria.
     */
    public static CloudEvent untaggedDcbEvent(String type) {
        return DcbCloudEvents.withTags(ConformanceEvents.event(type), Set.of());
    }

    /**
     * Parses tags from their string form.
     */
    public static List<Tag> tagsOf(String... tags) {
        requireNonNull(tags, "tags cannot be null");
        return Arrays.stream(tags).map(tag -> Tag.parse(requireNonNull(tag, "tag cannot be null"))).toList();
    }

    /**
     * A single tag from its string form, so a suite can name a boundary without importing {@link Tag}.
     */
    public static Tag tag(String tag) {
        return Tag.parse(requireNonNull(tag, "tag cannot be null"));
    }

    /**
     * The DCB tags on the supplied event, for an assertion about which boundary a store placed an event in.
     */
    public static Set<Tag> tagsOn(CloudEvent event) {
        return DcbCloudEvents.getTags(requireNonNull(event, "event cannot be null"));
    }

    /**
     * The CloudEvent types of the supplied events, in order.
     * <p>
     * The DCB suites assert on types far more than on ids, because a DCB read is selected by type and tag and a
     * failure reading "expected [NameDefined, NameChanged] but was [NameDefined]" says what went wrong where a pair of
     * generated ids does not.
     */
    public static List<String> typesOf(List<CloudEvent> events) {
        requireNonNull(events, "events cannot be null");
        return events.stream().map(CloudEvent::getType).toList();
    }

    /**
     * The CloudEvent types of the supplied events, in order.
     */
    public static List<String> typesOf(CloudEvent... events) {
        requireNonNull(events, "events cannot be null");
        return Stream.of(events).map(CloudEvent::getType).toList();
    }
}
