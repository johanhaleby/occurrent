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

package org.occurrent.eventstore.mongodb.dcb.internal;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.Document;
import org.jspecify.annotations.NullMarked;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.mongodb.internal.OccurrentCloudEventMongoDocumentMapper;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;

import java.util.ArrayList;

/**
 * Maps DCB events between a {@link CloudEvent} and a MongoDB {@link Document}. It wraps the stream-only
 * {@link OccurrentCloudEventMongoDocumentMapper} and adds the DCB storage fields the stream mapper does not know about.
 * This is the single home for the DCB storage contract shared between the event store, which writes the fields, and a
 * DCB subscription, which matches a change stream against them.
 */
@NullMarked
public final class DcbDocumentMapper {

    /**
     * The name of the indexed array field that holds an event's DCB tags in the stored MongoDB document, kept alongside
     * the newline-joined {@code dcbtags} CloudEvent extension so that tag containment can be queried with {@code $all}.
     */
    public static final String DCB_TAGS_INDEX_FIELD = "dcbTags";

    private DcbDocumentMapper() {
    }

    /**
     * Converts a DCB CloudEvent to a stored document, adding the DCB position field and the indexed tags array on top of
     * the stream document the common mapper produces.
     */
    public static Document toDocument(TimeRepresentation timeRepresentation, String streamId, long streamVersion, CloudEvent dcbCloudEvent, long position) {
        Document document = OccurrentCloudEventMongoDocumentMapper.convertToDocument(timeRepresentation, streamId, streamVersion, dcbCloudEvent);
        document.put(DcbCloudEvents.POSITION, position);
        document.put(DCB_TAGS_INDEX_FIELD, new ArrayList<>(DcbCloudEvents.getTags(dcbCloudEvent)));
        return document;
    }

    /**
     * Converts a stored document back to a CloudEvent. The DCB index fields are stripped before delegating to the
     * stream mapper, and the DCB position is reattached as a CloudEvent extension. A plain stream document carries
     * neither field, so this also handles stream events and is safe as the single deserialization point for both.
     */
    public static CloudEvent toCloudEvent(TimeRepresentation timeRepresentation, Document cloudEventDocument) {
        Object dcbPosition = cloudEventDocument.get(DcbCloudEvents.POSITION);
        Document stripped = new Document(cloudEventDocument);
        stripped.remove(DCB_TAGS_INDEX_FIELD);
        stripped.remove(DcbCloudEvents.POSITION);

        CloudEvent cloudEvent = OccurrentCloudEventMongoDocumentMapper.convertToCloudEvent(timeRepresentation, stripped);
        if (dcbPosition == null) {
            return cloudEvent;
        }

        CloudEventBuilder cloudEventBuilder = CloudEventBuilder.v1(cloudEvent);
        if (dcbPosition instanceof Number number) {
            cloudEventBuilder.withExtension(DcbCloudEvents.POSITION, number.longValue());
        } else if (dcbPosition instanceof String string) {
            cloudEventBuilder.withExtension(DcbCloudEvents.POSITION, Long.parseLong(string));
        } else {
            throw new IllegalStateException("Expected " + DcbCloudEvents.POSITION + " to be a Number or String but was " + dcbPosition.getClass().getName());
        }
        return cloudEventBuilder.build();
    }
}
