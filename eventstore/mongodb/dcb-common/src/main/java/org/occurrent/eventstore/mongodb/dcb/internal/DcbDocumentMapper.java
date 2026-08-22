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
import org.bson.Document;
import org.jspecify.annotations.NullMarked;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.internal.OccurrentCloudEventMongoDocumentMapper;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;

import java.util.ArrayList;
import java.util.stream.Collectors;

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
        PositionDocumentMapper.addPosition(document, position);
        document.put(DCB_TAGS_INDEX_FIELD, DcbCloudEvents.getTags(dcbCloudEvent).stream().map(Tag::canonical).collect(Collectors.toCollection(ArrayList::new)));
        return document;
    }

    /**
     * Converts a stored document back to a CloudEvent. The DCB index fields are stripped before delegating to the
     * stream mapper, and the position is reattached as a CloudEvent extension. A plain stream document carries
     * neither field, so this also handles stream events and is safe as the single deserialization point for both.
     */
    public static CloudEvent toCloudEvent(TimeRepresentation timeRepresentation, Document cloudEventDocument) {
        Object storedPosition = cloudEventDocument.get(OccurrentCloudEventExtension.POSITION);
        Document stripped = new Document(cloudEventDocument);
        stripped.remove(DCB_TAGS_INDEX_FIELD);
        PositionDocumentMapper.stripPosition(stripped);

        CloudEvent cloudEvent = OccurrentCloudEventMongoDocumentMapper.convertToCloudEvent(timeRepresentation, stripped);
        return PositionDocumentMapper.reattachPosition(cloudEvent, storedPosition);
    }

    /**
     * Reapplies the position and DCB tag index fields from {@code originalCloudEvent} onto {@code updatedDocument},
     * for an {@code updateEvent} write-back that rebuilt the document from the stream-only
     * {@link OccurrentCloudEventMongoDocumentMapper#convertToDocument}. That mapper does not know about either field:
     * it round-trips position through the general CloudEvent extension writer, which has no {@code Long} overload and
     * so coerces it to a string, and it never writes the indexed tags array at all. Both fields are store-owned, the
     * way {@link OccurrentCloudEventExtension#preserveAppendId} treats the append id, so an update reapplies them
     * from the event read before the update rather than trusting that lossy round trip. A plain stream event without
     * a position, or without DCB tags, is left as the stream-only mapper produced it.
     */
    public static void preservePositionAndDcbTags(CloudEvent originalCloudEvent, Document updatedDocument) {
        long position = OccurrentCloudEventExtension.getPosition(originalCloudEvent);
        if (position > 0) {
            PositionDocumentMapper.addPosition(updatedDocument, position);
        }
        if (DcbCloudEvents.isDcbEvent(originalCloudEvent)) {
            updatedDocument.put(DCB_TAGS_INDEX_FIELD, DcbCloudEvents.getTags(originalCloudEvent).stream().map(Tag::canonical).collect(Collectors.toCollection(ArrayList::new)));
        }
    }
}
