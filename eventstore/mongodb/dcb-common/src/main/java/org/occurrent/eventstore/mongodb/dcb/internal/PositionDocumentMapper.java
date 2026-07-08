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
import org.jspecify.annotations.Nullable;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;

/**
 * Adds and reads back the {@value org.occurrent.cloudevents.OccurrentCloudEventExtension#POSITION} field on a stored
 * MongoDB document. Extracted so write paths that add a position to a document share one field-name and
 * type-coercion contract instead of duplicating it.
 */
@NullMarked
public final class PositionDocumentMapper {

    private PositionDocumentMapper() {
    }

    /**
     * Adds {@code position} to {@code document} using the shared position field name.
     */
    public static void addPosition(Document document, long position) {
        document.put(OccurrentCloudEventExtension.POSITION, position);
    }

    /**
     * Removes the position field from {@code document}, if present.
     */
    public static void stripPosition(Document document) {
        document.remove(OccurrentCloudEventExtension.POSITION);
    }

    /**
     * Reattaches the position value read from a stored document as a CloudEvent extension, or returns
     * {@code cloudEvent} unchanged when {@code storedPosition} is {@code null} (a plain document without a position).
     */
    public static CloudEvent reattachPosition(CloudEvent cloudEvent, @Nullable Object storedPosition) {
        if (storedPosition == null) {
            return cloudEvent;
        }
        CloudEventBuilder cloudEventBuilder = CloudEventBuilder.v1(cloudEvent);
        if (storedPosition instanceof Number number) {
            cloudEventBuilder.withExtension(OccurrentCloudEventExtension.POSITION, number.longValue());
        } else if (storedPosition instanceof String string) {
            cloudEventBuilder.withExtension(OccurrentCloudEventExtension.POSITION, Long.parseLong(string));
        } else {
            throw new IllegalStateException("Expected " + OccurrentCloudEventExtension.POSITION + " to be a Number or String but was " + storedPosition.getClass().getName());
        }
        return cloudEventBuilder.build();
    }
}
