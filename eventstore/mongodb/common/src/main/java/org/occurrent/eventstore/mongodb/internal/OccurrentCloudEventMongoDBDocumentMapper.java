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

package org.occurrent.eventstore.mongodb.internal;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.Document;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.mongodb.cloudevent.DocumentCloudEventReader;
import org.occurrent.eventstore.mongodb.cloudevent.DocumentCloudEventWriter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;

import java.time.OffsetDateTime;
import java.util.Date;

import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MILLIS;
import static org.occurrent.mongodb.timerepresentation.TimeRepresentation.DATE;
import static org.occurrent.mongodb.timerepresentation.TimeRepresentation.RFC_3339_STRING;
import static org.occurrent.time.internal.RFC3339.RFC_3339_DATE_TIME_FORMATTER;

/**
 * Class responsible for converting a {@link CloudEvent} (that contains the Occurrent extensions)
 * into a MongoDB {@link Document} and vice versa.
 */
public class OccurrentCloudEventMongoDBDocumentMapper {

    public static Document convertToDocument(TimeRepresentation timeRepresentation, String streamId, long streamVersion, CloudEvent cloudEvent) {
        Document cloudEventDocument = DocumentCloudEventWriter.toDocument(cloudEvent);
        cloudEventDocument.put(OccurrentCloudEventExtension.STREAM_ID, streamId);
        cloudEventDocument.put(OccurrentCloudEventExtension.STREAM_VERSION, streamVersion);

        if (timeRepresentation == DATE && cloudEvent.getTime() != null) {
            OffsetDateTime time = cloudEvent.getTime();
            if (!time.truncatedTo(MILLIS).equals(time)) {
                throw new IllegalArgumentException("The " + OffsetDateTime.class.getSimpleName() + " in the CloudEvent time field contains micro-/nanoseconds. " +
                        "This is is not possible to represent when using " + TimeRepresentation.class.getSimpleName() + " " + DATE.name() +
                        ", either change to " + TimeRepresentation.class.getSimpleName() + " " + RFC_3339_STRING.name() +
                        " or remove micro-/nanoseconds using \"offsetDateTime.truncatedTo(ChronoUnit.MILLIS)\".");
            } else if (!time.equals(time.withOffsetSameInstant(UTC))) {
                throw new IllegalArgumentException("The " + OffsetDateTime.class.getSimpleName() + " in the CloudEvent time field is not defined in UTC. " +
                        TimeRepresentation.class.getSimpleName() + " " + DATE.name() + " require UTC as timezone to not loose precision. " +
                        "Either change to " + TimeRepresentation.class.getSimpleName() + " " + RFC_3339_STRING.name() +
                        " or convert the " + OffsetDateTime.class.getSimpleName() + " to UTC using e.g. \"offsetDateTime.withOffsetSameInstant(ZoneOffset.UTC)\".");
            }

            // Convert date string to a date in order to be able to perform date/time queries on the "time" property name
            Date date = Date.from(time.toInstant());
            cloudEventDocument.put("time", date);
        }

        return cloudEventDocument;
    }

    public static CloudEvent convertToCloudEvent(TimeRepresentation timeRepresentation, Document cloudEventDocument) {
        Document document = new Document(cloudEventDocument);
        document.remove("_id");

        if (timeRepresentation == DATE) {
            Object time = document.get("time"); // Be a bit nice and don't enforce Date here if TimeRepresentation has been changed
            if (time instanceof Date) {
                Date timeAsDate = (Date) time;
                OffsetDateTime offsetDateTime = OffsetDateTime.ofInstant(timeAsDate.toInstant(), UTC);
                String format = RFC_3339_DATE_TIME_FORMATTER.format(offsetDateTime);
                document.put("time", format);
            }
        }

        CloudEvent cloudEvent = DocumentCloudEventReader.toCloudEvent(document);
        // When converting to JSON (document.toJson()) the stream version is interpreted as an int in Jackson, we convert it manually to long afterwards.
        return CloudEventBuilder.v1(cloudEvent).withExtension(OccurrentCloudEventExtension.STREAM_VERSION, document.getLong(OccurrentCloudEventExtension.STREAM_VERSION)).build();
    }
}