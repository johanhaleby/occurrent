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

package org.occurrent.subscription.mongodb.internal;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import io.cloudevents.CloudEvent;
import org.bson.Document;
import org.occurrent.eventstore.mongodb.internal.OccurrentCloudEventMongoDocumentMapper;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;

import java.util.Optional;

import static com.mongodb.client.model.changestream.OperationType.INSERT;

public class MongoCloudEventsToJsonDeserializer {

    public static final String ID = "_id";

    public static Optional<CloudEvent> deserializeToCloudEvent(ChangeStreamDocument<Document> changeStreamDocument, TimeRepresentation timeRepresentation) {
        return changeStreamDocumentToCloudEventAsJson(changeStreamDocument)
                .map(document -> OccurrentCloudEventMongoDocumentMapper.convertToCloudEvent(timeRepresentation, document));
    }

    private static Optional<Document> changeStreamDocumentToCloudEventAsJson(ChangeStreamDocument<Document> changeStreamDocument) {
        final Document eventsAsJson;
        OperationType operationType = changeStreamDocument.getOperationType();
        if (operationType == INSERT) {
            eventsAsJson = changeStreamDocument.getFullDocument();
        } else {
            eventsAsJson = null;
        }

        return Optional.ofNullable(eventsAsJson);
    }

}