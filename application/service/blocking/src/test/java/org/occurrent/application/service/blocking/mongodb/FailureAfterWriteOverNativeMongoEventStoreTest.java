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

package org.occurrent.application.service.blocking.mongodb;

import org.junit.jupiter.api.DisplayName;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;

import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

@DisplayName("Failure after the write over MongoEventStore")
class FailureAfterWriteOverNativeMongoEventStoreTest extends MongoBackedFailureAfterWriteTest {

    @Override
    @SuppressWarnings("unchecked")
    <S extends EventStore & DcbEventStore> S eventStore() {
        EventStoreConfig config = new EventStoreConfig.Builder()
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        return (S) new MongoEventStore(mongoClient, databaseName, "events", config);
    }
}
