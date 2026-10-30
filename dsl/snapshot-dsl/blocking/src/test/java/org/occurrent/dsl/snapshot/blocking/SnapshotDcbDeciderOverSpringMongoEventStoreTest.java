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

package org.occurrent.dsl.snapshot.blocking;

import org.junit.jupiter.api.DisplayName;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;


import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

@DisplayName("SnapshotDcbDeciderApplicationService over SpringMongoEventStore")
class SnapshotDcbDeciderOverSpringMongoEventStoreTest extends MongoBackedSnapshotDcbDeciderTest {

    private MongoTransactionManager transactionManager;
    private MongoTemplate template;

    @Override
    DcbEventStore eventStore() {
        SimpleMongoClientDatabaseFactory databaseFactory = new SimpleMongoClientDatabaseFactory(mongoClient, databaseName);
        transactionManager = new MongoTransactionManager(databaseFactory);
        template = new MongoTemplate(databaseFactory);
        return storeOver(transactionManager);
    }

    @Override
    DcbEventStore holdingEventStore(Runnable hold) {
        return storeOver(new HoldingTransactionTemplate(transactionManager, hold));
    }

    @Override
    void inCallerTransactionRolledBack(Runnable body) {
        new TransactionTemplate(transactionManager).executeWithoutResult(status -> {
            try {
                body.run();
            } finally {
                status.setRollbackOnly();
            }
        });
    }

    private SpringMongoEventStore storeOver(MongoTransactionManager manager) {
        return new SpringMongoEventStore(template, config().transactionConfig(manager).build());
    }

    private SpringMongoEventStore storeOver(TransactionTemplate transactionTemplate) {
        return new SpringMongoEventStore(template, config().transactionConfig(transactionTemplate).build());
    }

    private static EventStoreConfig.Builder config() {
        return new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB);
    }

    private static final class HoldingTransactionTemplate extends TransactionTemplate {

        private final transient Runnable hold;

        HoldingTransactionTemplate(MongoTransactionManager transactionManager, Runnable hold) {
            super(transactionManager);
            this.hold = hold;
        }

        @Override
        public <T> T execute(TransactionCallback<T> action) {
            hold.run();
            return super.execute(action);
        }
    }
}
