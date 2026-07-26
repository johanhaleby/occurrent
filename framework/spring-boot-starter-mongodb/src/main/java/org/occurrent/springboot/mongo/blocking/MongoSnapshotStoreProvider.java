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

package org.occurrent.springboot.mongo.blocking;

import org.occurrent.dsl.snapshot.blocking.SnapshotStore;
import org.occurrent.dsl.snapshot.mongodb.spring.blocking.SpringMongoSnapshotStore;
import org.occurrent.springboot.blocking.DefaultSnapshotStoreProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoOperations;

/**
 * Stores a {@code @Snapshot} that declares no store in an {@code occurrent-snapshot-<id>} MongoDB collection.
 */
class MongoSnapshotStoreProvider implements DefaultSnapshotStoreProvider {

    private final ApplicationContext applicationContext;

    MongoSnapshotStoreProvider(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    @Override
    public <S> SnapshotStore<S> createDefaultSnapshotStore(String snapshotId, Class<S> stateType) {
        // Resolved on use rather than injected, so a store that is never defaulted to is never created.
        MongoOperations mongoOperations = applicationContext.getBean(MongoOperations.class);
        return new SpringMongoSnapshotStore<>(mongoOperations, stateType, "occurrent-snapshot-" + snapshotId);
    }
}
