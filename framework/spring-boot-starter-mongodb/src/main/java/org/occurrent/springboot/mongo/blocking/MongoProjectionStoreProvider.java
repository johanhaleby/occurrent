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

import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.springboot.blocking.DefaultProjectionStoreProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoOperations;

/**
 * Materializes a {@code @Projection} that declares no store into MongoDB, into the collection Spring Data derives from
 * the state type.
 * <p>
 * {@code save} looks up the document by the state's own {@code @Id}, not by the projection key it is handed. The
 * projection key is used for {@code findById} only. Give the state type an {@code @Id} field that holds the same
 * value the projection resolves as its key, or a read and a write for one instance land on two different documents
 * and the read model never accumulates. This store does no optimistic locking unless the state type also declares
 * {@code @Version}, so under concurrent delivery to one projection key it is last write wins. Thread a
 * {@code RetryStrategy} through {@code Projections.materializedView(..)} to recover from that once the state carries
 * {@code @Version}.
 */
class MongoProjectionStoreProvider implements DefaultProjectionStoreProvider {

    private final ApplicationContext applicationContext;

    MongoProjectionStoreProvider(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    @Override
    public <S, ID> ViewStateRepository<S, ID> createDefaultProjectionStore(String projectionId, Class<S> stateType) {
        // Resolved on use rather than injected, so a store that is never defaulted to is never created.
        MongoOperations mongoOperations = applicationContext.getBean(MongoOperations.class);
        return ViewStateRepository.create(
                instanceId -> mongoOperations.findById(instanceId, stateType),
                // instanceId is not passed to save: the document id used is whatever @Id is set on state itself. See
                // the class javadoc.
                (instanceId, state) -> mongoOperations.save(state));
    }
}
