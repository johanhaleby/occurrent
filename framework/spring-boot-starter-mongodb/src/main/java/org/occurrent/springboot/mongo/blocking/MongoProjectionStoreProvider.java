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
                (instanceId, state) -> mongoOperations.save(state));
    }
}
