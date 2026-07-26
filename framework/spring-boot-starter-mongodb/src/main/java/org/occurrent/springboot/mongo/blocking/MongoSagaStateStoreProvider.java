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

import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.flow.FlowState;
import org.occurrent.dsl.saga.mongodb.spring.SpringMongoSagaStateStore;
import org.occurrent.springboot.blocking.DefaultSagaStateStoreProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoOperations;

/**
 * Stores a {@code @Saga} that declares no store in a {@code saga-<id>} MongoDB collection.
 */
class MongoSagaStateStoreProvider implements DefaultSagaStateStoreProvider {

    private final ApplicationContext applicationContext;

    MongoSagaStateStoreProvider(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    @Override
    public <S> SagaStateStore<S> createDefaultSagaStateStore(String sagaId, Class<S> stateType) {
        // Resolved on use rather than injected, so a store that is never defaulted to is never created.
        MongoOperations mongoOperations = applicationContext.getBean(MongoOperations.class);
        if (stateType == FlowState.class) {
            // A flow saga's FlowState holds domain events, serialize them as CloudEvents (stable types) so they can move packages.
            CloudEventConverter<?> converter = applicationContext.getBean(CloudEventConverter.class);
            return new SpringMongoSagaStateStore<>(mongoOperations, "saga-" + sagaId, stateType, converter);
        }
        return new SpringMongoSagaStateStore<>(mongoOperations, "saga-" + sagaId, stateType);
    }
}
