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
import org.occurrent.dsl.view.internal.DocumentIdsKt;
import org.occurrent.dsl.view.internal.MongoBulkViewStateOperations;
import org.occurrent.springboot.blocking.DefaultProjectionStoreProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoOperations;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * Materializes a {@code @Projection} that declares no store into MongoDB, into the collection Spring Data derives from
 * the state type.
 * <p>
 * {@code save} looks up the document by the state's own {@code @Id}, not by the projection key it is handed. Give
 * the state type an {@code @Id} field that holds the same value the projection resolves as its key, or a read and a
 * write for one instance would land on two different documents and the read model would never accumulate. Since
 * 0.33.0, {@code save} and {@code saveAll} check the state's {@code @Id} against the resolved key and fail fast with
 * an {@code IllegalStateException} on a mismatch, instead of silently orphaning the read model. This store does no
 * optimistic locking unless the state type also declares {@code @Version}, so under concurrent delivery to one
 * projection key it is last write wins. Thread a {@code RetryStrategy} through
 * {@code Projections.materializedView(..)} to recover from that once the state carries {@code @Version}.
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
        // An anonymous implementation rather than ViewStateRepository.create(find, save): that lambda factory can
        // only build the two single-entry methods, and findAllById/saveAll below need a real MongoOperations handle
        // to batch into one round trip instead of looping.
        return new ViewStateRepository<S, ID>() {
            @Override
            public Optional<S> findById(ID id) {
                return Optional.ofNullable(mongoOperations.findById(id, stateType));
            }

            @Override
            public void save(ID id, S state) {
                // The document id used is whatever @Id is set on state itself, not id. See the class javadoc. Verify
                // the two agree before writing, or a mismatch would read one document and write another.
                DocumentIdsKt.requireMatchingDocumentId(mongoOperations, stateType, state, id);
                mongoOperations.save(state);
            }

            @Override
            public Map<ID, S> findAllById(Collection<ID> ids) {
                return MongoBulkViewStateOperations.findAllById(mongoOperations, stateType, ids);
            }

            @Override
            public void saveAll(Map<ID, S> states) {
                // Same as save: every state is written under its own @Id, not the map's ID keys. Verify every entry
                // before any write is issued, so a mismatched id fails the whole batch rather than the entries that
                // would have followed it in a loop.
                states.forEach((id, state) -> DocumentIdsKt.requireMatchingDocumentId(mongoOperations, stateType, state, id));
                MongoBulkViewStateOperations.saveAll(mongoOperations, stateType, new ArrayList<>(states.values()));
            }
        };
    }
}
