/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.springboot.mongo.reactor;

import org.bson.Document;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.dsl.snapshot.Snapshot;
import org.occurrent.dsl.snapshot.reactor.ReactiveSnapshotStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * A reactive {@link ReactiveSnapshotStore} backed by MongoDB, the reactive counterpart to
 * {@code SpringMongoSnapshotStore}. Each snapshot is one document in the store's collection, keyed by the snapshot key,
 * carrying the folded {@code state}, the {@code version} it was folded up to, and the {@code schemaVersion}. The state is
 * (de)serialized with the same {@code MongoConverter} the rest of the application uses, so a scalar (a number, string,
 * enum) and a POJO or record both round-trip.
 *
 * @param <S> The snapshot state type.
 */
@NullMarked
public final class ReactiveSpringMongoSnapshotStore<S extends @Nullable Object> implements ReactiveSnapshotStore<S> {

    private static final Logger log = LoggerFactory.getLogger(ReactiveSpringMongoSnapshotStore.class);

    private static final String VERSION = "version";
    private static final String SCHEMA_VERSION = "schemaVersion";
    private static final String STATE = "state";

    private final ReactiveMongoOperations mongoOperations;
    private final Class<S> stateType;
    private final String collectionName;

    /**
     * Create a reactive MongoDB-backed snapshot store.
     *
     * @param mongoOperations The {@link ReactiveMongoOperations} used to read and write snapshot documents.
     * @param stateType       The state type, needed to read the stored state back into an object.
     * @param collectionName  The collection the snapshots are stored in.
     */
    public ReactiveSpringMongoSnapshotStore(ReactiveMongoOperations mongoOperations, Class<S> stateType, String collectionName) {
        this.mongoOperations = Objects.requireNonNull(mongoOperations, "mongoOperations cannot be null");
        this.stateType = Objects.requireNonNull(stateType, "stateType cannot be null");
        this.collectionName = Objects.requireNonNull(collectionName, "collectionName cannot be null");
    }

    @Override
    public Mono<Snapshot<S>> findLatest(String key) {
        Objects.requireNonNull(key, "key cannot be null");
        return mongoOperations.findById(key, Document.class, collectionName)
                .map(document -> {
                    long version = document.getLong(VERSION);
                    int schemaVersion = document.getInteger(SCHEMA_VERSION);
                    S state = readState(mongoOperations.getConverter(), document.get(STATE));
                    return new Snapshot<>(state, version, schemaVersion);
                })
                .onErrorResume(e -> {
                    // A snapshot is a discardable optimization. If a stored snapshot can no longer be read (for example
                    // after the state shape changed), degrade to a full replay rather than failing the command.
                    log.warn("Ignoring unreadable snapshot '{}' in collection '{}', falling back to a full replay", key, collectionName, e);
                    return Mono.empty();
                });
    }

    @Override
    public Mono<Void> save(String key, Snapshot<S> snapshot) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(snapshot, "snapshot cannot be null");
        Document document = new Document("_id", key)
                .append(VERSION, snapshot.version())
                .append(SCHEMA_VERSION, snapshot.schemaVersion());
        S state = snapshot.state();
        if (state != null) {
            document.append(STATE, mongoOperations.getConverter().convertToMongoType(state));
        }
        return mongoOperations.save(document, collectionName).then();
    }

    @Override
    public Mono<Void> delete(String key) {
        Objects.requireNonNull(key, "key cannot be null");
        return mongoOperations.remove(Query.query(Criteria.where("_id").is(key)), collectionName).then();
    }

    @SuppressWarnings("unchecked")
    private @Nullable S readState(MongoConverter converter, @Nullable Object stateField) {
        if (stateField == null) {
            return null;
        }
        if (stateField instanceof Document stateDocument) {
            return converter.read(stateType, stateDocument);
        }
        // A scalar or otherwise simple state stored directly, converted back to the declared state type.
        return (S) converter.getConversionService().convert(stateField, stateType);
    }
}
