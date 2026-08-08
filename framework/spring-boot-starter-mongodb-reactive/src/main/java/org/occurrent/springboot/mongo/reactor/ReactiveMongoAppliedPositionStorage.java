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

package org.occurrent.springboot.mongo.reactor;

import org.bson.Document;
import org.jspecify.annotations.NullMarked;
import org.occurrent.dsl.projection.AppliedPositionStorage;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.query.Update;

import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

/**
 * The {@link AppliedPositionStorage} the reactive Mongo starter contributes as
 * {@code @Projection(recordAppliedPosition = true)}'s zero-config default. {@link AppliedPositionStorage} is a
 * blocking-shaped interface on both stacks, called from the reactor recorder's {@code doOnSuccess} callback, which
 * already runs on {@code boundedElastic}, so blocking on the underlying reactive Mongo call here is the same bridge
 * the rest of this reactor stack makes in the other direction.
 * <p>
 * One document per projection id, {@code _id} the projection id and {@code position} the applied position.
 * {@link #advance(String, long)} writes with MongoDB's {@code $max} update operator in one round trip, so the
 * never-moves-backwards guarantee holds under concurrent advances for the same projection id, with no
 * read-modify-write race.
 */
@NullMarked
class ReactiveMongoAppliedPositionStorage implements AppliedPositionStorage {

    private static final String ID = "_id";
    private static final String POSITION = "position";

    private final ReactiveMongoOperations mongoOperations;
    private final String collection;

    ReactiveMongoAppliedPositionStorage(ReactiveMongoOperations mongoOperations, String collection) {
        this.mongoOperations = requireNonNull(mongoOperations, "mongoOperations cannot be null");
        this.collection = requireNonNull(collection, "collection cannot be null");
    }

    @Override
    public OptionalLong appliedPosition(String projectionId) {
        requireNonNull(projectionId, "projectionId cannot be null");
        Document document = mongoOperations.findOne(query(where(ID).is(projectionId)), Document.class, collection).block();
        if (document == null) {
            return OptionalLong.empty();
        }
        Number position = document.get(POSITION, Number.class);
        return position == null ? OptionalLong.empty() : OptionalLong.of(position.longValue());
    }

    @Override
    public void advance(String projectionId, long position) {
        requireNonNull(projectionId, "projectionId cannot be null");
        if (position <= 0) {
            throw new IllegalArgumentException("position must be positive but was " + position);
        }
        mongoOperations.upsert(query(where(ID).is(projectionId)), new Update().max(POSITION, position), collection).block();
    }
}
