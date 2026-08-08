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

import org.bson.Document;
import org.jspecify.annotations.NullMarked;
import org.occurrent.dsl.projection.AppliedPositionStore;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Update;

import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

/**
 * The {@link AppliedPositionStore} the Mongo starter contributes as {@code @Projection(recordAppliedPosition = true)}'s
 * zero-config default. One document per projection id, {@code _id} the projection id and {@code position} the applied
 * position.
 * <p>
 * {@link #advance(String, long)} writes with MongoDB's {@code $max} update operator in one round trip, so the
 * never-moves-backwards guarantee {@link AppliedPositionStore#advance(String, long)} makes holds even under
 * concurrent advances for the same projection id, with no read-modify-write race.
 */
@NullMarked
class MongoAppliedPositionStore implements AppliedPositionStore {

    private static final String ID = "_id";
    private static final String POSITION = "position";

    private final MongoOperations mongoOperations;
    private final String collection;

    MongoAppliedPositionStore(MongoOperations mongoOperations, String collection) {
        this.mongoOperations = requireNonNull(mongoOperations, "mongoOperations cannot be null");
        this.collection = requireNonNull(collection, "collection cannot be null");
    }

    @Override
    public OptionalLong appliedPosition(String projectionId) {
        requireNonNull(projectionId, "projectionId cannot be null");
        Document document = mongoOperations.findOne(query(where(ID).is(projectionId)), Document.class, collection);
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
        mongoOperations.upsert(query(where(ID).is(projectionId)), new Update().max(POSITION, position), collection);
    }
}
