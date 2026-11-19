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

package org.occurrent.dsl.saga.mongodb.spring;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.mongodb.core.CollectionCallback;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A real {@link MongoTemplate} that fails a named call with a {@link DataAccessResourceFailureException} a set number of
 * times before it lets calls through, and counts the calls that reached the name. The store reads through
 * {@code findOne} and {@code find}, writes through {@code execute} and creates its indexes through
 * {@code getCollection}.
 */
final class FlakyMongoTemplate extends MongoTemplate {
    private final String failingMethod;
    private final AtomicInteger remainingFailures;
    private final AtomicInteger calls = new AtomicInteger();

    /** Fails the first {@code failures} calls of {@code failingMethod}, or every call when {@code failures} is {@link Integer#MAX_VALUE} */
    FlakyMongoTemplate(MongoClient client, String database, String failingMethod, int failures) {
        super(client, database);
        this.failingMethod = failingMethod;
        this.remainingFailures = new AtomicInteger(failures);
    }

    int calls() {
        return calls.get();
    }

    private void maybeFail(String method) {
        if (method.equals(failingMethod)) {
            calls.incrementAndGet();
            if (remainingFailures.getAndDecrement() > 0) {
                throw new DataAccessResourceFailureException("simulated transient MongoDB failure in " + method);
            }
        }
    }

    @Override
    public <T> T findOne(Query query, Class<T> entityClass, String collectionName) {
        maybeFail("findOne");
        return super.findOne(query, entityClass, collectionName);
    }

    @Override
    public <T> List<T> find(Query query, Class<T> entityClass, String collectionName) {
        maybeFail("find");
        return super.find(query, entityClass, collectionName);
    }

    @Override
    public <T> T execute(String collectionName, CollectionCallback<T> callback) {
        maybeFail("execute");
        return super.execute(collectionName, callback);
    }

    @Override
    public MongoCollection<Document> getCollection(String collectionName) {
        maybeFail("getCollection");
        return super.getCollection(collectionName);
    }
}
