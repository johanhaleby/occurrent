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

import com.mongodb.ReadPreference;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;

/**
 * A secondary that lags behind the primary cannot be made to happen on a test container, and a client on a container's
 * replica set url connects directly to the one node, which ignores the read preference. So these tests look at the query
 * the store hands to {@link MongoOperations}, which is where the store chooses the primary. They do not show that the
 * driver routes it there.
 */
@DisplayName("The reads of a saga instance that a hold depends on")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoSagaStateStoreReadPreferenceTest {

    private static final String COLLECTION = "saga-read-preference";
    private static final String SAGA_ID = "order-1";

    private final MongoOperations mongoOperations = mock(MongoOperations.class);
    private final SpringMongoSagaStateStore<String> store = storeOver(mongoOperations);

    // The store creates its indexes when it is constructed, so the collections it asks for accept that.
    @SuppressWarnings("unchecked")
    private static SpringMongoSagaStateStore<String> storeOver(MongoOperations mongoOperations) {
        given(mongoOperations.getCollection(anyString())).willReturn(mock(MongoCollection.class));
        return new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, String.class);
    }

    private Query queryOfTheRead() {
        ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
        then(mongoOperations).should().findOne(query.capture(), eq(Document.class), eq(COLLECTION));
        return query.getValue();
    }

    @Nested
    @DisplayName("when a hold has found the instance started")
    class WhenAHoldHasFoundTheInstanceStarted {

        @Test
        void find_asks_the_primary_for_the_whole_instance_so_the_drain_sees_the_start_s_save_and_the_hold_s_mark() {
            // When
            store.find(SAGA_ID);

            // Then
            Query query = queryOfTheRead();
            assertAll(
                    () -> assertThat(query.getReadPreference()).isEqualTo(ReadPreference.primary()),
                    () -> assertThat(query.getQueryObject()).isEqualTo(new Document("_id", SAGA_ID)));
        }

        @Test
        void findWithoutState_asks_the_primary_for_the_instance_so_the_hold_sees_the_start_s_save() {
            // When
            store.findWithoutState(SAGA_ID);

            // Then
            Query query = queryOfTheRead();
            assertAll(
                    () -> assertThat(query.getReadPreference()).isEqualTo(ReadPreference.primary()),
                    () -> assertThat(query.getQueryObject()).isEqualTo(new Document("_id", SAGA_ID)));
        }
    }
}
