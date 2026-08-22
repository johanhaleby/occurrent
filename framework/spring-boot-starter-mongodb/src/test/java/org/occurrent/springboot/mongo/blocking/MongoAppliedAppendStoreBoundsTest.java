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

import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.eventstore.api.AppendId;
import org.occurrent.retry.Backoff;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.springboot.common.OccurrentProperties;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Every call this store makes gives up after a number of attempts fixed before it starts, and a configuration it can
 * never satisfy is rejected when the bean is built rather than attempted on a schedule. A projection records on the
 * thread that delivers its events, so a call that retries for as long as an outage lasts holds that delivery up for
 * as long as the outage lasts, and the clear that ADR 132 decision 7 expects to give up and stop its recorder never
 * gives up at all. A mocked {@link MongoOperations} is enough for both, no Testcontainers needed.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(10)
class MongoAppliedAppendStoreBoundsTest {

    private static final int ATTEMPTS = 3;

    @Test
    void recordApplied_gives_up_once_its_attempts_run_out_rather_than_calling_the_store_for_as_long_as_it_stays_down() {
        MongoOperations mongoOperations = mock(MongoOperations.class);
        when(mongoOperations.indexOps(anyString())).thenReturn(mock(IndexOperations.class));
        when(mongoOperations.upsert(any(Query.class), any(UpdateDefinition.class), anyString()))
                .thenThrow(new RuntimeException("store outage"));
        AppliedAppendStore store = storeWith(mongoOperations, boundedRetry());

        assertThatThrownBy(() -> store.recordApplied("orders", AppendId.mint()))
                .isInstanceOf(RuntimeException.class);

        verify(mongoOperations, times(ATTEMPTS)).upsert(any(Query.class), any(UpdateDefinition.class), anyString());
    }

    @Test
    void clear_gives_up_once_its_attempts_run_out_so_a_recorder_can_stop_itself_instead_of_recording_as_though_it_had_cleared() {
        MongoOperations mongoOperations = mock(MongoOperations.class);
        when(mongoOperations.indexOps(anyString())).thenReturn(mock(IndexOperations.class));
        doThrow(new RuntimeException("store outage")).when(mongoOperations).remove(any(Query.class), anyString());
        AppliedAppendStore store = storeWith(mongoOperations, boundedRetry());

        assertThatThrownBy(() -> store.clear("orders"))
                .isInstanceOf(RuntimeException.class);

        verify(mongoOperations, times(ATTEMPTS)).remove(any(Query.class), anyString());
    }

    @Test
    void hasApplied_gives_up_once_its_attempts_run_out_rather_than_blocking_the_caller_for_the_length_of_the_outage() {
        MongoOperations mongoOperations = mock(MongoOperations.class);
        when(mongoOperations.indexOps(anyString())).thenReturn(mock(IndexOperations.class));
        when(mongoOperations.exists(any(Query.class), anyString()))
                .thenThrow(new RuntimeException("store outage"));
        AppliedAppendStore store = storeWith(mongoOperations, boundedRetry());

        assertThatThrownBy(() -> store.hasApplied("orders", AppendId.mint()))
                .isInstanceOf(RuntimeException.class);

        verify(mongoOperations, times(ATTEMPTS)).exists(any(Query.class), anyString());
    }

    @Test
    void a_compound_index_that_already_exists_with_other_options_fails_the_call_once_instead_of_being_attempted_again() {
        MongoOperations mongoOperations = mock(MongoOperations.class);
        IndexOperations indexOperations = mock(IndexOperations.class);
        when(mongoOperations.indexOps(anyString())).thenReturn(indexOperations);
        when(indexOperations.ensureIndex(any())).thenThrow(indexOptionsConflict());
        AppliedAppendStore store = storeWith(mongoOperations, boundedRetry());

        assertThatThrownBy(() -> store.recordApplied("orders", AppendId.mint()))
                .isInstanceOf(MongoAppliedAppendStore.ConflictingIndexException.class)
                .hasMessageContaining("projectionId_appendId")
                .hasMessageContaining("Drop that index");

        verify(indexOperations, times(1)).ensureIndex(any());
    }

    @Test
    void a_conflicting_compound_index_fails_a_read_the_same_way_it_fails_a_write() {
        MongoOperations mongoOperations = mock(MongoOperations.class);
        IndexOperations indexOperations = mock(IndexOperations.class);
        when(mongoOperations.indexOps(anyString())).thenReturn(indexOperations);
        when(indexOperations.ensureIndex(any())).thenThrow(indexOptionsConflict());
        AppliedAppendStore store = storeWith(mongoOperations, boundedRetry());

        assertThatThrownBy(() -> store.hasApplied("orders", AppendId.mint()))
                .isInstanceOf(MongoAppliedAppendStore.ConflictingIndexException.class);

        verify(indexOperations, times(1)).ensureIndex(any());
    }

    @Test
    void the_constructor_rejects_a_blank_collection_instead_of_retrying_the_name_mongodb_will_never_accept() {
        MongoOperations mongoOperations = mock(MongoOperations.class);

        assertThatThrownBy(() -> storeWith(mongoOperations, boundedRetry(), "   "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("collection cannot be blank");
    }

    @Test
    void the_constructor_rejects_an_empty_collection_the_same_way_it_rejects_a_blank_one() {
        MongoOperations mongoOperations = mock(MongoOperations.class);

        assertThatThrownBy(() -> storeWith(mongoOperations, boundedRetry(), ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("collection cannot be blank");
    }

    @Test
    void the_attempt_limit_an_application_configures_defaults_to_the_one_this_store_documents() {
        OccurrentProperties.ProjectionProperties.AppliedAppendProperties appliedAppend =
                new OccurrentProperties().getProjection().getAppliedAppend();

        assertThat(appliedAppend.getMaxAttempts()).isEqualTo(MongoAppliedAppendStore.DEFAULT_MAX_ATTEMPTS);
    }

    private static RetryStrategy boundedRetry() {
        return RetryStrategy.retry().backoff(Backoff.fixed(5)).maxAttempts(ATTEMPTS);
    }

    private static AppliedAppendStore storeWith(MongoOperations mongoOperations, RetryStrategy retryStrategy) {
        return storeWith(mongoOperations, retryStrategy, "appliedAppends");
    }

    private static AppliedAppendStore storeWith(MongoOperations mongoOperations, RetryStrategy retryStrategy, String collection) {
        return new MongoAppliedAppendStore(mongoOperations, collection, Duration.ofDays(7), retryStrategy, Backoff.fixed(20));
    }

    private static UncategorizedMongoDbException indexOptionsConflict() {
        BsonDocument response = new BsonDocument("ok", new BsonInt32(0))
                .append("code", new BsonInt32(85))
                .append("codeName", new BsonString("IndexOptionsConflict"))
                .append("errmsg", new BsonString("Index already exists with different options"));
        return new UncategorizedMongoDbException("index setup failed", new MongoCommandException(response, new ServerAddress()));
    }
}
