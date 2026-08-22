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

import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
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
import org.occurrent.springboot.common.OccurrentProperties;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.index.ReactiveIndexOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The reactive mirror of {@code MongoAppliedAppendStoreBoundsTest}. Every call gives up after a number of attempts
 * fixed before it starts, and a configuration this store can never satisfy is rejected when the bean is built.
 * Reactor resubscribes a {@code Mono} that was built once, so the number of attempts is counted by subscription
 * rather than by how often the mocked {@link ReactiveMongoOperations} method was called.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(10)
class ReactiveMongoAppliedAppendStoreBoundsTest {

    private static final int ATTEMPTS = 3;

    @Test
    void recordApplied_gives_up_once_its_attempts_run_out_rather_than_calling_the_store_for_as_long_as_it_stays_down() {
        AtomicInteger attempts = new AtomicInteger();
        ReactiveMongoOperations mongoOperations = mongoOperationsWithWorkingIndexes();
        when(mongoOperations.upsert(any(Query.class), any(UpdateDefinition.class), anyString()))
                .thenReturn(failingAfterCounting(attempts));
        AppliedAppendStore store = storeWith(mongoOperations, boundedRetry());

        assertThatThrownBy(() -> store.recordApplied("orders", AppendId.mint()))
                .isInstanceOf(RuntimeException.class);

        assertThat(attempts).hasValue(ATTEMPTS);
    }

    @Test
    void clear_gives_up_once_its_attempts_run_out_so_a_recorder_can_stop_itself_instead_of_recording_as_though_it_had_cleared() {
        AtomicInteger attempts = new AtomicInteger();
        ReactiveMongoOperations mongoOperations = mongoOperationsWithWorkingIndexes();
        when(mongoOperations.remove(any(Query.class), anyString()))
                .thenReturn(ReactiveMongoAppliedAppendStoreBoundsTest.<DeleteResult>failingAfterCounting(attempts));
        AppliedAppendStore store = storeWith(mongoOperations, boundedRetry());

        assertThatThrownBy(() -> store.clear("orders"))
                .isInstanceOf(RuntimeException.class);

        assertThat(attempts).hasValue(ATTEMPTS);
    }

    @Test
    void hasApplied_gives_up_once_its_attempts_run_out_rather_than_blocking_the_caller_for_the_length_of_the_outage() {
        AtomicInteger attempts = new AtomicInteger();
        ReactiveMongoOperations mongoOperations = mongoOperationsWithWorkingIndexes();
        when(mongoOperations.exists(any(Query.class), anyString()))
                .thenReturn(ReactiveMongoAppliedAppendStoreBoundsTest.<Boolean>failingAfterCounting(attempts));
        AppliedAppendStore store = storeWith(mongoOperations, boundedRetry());

        assertThatThrownBy(() -> store.hasApplied("orders", AppendId.mint()))
                .isInstanceOf(RuntimeException.class);

        assertThat(attempts).hasValue(ATTEMPTS);
    }

    @Test
    void a_compound_index_that_already_exists_with_other_options_fails_the_call_once_instead_of_being_attempted_again() {
        AtomicInteger indexAttempts = new AtomicInteger();
        ReactiveMongoOperations mongoOperations = mongoOperationsWhoseIndexSetupConflicts(indexAttempts);
        when(mongoOperations.upsert(any(Query.class), any(UpdateDefinition.class), anyString()))
                .thenReturn(Mono.just(mock(UpdateResult.class)));
        // The store's own shipped retry, which would attempt the index DEFAULT_MAX_ATTEMPTS times if it repeated
        // error 85 at all.
        AppliedAppendStore store = storeWith(mongoOperations, ReactiveMongoAppliedAppendStore.defaultRetry());

        assertThatThrownBy(() -> store.recordApplied("orders", AppendId.mint()))
                .isInstanceOf(ReactiveMongoAppliedAppendStore.ConflictingIndexException.class)
                .hasMessageContaining("projectionId_appendId")
                .hasMessageContaining("Drop that index");

        assertThat(indexAttempts).hasValue(1);
    }

    @Test
    void a_conflicting_compound_index_fails_a_read_the_same_way_it_fails_a_write() {
        AtomicInteger indexAttempts = new AtomicInteger();
        ReactiveMongoOperations mongoOperations = mongoOperationsWhoseIndexSetupConflicts(indexAttempts);
        when(mongoOperations.exists(any(Query.class), anyString())).thenReturn(Mono.just(true));
        AppliedAppendStore store = storeWith(mongoOperations, ReactiveMongoAppliedAppendStore.defaultRetry());

        assertThatThrownBy(() -> store.hasApplied("orders", AppendId.mint()))
                .isInstanceOf(ReactiveMongoAppliedAppendStore.ConflictingIndexException.class);

        assertThat(indexAttempts).hasValue(1);
    }

    @Test
    void the_constructor_rejects_a_blank_collection_instead_of_retrying_the_name_mongodb_will_never_accept() {
        ReactiveMongoOperations mongoOperations = mock(ReactiveMongoOperations.class);

        assertThatThrownBy(() -> storeWith(mongoOperations, boundedRetry(), "   "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("collection cannot be blank");
    }

    @Test
    void the_constructor_rejects_an_empty_collection_the_same_way_it_rejects_a_blank_one() {
        ReactiveMongoOperations mongoOperations = mock(ReactiveMongoOperations.class);

        assertThatThrownBy(() -> storeWith(mongoOperations, boundedRetry(), ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("collection cannot be blank");
    }

    /**
     * Runs the retry this store ships with, only with its backoff shortened, so the number of calls it makes to an
     * unreachable store is the shipped number rather than one the test chose. Left with no limit, as this default
     * once was, the call never returns and the class timeout is what ends it.
     */
    @Test
    void the_shipped_default_stops_calling_an_unreachable_store_after_the_number_of_attempts_it_documents() {
        AtomicInteger attempts = new AtomicInteger();
        ReactiveMongoOperations mongoOperations = mongoOperationsWithWorkingIndexes();
        when(mongoOperations.upsert(any(Query.class), any(UpdateDefinition.class), anyString()))
                .thenReturn(failingAfterCounting(attempts));
        RetryBackoffSpec shippedLimit = ((RetryBackoffSpec) ReactiveMongoAppliedAppendStore.defaultRetry())
                .minBackoff(Duration.ofMillis(1))
                .maxBackoff(Duration.ofMillis(2));
        AppliedAppendStore store = storeWith(mongoOperations, shippedLimit);

        assertThatThrownBy(() -> store.recordApplied("orders", AppendId.mint()))
                .isInstanceOf(RuntimeException.class);

        assertThat(attempts).hasValue(ReactiveMongoAppliedAppendStore.DEFAULT_MAX_ATTEMPTS);
    }

    @Test
    void the_attempt_limit_an_application_configures_defaults_to_the_one_this_store_documents() {
        OccurrentProperties.ProjectionProperties.AppliedAppendProperties appliedAppend =
                new OccurrentProperties().getProjection().getAppliedAppend();

        assertThat(appliedAppend.getMaxAttempts()).isEqualTo(ReactiveMongoAppliedAppendStore.DEFAULT_MAX_ATTEMPTS);
    }

    private static <T> Mono<T> failingAfterCounting(AtomicInteger attempts) {
        return Mono.defer(() -> {
            attempts.incrementAndGet();
            return Mono.error(new RuntimeException("store outage"));
        });
    }

    /**
     * Counts subscriptions rather than calls to {@code ensureIndex}, because the chain builds both index operations
     * before it subscribes to either, so the mocked method runs twice for one attempt at creating the index.
     */
    private static ReactiveMongoOperations mongoOperationsWhoseIndexSetupConflicts(AtomicInteger indexAttempts) {
        ReactiveMongoOperations mongoOperations = mock(ReactiveMongoOperations.class);
        ReactiveIndexOperations indexOperations = mock(ReactiveIndexOperations.class);
        when(mongoOperations.indexOps(anyString())).thenReturn(indexOperations);
        when(indexOperations.ensureIndex(any())).thenReturn(Mono.defer(() -> {
            indexAttempts.incrementAndGet();
            return Mono.error(indexOptionsConflict());
        }));
        return mongoOperations;
    }

    private static ReactiveMongoOperations mongoOperationsWithWorkingIndexes() {
        ReactiveMongoOperations mongoOperations = mock(ReactiveMongoOperations.class);
        ReactiveIndexOperations indexOperations = mock(ReactiveIndexOperations.class);
        when(mongoOperations.indexOps(anyString())).thenReturn(indexOperations);
        when(indexOperations.ensureIndex(any())).thenReturn(Mono.just("index"));
        return mongoOperations;
    }

    private static Retry boundedRetry() {
        return Retry.fixedDelay(ATTEMPTS - 1L, Duration.ofMillis(5))
                .filter(e -> !(e instanceof ReactiveMongoAppliedAppendStore.ConflictingIndexException))
                .onRetryExhaustedThrow((spec, signal) -> signal.failure());
    }

    private static AppliedAppendStore storeWith(ReactiveMongoOperations mongoOperations, Retry retry) {
        return storeWith(mongoOperations, retry, "appliedAppends");
    }

    private static AppliedAppendStore storeWith(ReactiveMongoOperations mongoOperations, Retry retry, String collection) {
        return new ReactiveMongoAppliedAppendStore(mongoOperations, collection, Duration.ofDays(7), retry, Backoff.fixed(20));
    }

    private static UncategorizedMongoDbException indexOptionsConflict() {
        BsonDocument response = new BsonDocument("ok", new BsonInt32(0))
                .append("code", new BsonInt32(85))
                .append("codeName", new BsonString("IndexOptionsConflict"))
                .append("errmsg", new BsonString("Index already exists with different options"));
        return new UncategorizedMongoDbException("index setup failed", new MongoCommandException(response, new ServerAddress()));
    }
}
