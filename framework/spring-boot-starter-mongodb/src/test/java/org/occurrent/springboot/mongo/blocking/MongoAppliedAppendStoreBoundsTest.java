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
import java.time.Instant;

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

    /**
     * Runs the retry this store ships with, only with its backoff swapped for a fast one, so the number of calls it
     * makes to an unreachable store is the shipped number rather than one the test chose. Left with no limit, as
     * this default once was, the call never returns and the class timeout is what ends it.
     */
    @Test
    void the_shipped_default_stops_calling_an_unreachable_store_after_the_number_of_attempts_it_documents() {
        MongoOperations mongoOperations = mock(MongoOperations.class);
        when(mongoOperations.indexOps(anyString())).thenReturn(mock(IndexOperations.class));
        when(mongoOperations.upsert(any(Query.class), any(UpdateDefinition.class), anyString()))
                .thenThrow(new RuntimeException("store outage"));
        RetryStrategy shippedLimit = MongoAppliedAppendStore.defaultRetryStrategy().backoff(Backoff.fixed(1));
        AppliedAppendStore store = storeWith(mongoOperations, shippedLimit);

        assertThatThrownBy(() -> store.recordApplied("orders", AppendId.mint()))
                .isInstanceOf(RuntimeException.class);

        verify(mongoOperations, times(MongoAppliedAppendStore.DEFAULT_MAX_ATTEMPTS))
                .upsert(any(Query.class), any(UpdateDefinition.class), anyString());
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

    /**
     * The invariant, stated as a test. A wait polls, so if the store re-attempted an index it can never create on
     * every poll, the number of calls it makes would be a function of the caller's timeout. It attempts it once per
     * process instead, and the wait still runs to its own deadline, which is what ADR 132 decision 5 requires of a
     * store that cannot be read.
     */
    @Test
    void a_wait_attempts_a_conflicting_index_once_however_long_the_caller_waits() {
        MongoOperations mongoOperations = mock(MongoOperations.class);
        IndexOperations indexOperations = mock(IndexOperations.class);
        when(mongoOperations.indexOps(anyString())).thenReturn(indexOperations);
        when(indexOperations.ensureIndex(any())).thenThrow(indexOptionsConflict());
        AppliedAppendStore store = storeWith(mongoOperations, boundedRetry());
        Duration timeout = Duration.ofMillis(400);

        Instant start = Instant.now();
        boolean applied = store.waitUntilApplied("orders", AppendId.mint(), timeout, Backoff.fixed(20));
        Duration elapsed = Duration.between(start, Instant.now());

        assertThat(applied).isFalse();
        assertThat(elapsed).isGreaterThanOrEqualTo(timeout.minusMillis(50));
        verify(indexOperations, times(1)).ensureIndex(any());
    }

    @Test
    void a_call_after_a_conflicting_index_fails_without_calling_the_store_again() {
        MongoOperations mongoOperations = mock(MongoOperations.class);
        IndexOperations indexOperations = mock(IndexOperations.class);
        when(mongoOperations.indexOps(anyString())).thenReturn(indexOperations);
        when(indexOperations.ensureIndex(any())).thenThrow(indexOptionsConflict());
        AppliedAppendStore store = storeWith(mongoOperations, boundedRetry());

        assertThatThrownBy(() -> store.recordApplied("orders", AppendId.mint()))
                .isInstanceOf(MongoAppliedAppendStore.ConflictingIndexException.class);
        assertThatThrownBy(() -> store.hasApplied("orders", AppendId.mint()))
                .isInstanceOf(MongoAppliedAppendStore.ConflictingIndexException.class);
        assertThatThrownBy(() -> store.clear("orders"))
                .isInstanceOf(MongoAppliedAppendStore.ConflictingIndexException.class);

        verify(indexOperations, times(1)).ensureIndex(any());
    }

    /**
     * A timeout that has already run out must not turn a true answer into a false one. The interface default reads
     * once before it looks at the deadline, so a store that skipped the read entirely disagreed with the interface
     * it implements, and told a caller that an append it holds is not applied.
     */
    @Test
    void a_wait_whose_timeout_has_already_elapsed_still_answers_that_an_applied_append_is_applied() {
        MongoOperations mongoOperations = mock(MongoOperations.class);
        when(mongoOperations.indexOps(anyString())).thenReturn(mock(IndexOperations.class));
        when(mongoOperations.exists(any(Query.class), anyString())).thenReturn(true);
        AppliedAppendStore store = storeWith(mongoOperations, boundedRetry());

        assertThat(store.waitUntilApplied("orders", AppendId.mint(), Duration.ZERO)).isTrue();
        assertThat(store.waitUntilApplied("orders", AppendId.mint(), Duration.ofSeconds(-1))).isTrue();

        verify(mongoOperations, times(2)).exists(any(Query.class), anyString());
    }

    @Test
    void a_wait_whose_timeout_has_already_elapsed_reads_once_and_gives_up_when_the_append_is_not_applied() {
        MongoOperations mongoOperations = mock(MongoOperations.class);
        when(mongoOperations.indexOps(anyString())).thenReturn(mock(IndexOperations.class));
        when(mongoOperations.exists(any(Query.class), anyString())).thenReturn(false);
        AppliedAppendStore store = storeWith(mongoOperations, boundedRetry());

        assertThat(store.waitUntilApplied("orders", AppendId.mint(), Duration.ZERO)).isFalse();

        verify(mongoOperations, times(1)).exists(any(Query.class), anyString());
    }

    /**
     * A caller can build a `RetryStrategy` that never gives up, and no public API on it reports that, so the store
     * cannot reject one at construction the way it rejects a blank collection. It stops the call itself instead, so
     * the number of times it reaches MongoDB is still decided before the call starts rather than by how long the
     * outage lasts.
     */
    @Test
    void a_retry_policy_that_never_gives_up_is_still_stopped_by_the_store() {
        MongoOperations mongoOperations = mock(MongoOperations.class);
        when(mongoOperations.indexOps(anyString())).thenReturn(mock(IndexOperations.class));
        when(mongoOperations.upsert(any(Query.class), any(UpdateDefinition.class), anyString()))
                .thenThrow(new RuntimeException("store outage"));
        RetryStrategy neverGivesUp = RetryStrategy.retry().backoff(Backoff.fixed(1));
        AppliedAppendStore store = storeWith(mongoOperations, neverGivesUp);

        assertThatThrownBy(() -> store.recordApplied("orders", AppendId.mint()))
                .isInstanceOf(RuntimeException.class);

        verify(mongoOperations, times(MongoAppliedAppendStore.MAX_ATTEMPTS_CEILING + 1))
                .upsert(any(Query.class), any(UpdateDefinition.class), anyString());
    }

    @Test
    void the_ceiling_never_shortens_a_policy_the_caller_actually_chose() {
        MongoOperations mongoOperations = mock(MongoOperations.class);
        when(mongoOperations.indexOps(anyString())).thenReturn(mock(IndexOperations.class));
        when(mongoOperations.exists(any(Query.class), anyString()))
                .thenThrow(new RuntimeException("store outage"));
        AppliedAppendStore store = storeWith(mongoOperations, boundedRetry());

        assertThatThrownBy(() -> store.hasApplied("orders", AppendId.mint()))
                .isInstanceOf(RuntimeException.class);

        assertThat(ATTEMPTS).isLessThan(MongoAppliedAppendStore.MAX_ATTEMPTS_CEILING);
        verify(mongoOperations, times(ATTEMPTS)).exists(any(Query.class), anyString());
    }

    /**
     * The blocking mirror of the reactive already-elapsed case, so the two stacks are held to the same number.
     */
    @Test
    void a_failing_read_under_an_already_elapsed_timeout_is_attempted_once_and_does_not_spend_the_retry_budget() {
        MongoOperations mongoOperations = mock(MongoOperations.class);
        when(mongoOperations.indexOps(anyString())).thenReturn(mock(IndexOperations.class));
        when(mongoOperations.exists(any(Query.class), anyString()))
                .thenThrow(new RuntimeException("store outage"));
        AppliedAppendStore store = new MongoAppliedAppendStore(mongoOperations, "appliedAppends", Duration.ofDays(7),
                MongoAppliedAppendStore.defaultRetryStrategy(), Backoff.fixed(20));

        Instant start = Instant.now();
        boolean applied = store.waitUntilApplied("orders", AppendId.mint(), Duration.ZERO);
        Duration elapsed = Duration.between(start, Instant.now());

        assertThat(applied).isFalse();
        verify(mongoOperations, times(1)).exists(any(Query.class), anyString());
        assertThat(elapsed).isLessThan(Duration.ofSeconds(2));
    }

    /**
     * The other half of the asymmetry. This store cannot cancel a read it has started, so a read slower than the
     * time left runs to the end and the wait answers true after its timeout has passed. The reactive store, given
     * the same delay, answers false on time instead.
     */
    @Test
    void a_read_slower_than_the_time_left_makes_this_store_overrun_rather_than_answer_false() {
        MongoOperations mongoOperations = mock(MongoOperations.class);
        when(mongoOperations.indexOps(anyString())).thenReturn(mock(IndexOperations.class));
        when(mongoOperations.exists(any(Query.class), anyString())).thenAnswer(invocation -> {
            Thread.sleep(300);
            return true;
        });
        AppliedAppendStore store = storeWith(mongoOperations, boundedRetry());

        Instant start = Instant.now();
        boolean applied = store.waitUntilApplied("orders", AppendId.mint(), Duration.ofMillis(100), Backoff.fixed(20));

        assertThat(applied).isTrue();
        assertThat(Duration.between(start, Instant.now())).isGreaterThanOrEqualTo(Duration.ofMillis(250));
    }

    @Test
    void the_largest_attempt_limit_an_application_can_configure_is_the_one_this_store_will_actually_make() {
        assertThat(OccurrentProperties.ProjectionProperties.AppliedAppendProperties.MAX_ATTEMPTS_CEILING)
                .isEqualTo(MongoAppliedAppendStore.MAX_ATTEMPTS_CEILING);
    }

    /**
     * A policy that stops at exactly the ceiling stops on its own terms. The store's guard is for a policy that
     * does not stop, so at the boundary it must not be what ends the call.
     */
    @Test
    void a_policy_that_stops_at_the_ceiling_ends_the_call_itself() {
        MongoOperations mongoOperations = mock(MongoOperations.class);
        when(mongoOperations.indexOps(anyString())).thenReturn(mock(IndexOperations.class));
        when(mongoOperations.upsert(any(Query.class), any(UpdateDefinition.class), anyString()))
                .thenThrow(new RuntimeException("store outage"));
        RetryStrategy stopsExactlyAtTheCeiling = RetryStrategy.retry()
                .backoff(Backoff.fixed(1))
                .maxAttempts(MongoAppliedAppendStore.MAX_ATTEMPTS_CEILING);
        AppliedAppendStore store = storeWith(mongoOperations, stopsExactlyAtTheCeiling);

        assertThatThrownBy(() -> store.recordApplied("orders", AppendId.mint()))
                .isInstanceOf(RuntimeException.class);

        verify(mongoOperations, times(MongoAppliedAppendStore.MAX_ATTEMPTS_CEILING))
                .upsert(any(Query.class), any(UpdateDefinition.class), anyString());
    }
}
