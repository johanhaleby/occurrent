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

import com.mongodb.client.result.UpdateResult;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.eventstore.api.AppendId;
import org.occurrent.retry.Backoff;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.index.ReactiveIndexOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * A retry must call {@link ReactiveMongoOperations#upsert} again, not resubscribe to a {@code Mono} built once
 * before the first attempt. Building it once would carry the first attempt's {@code Update}, and with it the first
 * attempt's {@code new Date()}, into every retry, so a record that only succeeds after a delay would carry a
 * {@code recordedAt} from well before the insert. Mockito's invocation count is what proves the method itself runs
 * again on retry, since a stale-Mono bug would still leave {@code recordApplied} succeeding, just with the wrong
 * timestamp baked in.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(10)
class ReactiveMongoAppliedAppendStoreRecordAppliedRetryTest {

    @Test
    void a_retried_upsert_calls_the_operations_again_rather_than_resubscribing_to_the_first_attempts_mono() {
        ReactiveMongoOperations mongoOperations = mock(ReactiveMongoOperations.class);
        ReactiveIndexOperations indexOperations = mock(ReactiveIndexOperations.class);
        when(mongoOperations.indexOps(anyString())).thenReturn(indexOperations);
        when(indexOperations.ensureIndex(any())).thenReturn(Mono.just("index"));
        when(mongoOperations.upsert(any(Query.class), any(UpdateDefinition.class), anyString()))
                .thenReturn(Mono.error(new RuntimeException("transient store error")))
                .thenReturn(Mono.just(mock(UpdateResult.class)));
        Retry retry = Retry.fixedDelay(3, Duration.ofMillis(10))
                .onRetryExhaustedThrow((spec, signal) -> signal.failure());
        AppliedAppendStore store = new ReactiveMongoAppliedAppendStore(mongoOperations, "appliedAppends", Duration.ofDays(7), retry, Backoff.fixed(20));

        store.recordApplied("orders", AppendId.mint());

        verify(mongoOperations, times(2)).upsert(any(Query.class), any(UpdateDefinition.class), anyString());
    }

    /**
     * ADR 132 decision 5 asks for a retry so a transient outage does not fail a wait, not for one that never gives
     * up, and the number of attempts it does give up after is {@code ReactiveMongoAppliedAppendStoreBoundsTest}'s.
     * Seven failures in a row is well inside that, and well past the five attempts this default once stopped at.
     */
    @Test
    void the_default_retry_rides_out_seven_failures_in_a_row_rather_than_giving_up_after_a_handful() {
        int failuresBeforeSuccess = 7;
        ReactiveMongoOperations mongoOperations = mock(ReactiveMongoOperations.class);
        ReactiveIndexOperations indexOperations = mock(ReactiveIndexOperations.class);
        when(mongoOperations.indexOps(anyString())).thenReturn(indexOperations);
        when(indexOperations.ensureIndex(any())).thenReturn(Mono.just("index"));
        List<Mono<UpdateResult>> attempts = Stream.concat(
                        Stream.generate(() -> Mono.<UpdateResult>error(new RuntimeException("transient store error"))).limit(failuresBeforeSuccess),
                        Stream.of(Mono.just(mock(UpdateResult.class))))
                .collect(Collectors.toList());
        Iterator<Mono<UpdateResult>> remaining = attempts.iterator();
        when(mongoOperations.upsert(any(Query.class), any(UpdateDefinition.class), anyString()))
                .thenAnswer(invocation -> remaining.next());
        AppliedAppendStore store = new ReactiveMongoAppliedAppendStore(mongoOperations, "appliedAppends", Duration.ofDays(7));

        store.recordApplied("orders", AppendId.mint());

        verify(mongoOperations, times(failuresBeforeSuccess + 1)).upsert(any(Query.class), any(UpdateDefinition.class), anyString());
    }
}
