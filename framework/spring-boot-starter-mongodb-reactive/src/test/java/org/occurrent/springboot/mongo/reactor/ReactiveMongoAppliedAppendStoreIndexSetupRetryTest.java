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
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@code Retry.backoff}'s delayed resubscription runs on {@code Schedulers.parallel()}, not the calling thread. A
 * fresh store's index setup that blocked internally, as an earlier version of {@code ensureIndexesOnce} did, would
 * hold one of that shared pool's few threads for the length of the Mongo call on every retry rather than releasing
 * it back between attempts. This forces a real transient failure through a real backoff delay, not an
 * immediately-resolved retry, so the resubscription genuinely happens off the calling thread, and asserts recovery
 * still completes correctly composed entirely as {@code Mono} operations.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(10)
class ReactiveMongoAppliedAppendStoreIndexSetupRetryTest {

    @Test
    void a_transient_index_setup_failure_recovers_on_retry_off_the_calling_thread() {
        ReactiveMongoOperations mongoOperations = mock(ReactiveMongoOperations.class);
        ReactiveIndexOperations indexOperations = mock(ReactiveIndexOperations.class);
        when(mongoOperations.indexOps(anyString())).thenReturn(indexOperations);
        // First call (the unique index) fails once, then every subsequent ensureIndex call succeeds, covering the
        // unique index retry attempt and the TTL index call that follows it in the same setup.
        when(indexOperations.ensureIndex(any()))
                .thenReturn(Mono.error(new RuntimeException("transient store error")))
                .thenReturn(Mono.just("index"));
        when(mongoOperations.exists(any(Query.class), anyString())).thenReturn(Mono.just(true));
        Retry retry = Retry.backoff(3, Duration.ofMillis(10))
                .onRetryExhaustedThrow((spec, signal) -> signal.failure());
        AppliedAppendStore store = new ReactiveMongoAppliedAppendStore(mongoOperations, "appliedAppends", Duration.ofDays(7), retry, Backoff.fixed(20));

        boolean applied = store.hasApplied("orders", AppendId.mint());

        assertThat(applied).isTrue();
    }
}
