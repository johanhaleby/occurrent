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
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * None of this store's other tests call it from a thread {@link Schedulers#isInNonBlockingThread()} recognizes, so
 * none of them could have caught {@link ReactiveMongoAppliedAppendStore#waitUntilApplied(String, AppendId, Duration)}
 * answering {@code false} for an append that was there, after blocking that thread for the full timeout, the defect
 * fixed alongside this test. {@link Schedulers#parallel()} supplies such a thread here, the same marker
 * {@code reactor-netty} puts on its own event loop threads.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(10)
class ReactiveMongoAppliedAppendStoreNonBlockingCallerTest {

    private static ReactiveMongoOperations mongoOperationsThatWouldAnswerApplied() {
        ReactiveMongoOperations mongoOperations = mock(ReactiveMongoOperations.class);
        ReactiveIndexOperations indexOperations = mock(ReactiveIndexOperations.class);
        when(mongoOperations.indexOps(anyString())).thenReturn(indexOperations);
        when(indexOperations.ensureIndex(any())).thenReturn(Mono.just("index"));
        when(mongoOperations.exists(any(Query.class), anyString())).thenReturn(Mono.just(true));
        return mongoOperations;
    }

    private record CallOutcome(boolean applied, Throwable thrown, Duration elapsed) {
    }

    // Runs store.waitUntilApplied(..) on an actual Schedulers.parallel() worker, not merely on a Mono chain that
    // might itself hop before the call is made, since it is the thread the call runs on that this defect depends on.
    private static CallOutcome waitFromANonBlockingThread(AppliedAppendStore store, Duration timeout) throws InterruptedException {
        AtomicReference<Boolean> applied = new AtomicReference<>();
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        Instant start = Instant.now();
        Schedulers.parallel().schedule(() -> {
            try {
                applied.set(store.waitUntilApplied("orders", AppendId.mint(), timeout));
            } catch (Throwable t) {
                thrown.set(t);
            } finally {
                done.countDown();
            }
        });
        assertThat(done.await(9, TimeUnit.SECONDS)).as("the scheduled call finished within the test's own timeout").isTrue();
        Duration elapsed = Duration.between(start, Instant.now());
        return new CallOutcome(Boolean.TRUE.equals(applied.get()), thrown.get(), elapsed);
    }

    @Test
    void reports_it_cannot_answer_instead_of_blocking_the_calling_thread_to_the_deadline() throws InterruptedException {
        ReactiveMongoOperations mongoOperations = mongoOperationsThatWouldAnswerApplied();
        AppliedAppendStore store = new ReactiveMongoAppliedAppendStore(mongoOperations, "appliedAppends", Duration.ofDays(7));
        Duration timeout = Duration.ofSeconds(5);

        CallOutcome outcome = waitFromANonBlockingThread(store, timeout);

        assertThat(outcome.thrown()).isInstanceOf(IllegalStateException.class);
        // Well under the timeout: a caller told immediately, not one that slept its thread through every poll to
        // the deadline and got false regardless.
        assertThat(outcome.elapsed()).isLessThan(timeout.dividedBy(2));
        // Never attempted a read at all, which is what tells this apart from a store failure answering false.
        verifyNoInteractions(mongoOperations);
    }

    @Test
    void the_four_argument_overload_with_an_explicit_backoff_reports_it_cannot_answer_the_same_way() throws InterruptedException {
        ReactiveMongoOperations mongoOperations = mongoOperationsThatWouldAnswerApplied();
        AppliedAppendStore store = new ReactiveMongoAppliedAppendStore(mongoOperations, "appliedAppends", Duration.ofDays(7));

        AtomicReference<Throwable> thrown = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        Schedulers.parallel().schedule(() -> {
            try {
                store.waitUntilApplied("orders", AppendId.mint(), Duration.ofSeconds(5), Backoff.fixed(20));
            } catch (Throwable t) {
                thrown.set(t);
            } finally {
                done.countDown();
            }
        });

        assertThat(done.await(9, TimeUnit.SECONDS)).isTrue();
        assertThat(thrown.get()).isInstanceOf(IllegalStateException.class);
    }
}
