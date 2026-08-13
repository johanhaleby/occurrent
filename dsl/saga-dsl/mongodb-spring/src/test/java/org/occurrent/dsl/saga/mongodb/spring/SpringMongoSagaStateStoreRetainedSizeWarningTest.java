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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.flow.FlowState;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl.ActionKind;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoOperations;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.occurrent.dsl.saga.mongodb.spring.SpringMongoSagaStateStore.RETAINED_EVENT_WARNING_LATCH_CAPACITY;
import static org.occurrent.dsl.saga.mongodb.spring.SpringMongoSagaStateStore.RETAINED_EVENT_WARNING_THRESHOLD;

/**
 * Docker-free: {@code compareAndSave} only needs {@code MongoOperations.insert} to return without throwing, which a mock
 * gives for free, so the retained-size warning in {@code flowStateToDocument} is exercised without a real MongoDB.
 * Follows the {@code ListAppender} convention from
 * {@code SagaExecutionSupportTest.UnmatchedTimer}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoSagaStateStoreRetainedSizeWarningTest {

    sealed interface FlowEvent permits Tick {
    }

    record Tick(String eventId) implements FlowEvent {
    }

    private ListAppender<ILoggingEvent> appender;
    private Logger logger;
    private SpringMongoSagaStateStore<FlowState<FlowEvent>> store;

    private boolean loggerWasAdditive;

    @BeforeEach
    void attachAppenderAndCreateStore() {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        logger = context.getLogger(SpringMongoSagaStateStore.class);
        loggerWasAdditive = logger.isAdditive();
        // The capacity tests below invoke the warning path tens of thousands of times, and additivity would forward
        // every one of those log events to the root console appender as well as this test's own appender.
        logger.setAdditive(false);
        appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);

        MongoOperations mongoOperations = mock(MongoOperations.class);
        @SuppressWarnings("unchecked")
        MongoCollection<Document> collection = mock(MongoCollection.class);
        when(mongoOperations.getCollection(any())).thenReturn(collection);
        when(collection.createIndex(any())).thenReturn("index");

        CloudEventConverter<FlowEvent> converter = new JacksonCloudEventConverter.Builder<FlowEvent>(new ObjectMapper(), URI.create("urn:test"))
                .idMapper(FlowEvent::toString).build();
        store = new SpringMongoSagaStateStore<>(mongoOperations, "saga-flow", rawFlowStateType(), converter);
    }

    @AfterEach
    void detachAppender() {
        logger.detachAppender(appender);
        logger.setAdditive(loggerWasAdditive);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Class<FlowState<FlowEvent>> rawFlowStateType() {
        return (Class) FlowState.class;
    }

    private static List<FlowEvent> ticks(int count) {
        return IntStream.range(0, count).mapToObj(i -> (FlowEvent) new Tick("e" + i)).toList();
    }

    private void save(String sagaId, int retainedEventCount) {
        FlowStateImpl<FlowEvent> state = new FlowStateImpl<>(
                "waiting", ticks(retainedEventCount), 0, 0, false, null, ActionKind.NONE, -1);
        SagaEnvelope<FlowState<FlowEvent>> envelope = new SagaEnvelope<>(
                sagaId, state, SagaStatus.ACTIVE, 0, List.of(), Map.of(), null, null, null, null, null);
        store.compareAndSave(sagaId, envelope, 0);
    }

    private List<ILoggingEvent> warnings() {
        return appender.list.stream().filter(event -> event.getLevel() == Level.WARN).toList();
    }

    @Test
    void a_save_below_the_threshold_warns_nothing() {
        save("s1", RETAINED_EVENT_WARNING_THRESHOLD - 1);

        assertThat(warnings()).isEmpty();
    }

    @Test
    void a_save_that_crosses_the_threshold_warns_once_naming_the_instance_the_count_stepWindow_and_the_document_limit() {
        save("s1", RETAINED_EVENT_WARNING_THRESHOLD);

        assertThat(warnings()).hasSize(1);
        assertThat(warnings().get(0).getFormattedMessage())
                .contains("s1")
                .contains(String.valueOf(RETAINED_EVENT_WARNING_THRESHOLD))
                .contains("stepWindow")
                .contains("16 MB");
    }

    @Test
    void staying_above_the_threshold_across_further_saves_does_not_re_warn() {
        save("s1", RETAINED_EVENT_WARNING_THRESHOLD);
        save("s1", RETAINED_EVENT_WARNING_THRESHOLD + 1);
        save("s1", RETAINED_EVENT_WARNING_THRESHOLD + 2);

        assertThat(warnings()).hasSize(1);
    }

    // Deleting a saga instance that was never trimmed back below the threshold is the only lifecycle hook this store
    // gets for it, so without this the latch entry would otherwise survive for the store's lifetime.
    @Test
    void deleting_an_instance_still_above_the_threshold_frees_its_latch_entry() {
        save("s1", RETAINED_EVENT_WARNING_THRESHOLD);
        store.delete("s1");

        save("s1", RETAINED_EVENT_WARNING_THRESHOLD);

        assertThat(warnings()).as("the delete cleared the latch, so the next save above the threshold warns again").hasSize(2);
    }

    @Test
    void dropping_back_below_the_threshold_and_crossing_again_warns_a_second_time() {
        save("s1", RETAINED_EVENT_WARNING_THRESHOLD);
        // A stepWindow trim (or any other shrink of the retained log) carries the instance back below the threshold.
        save("s1", RETAINED_EVENT_WARNING_THRESHOLD - 1);
        save("s1", RETAINED_EVENT_WARNING_THRESHOLD);

        assertThat(warnings()).hasSize(2);
    }

    @Test
    void two_instances_crossing_the_threshold_each_warn_once_naming_their_own_id() {
        save("s1", RETAINED_EVENT_WARNING_THRESHOLD);
        save("s2", RETAINED_EVENT_WARNING_THRESHOLD);

        assertThat(warnings()).hasSize(2);
        assertThat(warnings().get(0).getFormattedMessage()).contains("s1");
        assertThat(warnings().get(1).getFormattedMessage()).contains("s2");
    }

    // Exercises warnIfRetainedSizeCrossesThreshold directly, with plain counts, rather than through compareAndSave: at
    // the real capacity that would mean building and CloudEvent-serializing 1,000+ retained events for over 10,000
    // instances, which is too slow for a unit test and tests nothing extra over the smaller counts used here.
    @Test
    void a_new_instance_past_capacity_evicts_the_least_recently_used_entry() {
        for (int i = 0; i < RETAINED_EVENT_WARNING_LATCH_CAPACITY; i++) {
            store.warnIfRetainedSizeCrossesThreshold("s" + i, RETAINED_EVENT_WARNING_THRESHOLD);
        }
        int warningsAtCapacity = warnings().size();

        store.warnIfRetainedSizeCrossesThreshold("overflow", RETAINED_EVENT_WARNING_THRESHOLD);

        assertThat(warningsAtCapacity).isEqualTo(RETAINED_EVENT_WARNING_LATCH_CAPACITY);
        assertThat(warnings().size()).isEqualTo(warningsAtCapacity + 1);
        assertThat(store.retainedEventWarningLatchSize()).isEqualTo(RETAINED_EVENT_WARNING_LATCH_CAPACITY);
    }

    // Every save of an already-tracked instance is itself an access under the latch's access-order mode, so it never
    // becomes the eviction target while it keeps being saved. This is what keeps a currently active instance safe
    // regardless of how many other instances cycle through the cache around it.
    @Test
    void resaving_already_tracked_instances_never_makes_them_re_warn() {
        for (int i = 0; i < RETAINED_EVENT_WARNING_LATCH_CAPACITY; i++) {
            store.warnIfRetainedSizeCrossesThreshold("s" + i, RETAINED_EVENT_WARNING_THRESHOLD);
        }
        // Evicts s0, the least recently used entry.
        store.warnIfRetainedSizeCrossesThreshold("overflow", RETAINED_EVENT_WARNING_THRESHOLD);
        int warningsAfterOverflow = warnings().size();

        for (int i = 1; i < RETAINED_EVENT_WARNING_LATCH_CAPACITY; i++) {
            store.warnIfRetainedSizeCrossesThreshold("s" + i, RETAINED_EVENT_WARNING_THRESHOLD);
        }

        assertThat(warnings().size()).as("every one of these instances was already tracked, so none re-warn")
                .isEqualTo(warningsAfterOverflow);
        assertThat(store.retainedEventWarningLatchSize()).isEqualTo(RETAINED_EVENT_WARNING_LATCH_CAPACITY);
    }

    // Distinguishes access order from plain insertion order: an instance saved first but kept warm by every later
    // save must never be the eviction target, even while capacity is filled entirely by instances inserted after it.
    // Counts every warning naming s0 across the whole run, not just after it: under plain insertion order s0 gets
    // evicted and re-tracked partway through the loop below, which would otherwise hide behind a resave that happens
    // to land after that silent extra warning instead of before it.
    @Test
    void repeatedly_resaving_an_instance_protects_it_from_eviction_by_later_arrivals() {
        for (int i = 0; i < RETAINED_EVENT_WARNING_LATCH_CAPACITY; i++) {
            store.warnIfRetainedSizeCrossesThreshold("s" + i, RETAINED_EVENT_WARNING_THRESHOLD);
        }
        for (int i = 0; i < RETAINED_EVENT_WARNING_LATCH_CAPACITY; i++) {
            store.warnIfRetainedSizeCrossesThreshold("s0", RETAINED_EVENT_WARNING_THRESHOLD);
            store.warnIfRetainedSizeCrossesThreshold("new" + i, RETAINED_EVENT_WARNING_THRESHOLD);
        }
        store.warnIfRetainedSizeCrossesThreshold("s0", RETAINED_EVENT_WARNING_THRESHOLD);

        long s0Warnings = warnings().stream().filter(event -> event.getFormattedMessage().contains("'s0'")).count();
        assertThat(s0Warnings).as("s0 was kept warm throughout and never evicted, so it only ever warned once, at the start")
                .isEqualTo(1);
    }

    // The property that fixes the case SagaStateStore.delete's own javadoc describes as the recommended default: a
    // completed instance retired by letting MongoDB's TTL expire it, never through delete(String). No call ever
    // reaches this store for that removal, so without eviction the instance's latch entry would survive forever.
    // Here it stops being saved (simulating exactly that), and once enough other instances have cycled through the
    // cache, its entry is evicted and it can warn again on its own, with no delete() call at all.
    @Test
    void an_instance_that_stops_being_saved_is_eventually_evicted_and_warns_again_without_delete() {
        for (int i = 0; i < RETAINED_EVENT_WARNING_LATCH_CAPACITY; i++) {
            store.warnIfRetainedSizeCrossesThreshold("s" + i, RETAINED_EVENT_WARNING_THRESHOLD);
        }
        // s0 is now abandoned: never saved again. Enough new instances arrive to cycle the whole cache, which must
        // evict s0 along the way since it is the least recently used entry throughout.
        for (int i = 0; i < RETAINED_EVENT_WARNING_LATCH_CAPACITY; i++) {
            store.warnIfRetainedSizeCrossesThreshold("t" + i, RETAINED_EVENT_WARNING_THRESHOLD);
        }
        int warningsBeforeResave = warnings().size();

        store.warnIfRetainedSizeCrossesThreshold("s0", RETAINED_EVENT_WARNING_THRESHOLD);

        assertThat(warnings().size()).as("s0's entry was evicted while abandoned, so this is a fresh crossing")
                .isEqualTo(warningsBeforeResave + 1);
    }

    // The limit of the LRU guarantee: it protects an active instance from eviction only while the number of
    // simultaneously active oversized instances stays at or below capacity. One more than that, cycled round-robin
    // with none of them ever idle, means every save evicts and re-admits the id about to be needed next, so the
    // excess re-warns on every single save. This is the accepted cost of keeping a hard memory bound rather than an
    // unbounded, idle-expiry-only cache, see RETAINED_EVENT_WARNING_LATCH_CAPACITY's comment.
    @Test
    void a_population_sustained_one_above_capacity_re_warns_on_every_save_for_the_whole_round() {
        int populationSize = RETAINED_EVENT_WARNING_LATCH_CAPACITY + 1;

        for (int round = 0; round < 3; round++) {
            for (int i = 0; i < populationSize; i++) {
                store.warnIfRetainedSizeCrossesThreshold("r" + i, RETAINED_EVENT_WARNING_THRESHOLD);
            }
        }

        assertThat(warnings().size()).as("every save in every round warns, not just the first round's crossings")
                .isEqualTo(3L * populationSize);
    }

    // Concurrent saves for fresh saga ids near capacity could each pass the size check before either was inserted,
    // pushing the latch past its cap. Threads race on distinct ids past RETAINED_EVENT_WARNING_LATCH_CAPACITY, so if
    // the check-and-insert sequence were not serialized, the assertion below would intermittently fail.
    @Test
    void concurrent_saves_of_fresh_instances_past_capacity_never_push_the_latch_past_its_cap() throws InterruptedException, ExecutionException {
        int threadCount = 50;
        int idsPerThread = 250;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threadCount; t++) {
            int threadIndex = t;
            futures.add(executor.submit(() -> {
                ready.countDown();
                await(start);
                for (int i = 0; i < idsPerThread; i++) {
                    store.warnIfRetainedSizeCrossesThreshold("thread" + threadIndex + "-id" + i, RETAINED_EVENT_WARNING_THRESHOLD);
                }
            }));
        }
        ready.await();
        start.countDown();
        executor.shutdown();
        assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).as("all saves completed").isTrue();
        // A worker exception would otherwise be swallowed and the test would pass on whatever the latch's state
        // happened to be when the exception cut that worker short.
        for (Future<?> future : futures) {
            future.get();
        }

        assertThat(threadCount * idsPerThread).as("test setup pushes past capacity").isGreaterThan(RETAINED_EVENT_WARNING_LATCH_CAPACITY);
        assertThat(store.retainedEventWarningLatchSize()).isEqualTo(RETAINED_EVENT_WARNING_LATCH_CAPACITY);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
