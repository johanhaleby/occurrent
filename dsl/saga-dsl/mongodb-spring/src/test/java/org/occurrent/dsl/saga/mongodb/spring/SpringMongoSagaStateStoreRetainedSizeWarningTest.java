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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
 * gives for free, so the retained-size warning ({@code flowStateToDocument}, around SpringMongoSagaStateStore.java:270)
 * is exercised without a real MongoDB. Follows the {@code ListAppender} convention from
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

    @BeforeEach
    void attachAppenderAndCreateStore() {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        logger = context.getLogger(SpringMongoSagaStateStore.class);
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
    void a_single_instance_past_capacity_evicts_exactly_one_existing_entry_instead_of_clearing_the_map() {
        for (int i = 0; i < RETAINED_EVENT_WARNING_LATCH_CAPACITY; i++) {
            store.warnIfRetainedSizeCrossesThreshold("s" + i, RETAINED_EVENT_WARNING_THRESHOLD);
        }
        int warningsAtCapacity = warnings().size();

        store.warnIfRetainedSizeCrossesThreshold("overflow", RETAINED_EVENT_WARNING_THRESHOLD);

        assertThat(warningsAtCapacity).isEqualTo(RETAINED_EVENT_WARNING_LATCH_CAPACITY);
        assertThat(warnings().size()).isEqualTo(warningsAtCapacity + 1);
        assertThat(store.retainedEventWarningLatchSize()).isEqualTo(RETAINED_EVENT_WARNING_LATCH_CAPACITY);
    }

    // The eviction the test above exercises picks an arbitrary existing entry, so which one it is depends on map
    // iteration order. Re-saving that same population afterward can therefore chain into further evictions (the
    // evicted id, once re-added, again sits at a map already at capacity), so the only property the backstop actually
    // promises under sustained churn is the SIZE cap, not a fixed count of re-warns. See RETAINED_EVENT_WARNING_LATCH_CAPACITY's
    // javadoc for why an exact duplicate-warning bound is not something this eviction policy can guarantee.
    @Test
    void sustained_churn_at_capacity_never_grows_the_latch_past_its_cap() {
        for (int i = 0; i < RETAINED_EVENT_WARNING_LATCH_CAPACITY; i++) {
            store.warnIfRetainedSizeCrossesThreshold("s" + i, RETAINED_EVENT_WARNING_THRESHOLD);
        }
        store.warnIfRetainedSizeCrossesThreshold("overflow", RETAINED_EVENT_WARNING_THRESHOLD);

        for (int i = 0; i < RETAINED_EVENT_WARNING_LATCH_CAPACITY; i++) {
            store.warnIfRetainedSizeCrossesThreshold("s" + i, RETAINED_EVENT_WARNING_THRESHOLD);
        }

        assertThat(store.retainedEventWarningLatchSize()).isEqualTo(RETAINED_EVENT_WARNING_LATCH_CAPACITY);
    }

    // Concurrent saves for fresh saga ids near capacity could each pass the size check before either evicted, pushing
    // the latch past its cap. Threads race on distinct ids past RETAINED_EVENT_WARNING_LATCH_CAPACITY, so if the
    // check-evict-insert sequence were not serialized, the assertion below would intermittently fail.
    @Test
    void concurrent_saves_of_fresh_instances_past_capacity_never_push_the_latch_past_its_cap() throws InterruptedException {
        int threadCount = 50;
        int idsPerThread = 250;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        for (int t = 0; t < threadCount; t++) {
            int threadIndex = t;
            executor.submit(() -> {
                ready.countDown();
                await(start);
                for (int i = 0; i < idsPerThread; i++) {
                    store.warnIfRetainedSizeCrossesThreshold("thread" + threadIndex + "-id" + i, RETAINED_EVENT_WARNING_THRESHOLD);
                }
            });
        }
        ready.await();
        start.countDown();
        executor.shutdown();
        assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).as("all saves completed").isTrue();

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
