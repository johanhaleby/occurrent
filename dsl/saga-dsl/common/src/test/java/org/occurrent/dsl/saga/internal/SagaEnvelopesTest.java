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

package org.occurrent.dsl.saga.internal;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.saga.InMemorySagaStateStore;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.codec.SagaCodec;
import org.occurrent.dsl.saga.codec.StoredSaga;
import org.occurrent.dsl.saga.flow.FlowState;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl.ActionKind;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaEnvelopesTest {

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final int ABOVE_THE_WARNING_THRESHOLD = 1_000;

    private final InMemorySagaStateStore<String> store = new InMemorySagaStateStore<>();
    private final SagaEnvelopes<String> envelopes = SagaEnvelopes.of(store, null);

    private ListAppender<ILoggingEvent> appender;
    private Logger codecLogger;

    @BeforeEach
    void attachAppender() {
        codecLogger = ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger(SagaCodec.class);
        appender = new ListAppender<>();
        appender.start();
        codecLogger.addAppender(appender);
    }

    @AfterEach
    void detachAppender() {
        codecLogger.detachAppender(appender);
    }

    @Test
    void a_save_under_an_id_that_is_not_the_envelopes_is_refused_and_stores_nothing() {
        assertThatThrownBy(() -> envelopes.compareAndSave("other", active("s1", "state", 0), 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("other")
                .hasMessageContaining("s1");
        assertThat(store.find("s1")).isEmpty();
        assertThat(store.find("other")).isEmpty();
    }

    @Test
    void a_save_without_the_state_under_an_id_that_is_not_the_envelopes_is_refused() {
        envelopes.compareAndSave("s1", active("s1", "state", 1), 0);

        assertThatThrownBy(() -> envelopes.compareAndSaveWithoutState("other", active("s1", "changed", 2), 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(envelopes.find("s1").orElseThrow().version()).isEqualTo(1L);
    }

    @Test
    void an_insert_through_the_save_without_the_state_writes_the_whole_envelope_including_the_state() {
        boolean saved = envelopes.compareAndSaveWithoutState("s1", active("s1", "state", 1), 0);

        assertThat(saved).isTrue();
        assertThat(envelopes.find("s1").orElseThrow().state()).isEqualTo("state");
    }

    @Test
    void an_update_through_the_save_without_the_state_leaves_the_stored_state_alone() {
        envelopes.compareAndSave("s1", active("s1", "original", 1), 0);

        boolean saved = envelopes.compareAndSaveWithoutState("s1", quarantinedWith("s1", "ignored", 2), 1);

        SagaEnvelope<String> found = envelopes.find("s1").orElseThrow();
        assertThat(saved).isTrue();
        assertThat(found.state()).isEqualTo("original");
        assertThat(found.status()).isEqualTo(SagaStatus.QUARANTINED);
        assertThat(found.version()).isEqualTo(2L);
    }

    @Test
    void a_read_without_the_state_gives_an_envelope_without_a_state_even_from_a_store_that_holds_one() {
        envelopes.compareAndSave("s1", active("s1", "state", 1), 0);

        assertThat(envelopes.findWithoutState("s1").orElseThrow().state()).isNull();
        assertThat(envelopes.find("s1").orElseThrow().state()).isEqualTo("state");
    }

    @Test
    void the_due_timer_read_gives_envelopes_without_a_state() {
        envelopes.compareAndSave("s1", new SagaEnvelope<>("s1", "state", SagaStatus.ACTIVE, 1, List.of(new TimerEntry("t", 10L, null)), Map.of(), null, NOW, NOW, null, null, true, null, Map.of(), null), 0);

        List<SagaEnvelope<String>> due = envelopes.findWithDueTimers(Instant.ofEpochMilli(10L), 10);

        assertThat(due).extracting(SagaEnvelope::sagaId).containsExactly("s1");
        assertThat(due.get(0).state()).isNull();
    }

    @Test
    void the_status_read_gives_envelopes_without_a_state() {
        envelopes.compareAndSave("s1", active("s1", "state", 1), 0);

        List<SagaEnvelope<String>> found = envelopes.findByStatus(SagaStatus.ACTIVE, NOW.plusSeconds(1), 10);

        assertThat(found).extracting(SagaEnvelope::sagaId).containsExactly("s1");
        assertThat(found.get(0).state()).isNull();
    }

    @Test
    void the_status_read_of_a_store_that_cannot_enumerate_is_unsupported_and_names_the_store() {
        SagaEnvelopes<String> withoutQueries = SagaEnvelopes.of(new CoreOnlyStore(), null);

        assertThatThrownBy(() -> withoutQueries.findByStatus(SagaStatus.ACTIVE, NOW, 1))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(CoreOnlyStore.class.getName());
    }

    @Test
    void an_insert_forgets_a_retained_size_warning_given_for_an_instance_the_store_lost_without_a_delete() {
        InMemorySagaStateStore<FlowState<Tick>> flowStore = new InMemorySagaStateStore<>();
        SagaEnvelopes<FlowState<Tick>> flowEnvelopes = SagaEnvelopes.of(flowStore, new TickConverter());
        flowEnvelopes.compareAndSave("s1", flowEnvelope("s1", 1), 0);
        assertThat(warnings()).hasSize(1);

        flowStore.delete("s1");
        flowEnvelopes.compareAndSave("s1", flowEnvelope("s1", 1), 0);

        assertThat(warnings()).as("a new instance under the id of one that a TTL index removed warns about its own events").hasSize(2);
    }

    @Test
    void an_update_does_not_forget_a_retained_size_warning_already_given() {
        InMemorySagaStateStore<FlowState<Tick>> flowStore = new InMemorySagaStateStore<>();
        SagaEnvelopes<FlowState<Tick>> flowEnvelopes = SagaEnvelopes.of(flowStore, new TickConverter());
        flowEnvelopes.compareAndSave("s1", flowEnvelope("s1", 1), 0);
        flowEnvelopes.compareAndSave("s1", flowEnvelope("s1", 1), 1);
        flowEnvelopes.compareAndSave("s1", flowEnvelope("s1", 2), 2);

        assertThat(warnings()).hasSize(1);
    }

    @Test
    void an_insert_through_the_save_without_the_state_forgets_the_warning_too() {
        InMemorySagaStateStore<FlowState<Tick>> flowStore = new InMemorySagaStateStore<>();
        SagaEnvelopes<FlowState<Tick>> flowEnvelopes = SagaEnvelopes.of(flowStore, new TickConverter());
        flowEnvelopes.compareAndSaveWithoutState("s1", flowEnvelope("s1", 1), 0);
        flowStore.delete("s1");

        flowEnvelopes.compareAndSaveWithoutState("s1", flowEnvelope("s1", 1), 0);

        assertThat(warnings()).hasSize(2);
    }

    @Test
    void a_flow_state_that_is_not_the_flow_executors_is_refused_on_the_first_save_when_a_converter_is_given() {
        SagaEnvelopes<FlowState<Tick>> flowEnvelopes = SagaEnvelopes.of(new InMemorySagaStateStore<FlowState<Tick>>(), new TickConverter());
        FlowState<Tick> foreign = new FlowState<>() {
            @Override
            public String currentStep() {
                return "waiting";
            }

            @Override
            public List<Tick> received() {
                return List.of();
            }

            @Override
            public boolean completed() {
                return false;
            }
        };

        assertThatThrownBy(() -> flowEnvelopes.compareAndSave("s1", new SagaEnvelope<>("s1", foreign, SagaStatus.ACTIVE, 0, List.of(), Map.of(), null, NOW, NOW, null, null, true, null, Map.of(), null), 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void construction_and_every_operation_refuse_null() {
        assertThatNullPointerException().isThrownBy(() -> SagaEnvelopes.of(null, null)).withMessageContaining("stateStore");
        assertThatNullPointerException().isThrownBy(() -> envelopes.find(null)).withMessageContaining("sagaId");
        assertThatNullPointerException().isThrownBy(() -> envelopes.findWithoutState(null)).withMessageContaining("sagaId");
        assertThatNullPointerException().isThrownBy(() -> envelopes.compareAndSave(null, active("s1", "state", 0), 0)).withMessageContaining("sagaId");
        assertThatNullPointerException().isThrownBy(() -> envelopes.compareAndSave("s1", null, 0)).withMessageContaining("envelope");
        assertThatNullPointerException().isThrownBy(() -> envelopes.compareAndSaveWithoutState(null, active("s1", "state", 0), 0)).withMessageContaining("sagaId");
        assertThatNullPointerException().isThrownBy(() -> envelopes.compareAndSaveWithoutState("s1", null, 0)).withMessageContaining("envelope");
        assertThatNullPointerException().isThrownBy(() -> envelopes.findWithDueTimers(null, 1)).withMessageContaining("now");
        assertThatNullPointerException().isThrownBy(() -> envelopes.findByStatus(null, NOW, 1)).withMessageContaining("status");
        assertThatNullPointerException().isThrownBy(() -> envelopes.findByStatus(SagaStatus.ACTIVE, null, 1)).withMessageContaining("updatedBefore");
    }

    private List<ILoggingEvent> warnings() {
        return appender.list.stream().filter(event -> event.getLevel() == Level.WARN).toList();
    }

    private static SagaEnvelope<FlowState<Tick>> flowEnvelope(String sagaId, int crossings) {
        int received = crossings == 0 ? 0 : ABOVE_THE_WARNING_THRESHOLD;
        List<Tick> ticks = IntStream.range(0, received).mapToObj(i -> new Tick("e" + i)).toList();
        FlowStateImpl<Tick> state = new FlowStateImpl<>("waiting", ticks, 0, 0, false, null, -1, ActionKind.NONE, -1, null);
        return new SagaEnvelope<>(sagaId, state, SagaStatus.ACTIVE, crossings, List.of(), Map.of(), null, NOW, NOW, null, null, true, null, Map.of(), null);
    }

    private static SagaEnvelope<String> active(String sagaId, String state, long version) {
        return new SagaEnvelope<>(sagaId, state, SagaStatus.ACTIVE, version, List.of(), Map.of(), null, NOW, NOW, null, null, true, null, Map.of(), null);
    }

    private static SagaEnvelope<String> quarantinedWith(String sagaId, String state, long version) {
        return new SagaEnvelope<>(sagaId, state, SagaStatus.QUARANTINED, version, List.of(), Map.of(), null, NOW, NOW, null, null, true,
                new org.occurrent.dsl.saga.SagaFailure("s@7", 7L, NOW, IllegalStateException.class.getName(), "boom", null, null), Map.of(), null);
    }

    private record Tick(String eventId) {
    }

    private static final class TickConverter implements CloudEventConverter<Tick> {
        @Override
        public CloudEvent toCloudEvent(Tick event) {
            return CloudEventBuilder.v1().withId(event.eventId()).withSource(URI.create("urn:test")).withType("tick").build();
        }

        @Override
        public Tick toDomainEvent(CloudEvent cloudEvent) {
            return new Tick(cloudEvent.getId());
        }

        @Override
        public String getCloudEventType(Class<? extends Tick> type) {
            return "tick";
        }
    }

    /** A store that implements only the core SPI, so it cannot enumerate instances by status. */
    private static final class CoreOnlyStore implements SagaStateStore<String> {
        private final InMemorySagaStateStore<String> delegate = new InMemorySagaStateStore<>();

        @Override
        public org.occurrent.dsl.saga.codec.SagaStateCodec<String> stateCodec() {
            return delegate.stateCodec();
        }

        @Override
        public Optional<StoredSaga> find(String sagaId) {
            return delegate.find(sagaId);
        }

        @Override
        public Optional<StoredSaga> findWithoutState(String sagaId) {
            return delegate.findWithoutState(sagaId);
        }

        @Override
        public boolean compareAndSave(StoredSaga saga, long expectedVersion) {
            return delegate.compareAndSave(saga, expectedVersion);
        }

        @Override
        public boolean compareAndSaveWithoutState(StoredSaga saga, long expectedVersion) {
            return delegate.compareAndSaveWithoutState(saga, expectedVersion);
        }

        @Override
        public List<StoredSaga> findWithDueTimers(Instant now, int limit) {
            return delegate.findWithDueTimers(now, limit);
        }

        @Override
        public void delete(String sagaId) {
            delegate.delete(sagaId);
        }
    }
}
