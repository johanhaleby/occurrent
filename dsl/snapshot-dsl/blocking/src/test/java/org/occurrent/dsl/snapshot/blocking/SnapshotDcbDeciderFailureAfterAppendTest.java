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

package org.occurrent.dsl.snapshot.blocking;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.service.blocking.SynchronousEventDispatcher;
import org.occurrent.application.service.blocking.TransactionExecutor;
import org.occurrent.application.service.blocking.dcb.GenericDcbApplicationService;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.dsl.dcb.DcbDecider;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.dsl.snapshot.SnapshotOptions;
import org.occurrent.dsl.snapshot.SnapshotPolicy;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Without a transaction the append has committed when a synchronous handler throws, so the snapshot decider must hand
 * that failure to the caller instead of deciding the command a second time.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SnapshotDcbDeciderFailureAfterAppendTest {

    private static final Tag ACCOUNT = Tag.of("name", "acct");
    private static final DcbCriteria CRITERIA = DcbCriteria.tags(ACCOUNT);

    private InMemoryEventStore eventStore;
    private CloudEventConverter<DomainEvent> converter;
    private final AtomicInteger decides = new AtomicInteger();

    @BeforeEach
    void initialize() {
        eventStore = new InMemoryEventStore();
        converter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
    }

    @Test
    void a_retried_conflict_thrown_by_a_handler_after_the_commit_reaches_the_caller_when_a_snapshot_exists() {
        DcbAppendConditionNotFulfilledException conflict = new DcbAppendConditionNotFulfilledException(DcbAppendCondition.failIfEventsMatch(CRITERIA), 1);
        FailingOnceDispatcher dispatcher = new FailingOnceDispatcher();
        var service = new SnapshotDcbDeciderApplicationService<>(GenericDcbApplicationService.builder(eventStore, converter)
                .tagGenerator((DomainEvent event) -> Set.of(ACCOUNT))
                .synchronousSubscriptions(dispatcher)
                .transactionExecutor(TransactionExecutor.noTransaction())
                .build());
        SnapshotDcbDecider<String, Integer, DomainEvent> decider = SnapshotDcbDecider.from(DcbDecider.from(counting(), command -> CRITERIA, event -> Set.of(ACCOUNT)),
                SnapshotStore.inMemory(), SnapshotOptions.of(1, SnapshotPolicy.always()));
        service.execute("first", decider);
        service.execute("second", decider);
        decides.set(0);
        dispatcher.failWith(conflict);

        Throwable thrown = catchThrowable(() -> service.execute("third", decider));

        assertThat(decides.get()).as("decide runs").isEqualTo(1);
        assertThat(thrown).as("exception reaching the caller").isSameAs(conflict);
        assertThat(eventStore.read(CRITERIA).events()).as("events in the boundary").hasSize(3);
    }

    private Decider<String, Integer, DomainEvent> counting() {
        return new Decider<>() {
            @Override
            public Integer initialState() {
                return 0;
            }

            @NonNull
            @Override
            public List<DomainEvent> decide(@NonNull String command, Integer state) {
                decides.incrementAndGet();
                return List.of(new NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.now(), "name", command));
            }

            @Override
            public Integer evolve(Integer state, @NonNull DomainEvent event) {
                return state + 1;
            }
        };
    }

    private static final class FailingOnceDispatcher implements SynchronousEventDispatcher {
        private @Nullable RuntimeException failure;

        void failWith(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public void dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
            RuntimeException toThrow = failure;
            failure = null;
            if (toThrow != null) {
                throw toThrow;
            }
        }

        @Override
        public boolean hasSubscriptions() {
            return true;
        }
    }
}
