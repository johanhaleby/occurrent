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

package org.occurrent.application.service.blocking.dcb;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.generic.GenericCloudEventConverter;
import org.occurrent.application.service.blocking.PolicySideEffect;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.DomainEventConverter;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.dcb.DcbQuery.tagsAllOf;

@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbApplicationServiceSideEffectTest {

    private DcbApplicationService<DomainEvent> applicationService;
    private InMemoryEventStore eventStore;

    @BeforeEach
    void initialize_application_service() {
        DomainEventConverter domainEventConverter = new DomainEventConverter(new ObjectMapper());
        CloudEventConverter<DomainEvent> cloudEventConverter = new GenericCloudEventConverter<>(domainEventConverter::convertToDomainEvent, domainEventConverter::convertToCloudEvent);
        eventStore = new InMemoryEventStore();
        applicationService = new GenericDcbApplicationService<>(eventStore, cloudEventConverter, event -> Set.of("name:1"));
    }

    @Test
    void invokes_side_effect_once_with_the_newly_written_events_after_a_successful_append() {
        AtomicInteger invocations = new AtomicInteger();
        List<DomainEvent> observed = new ArrayList<>();
        NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Johan");

        Optional<DcbAppendResult> result = applicationService.execute(tagsAllOf("name:1"),
                DcbExecuteOptions.<DomainEvent>options().sideEffect(events -> {
                    invocations.incrementAndGet();
                    events.forEach(observed::add);
                }),
                events -> Stream.of(nameDefined));

        assertThat(result).isPresent();
        assertThat(invocations).hasValue(1);
        assertThat(observed).containsExactly(nameDefined);
    }

    @Test
    void does_not_invoke_side_effect_when_the_domain_function_returns_no_events() {
        AtomicBoolean invoked = new AtomicBoolean();

        Optional<DcbAppendResult> result = applicationService.execute(tagsAllOf("name:1"),
                DcbExecuteOptions.<DomainEvent>options().sideEffect(events -> invoked.set(true)),
                events -> Stream.empty());

        assertThat(result).isEmpty();
        assertThat(invoked).isFalse();
    }

    @Test
    void reused_policy_side_effect_fires_only_for_the_matching_event_type() {
        AtomicReference<String> definedName = new AtomicReference<>("not-called");
        AtomicReference<String> changedName = new AtomicReference<>("not-called");
        PolicySideEffect<DomainEvent> policy = PolicySideEffect.<DomainEvent, NameDefined>executePolicy(NameDefined.class, e -> definedName.set(e.name()))
                .andThenExecuteAnotherPolicy(NameWasChanged.class, e -> changedName.set(e.name()));

        applicationService.execute(tagsAllOf("name:1"),
                DcbExecuteOptions.<DomainEvent>options().sideEffect(policy),
                events -> Stream.of(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Johan")));

        assertThat(definedName.get()).isEqualTo("Johan");
        assertThat(changedName.get()).isEqualTo("not-called");
    }

    @Test
    void simple_execute_overload_appends_without_a_side_effect() {
        Optional<DcbAppendResult> result = applicationService.execute(tagsAllOf("name:1"),
                events -> Stream.of(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "Johan")));

        assertThat(result).isPresent();
        assertThat(eventStore.read(tagsAllOf("name:1")).events()).extracting(CloudEvent::getType).hasSize(1);
    }
}
