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

package org.occurrent.dsl.projection.blocking;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.application.service.blocking.generic.GenericApplicationService;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.eventstore.api.AppendId;
import org.occurrent.eventstore.api.WriteResult;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The empty-append contract (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>,
 * decision 4): an append that persists no events stamps nothing, so its {@link WriteResult#appendId()} is absent,
 * and there is genuinely nothing to wait for.
 * <p>
 * The TCK already asserts absence at the {@code EventStore.write(...)} level directly. This proves the SAME
 * contract at the caller-facing surface an application actually uses:
 * {@code ApplicationService.execute(streamId, domainFunction)} routinely returns zero events (no emptiness guard in
 * {@code GenericApplicationService}), and the {@code Optional<AppendId>} component is what makes the absent case a
 * contract rather than a request, decision 3's reasoning. This falsifies two things at once: that the real
 * application-facing write path reports absence honestly, and that {@link AppliedAppendStore#waitUntilApplied}
 * cannot be reached with an absent id without the caller explicitly unwrapping an empty {@link java.util.Optional}
 * first, which is exactly the footgun decision 3 says the type system prevents.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class AppendIdEmptyAppendContractTest {

    private static final URI SOURCE = URI.create("urn:occurrent:empty-append-contract");

    sealed interface TestEvent {
    }

    record Noop() implements TestEvent {
    }

    @Test
    void a_command_that_writes_no_events_returns_a_write_result_whose_append_id_is_absent_and_cannot_reach_the_wait_unopened() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        CloudEventConverter<TestEvent> converter = new JacksonCloudEventConverter<>(new ObjectMapper(), SOURCE);
        ApplicationService<TestEvent> applicationService = new GenericApplicationService<>(eventStore, converter);

        // A domain function that decides nothing needs to happen: the routine "no-op command" shape
        // GenericApplicationService.execute has no emptiness guard against.
        WriteResult result = applicationService.execute(UUID.randomUUID().toString(), events -> List.of());

        assertThat(result.appendId()).as("an append that persisted nothing stamps nothing").isEmpty();

        // The compiler-enforced contract: a caller cannot reach waitUntilApplied with a value unless it explicitly
        // unwraps the Optional first, and unwrapping an absent one fails loudly rather than handing a null through.
        assertThatThrownBy(() -> result.appendId().orElseThrow())
                .as("there is nothing to wait on, and the type forces that decision onto the caller before any wait is attempted")
                .isInstanceOf(NoSuchElementException.class);

        // waitUntilApplied itself takes a non-null AppendId, never an Optional, so a caller holding only
        // Optional<AppendId> has to unwrap it before this line can even be written. The assertion below checks the
        // runtime half of that contract, a null that still reaches this method is rejected outright rather than
        // treated as "never applies".
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        assertThatThrownBy(() -> store.waitUntilApplied("orders", null, Duration.ofMillis(50)))
                .as("waitUntilApplied rejects a null append id outright rather than silently never applying")
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void a_command_that_writes_events_returns_a_write_result_whose_append_id_is_present() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        CloudEventConverter<TestEvent> converter = new JacksonCloudEventConverter<>(new ObjectMapper(), SOURCE);
        ApplicationService<TestEvent> applicationService = new GenericApplicationService<>(eventStore, converter);

        WriteResult result = applicationService.execute(UUID.randomUUID().toString(), events -> List.of(new Noop()));

        assertThat(result.appendId()).isPresent();
        AppendId appendId = result.appendId().orElseThrow();
        AppliedAppendStore store = AppliedAppendStore.inMemory();
        store.recordApplied("orders", appendId);

        assertThat(store.waitUntilApplied("orders", appendId, Duration.ofSeconds(1))).isTrue();
    }
}
