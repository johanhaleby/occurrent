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

package org.occurrent.example.snapshot.closingthebooks

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper
import org.occurrent.application.service.blocking.ApplicationService
import org.occurrent.application.service.blocking.generic.GenericApplicationService
import org.occurrent.dsl.decider.Decider
import org.occurrent.dsl.snapshot.SnapshotOptions
import org.occurrent.dsl.snapshot.SnapshotPolicy
import org.occurrent.dsl.snapshot.blocking.SnapshotDecider
import org.occurrent.dsl.snapshot.blocking.SnapshotDeciderApplicationService
import org.occurrent.dsl.snapshot.blocking.SnapshotPolicies
import org.occurrent.dsl.snapshot.blocking.SnapshotStore
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import java.net.URI

class ClosingTheBooksTest {

    private lateinit var eventStore: InMemoryEventStore
    private lateinit var applicationService: ApplicationService<LedgerEvent>
    private lateinit var decider: Decider<LedgerCommand, LedgerState, LedgerEvent>
    private lateinit var store: SnapshotStore<LedgerState>

    @BeforeEach
    fun setUp() {
        eventStore = InMemoryEventStore()
        val converter: CloudEventConverter<LedgerEvent> =
            JacksonCloudEventConverter.Builder<LedgerEvent>(tools.jackson.module.kotlin.jacksonObjectMapper(), URI.create("urn:occurrent:example:snapshot"))
                .typeMapper(ReflectionCloudEventTypeMapper.simple(LedgerEvent::class.java))
                .build()
        applicationService = GenericApplicationService(eventStore, converter)
        decider = ledgerDecider()
        store = SnapshotStore.inMemory()
    }

    @Test
    fun `technical snapshot is taken every N events and a later command resumes from it`() {
        val snapshots = SnapshotDeciderApplicationService(applicationService)
        val account = SnapshotDecider.from(decider, store, SnapshotOptions.everyNEvents<LedgerState, LedgerEvent>(1, 100))

        repeat(250) { snapshots.execute("account-1", Deposit(1), account) }

        // The most recent snapshot sits at the last version the every-100 policy crossed, not at the head.
        val snapshot = store.findLatest("account-1")
        assertThat(snapshot).isPresent
        assertThat(snapshot.get().version()).isEqualTo(200L)
        assertThat(snapshot.get().state()).isEqualTo(LedgerState(balance = 200, closed = false))

        // The next command loads the snapshot at version 200 and folds only the 50-event tail before deciding.
        val state = snapshots.executeAndReturnState("account-1", Deposit(5), account)
        assertThat(state).isEqualTo(LedgerState(balance = 255, closed = false))
    }

    @Test
    fun `closing the books snapshots the terminal state and carries the balance into the next period`() {
        val policy = SnapshotPolicies.whenTerminal(decider).or(SnapshotPolicy.everyNEvents<LedgerState, LedgerEvent>(100))
        val snapshots = SnapshotDeciderApplicationService(applicationService)
        val account = SnapshotDecider.from(decider, store, SnapshotOptions.of(1, policy))

        snapshots.execute("period-2026-Q1", Deposit(100), account)
        snapshots.execute("period-2026-Q1", Withdraw(30), account)
        snapshots.execute("period-2026-Q1", CloseBooks("2026-Q1"), account)

        // whenTerminal fired on CloseBooks, so the snapshot holds the closing balance at the terminal state.
        val closed = store.findLatest("period-2026-Q1")
        assertThat(closed).isPresent
        assertThat(closed.get().state()).isEqualTo(LedgerState(balance = 70, closed = true))

        // The closing balance becomes the opening balance of the next period, recorded as a real event in a new stream.
        val closingBalance = closed.get().state().balance
        val openingState = snapshots.executeAndReturnState("period-2026-Q2", SetOpeningBalance(closingBalance), account)
        assertThat(openingState).isEqualTo(LedgerState(balance = 70, closed = false))

        // The closed period's detailed events can now be archived. The snapshot is a discardable optimization, and the
        // authoritative opening balance lives as an event in the next period, so archiving does not lose money.
        eventStore.deleteEventStream("period-2026-Q1")
        assertThat(eventStore.read("period-2026-Q1").version()).isZero()

        // The next period is unaffected by archiving the previous one.
        val afterDeposit = snapshots.executeAndReturnState("period-2026-Q2", Deposit(10), account)
        assertThat(afterDeposit).isEqualTo(LedgerState(balance = 80, closed = false))
    }
}
