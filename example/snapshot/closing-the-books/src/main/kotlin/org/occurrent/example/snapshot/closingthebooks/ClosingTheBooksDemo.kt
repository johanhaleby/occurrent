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

import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper
import org.occurrent.application.service.blocking.generic.GenericApplicationService
import org.occurrent.dsl.snapshot.SnapshotOptions
import org.occurrent.dsl.snapshot.SnapshotPolicy
import org.occurrent.dsl.snapshot.SnapshotStore
import org.occurrent.dsl.snapshot.blocking.SnapshotDeciderApplicationService
import org.occurrent.dsl.snapshot.blocking.SnapshotPolicies
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import tools.jackson.module.kotlin.jacksonObjectMapper
import java.net.URI

/**
 * Runs the two snapshot styles this module demonstrates and prints what happens. See [ClosingTheBooksTest] for the same
 * flow expressed as assertions.
 */
fun main() {
    val eventStore = InMemoryEventStore()
    val converter = JacksonCloudEventConverter.Builder<LedgerEvent>(jacksonObjectMapper(), URI.create("urn:occurrent:example:snapshot"))
        .typeMapper(ReflectionCloudEventTypeMapper.simple(LedgerEvent::class.java))
        .build()
    val applicationService = GenericApplicationService(eventStore, converter)
    val decider = ledgerDecider()
    val store = SnapshotStore.inMemory<LedgerState>()

    // 1. Technical snapshot: take one every 100 events so a long-lived account does not replay its whole history.
    val everyHundred = SnapshotOptions.everyNEvents<LedgerState, LedgerEvent>(1, 100)
    val technicalSnapshots = SnapshotDeciderApplicationService(applicationService, store, everyHundred)
    repeat(250) { technicalSnapshots.execute("account-1", Deposit(1), decider) }
    val technical = store.findLatest("account-1").orElseThrow()
    println("Technical snapshot for account-1 sits at version ${technical.version()} with balance ${technical.state().balance}")
    val resumed = technicalSnapshots.executeAndReturnState("account-1", Deposit(5), decider)
    println("Next command resumed from the snapshot and only folded the tail, new balance ${resumed.balance}")

    // 2. Closing the books: snapshot the terminal state, carry the closing balance forward, then archive the old events.
    val onClose = SnapshotOptions.of(1, SnapshotPolicies.whenTerminal(decider).or(SnapshotPolicy.everyNEvents<LedgerState, LedgerEvent>(100)))
    val closingSnapshots = SnapshotDeciderApplicationService(applicationService, store, onClose)
    closingSnapshots.execute("period-2026-Q1", Deposit(100), decider)
    closingSnapshots.execute("period-2026-Q1", Withdraw(30), decider)
    closingSnapshots.execute("period-2026-Q1", CloseBooks("2026-Q1"), decider)
    val closingBalance = store.findLatest("period-2026-Q1").orElseThrow().state().balance
    println("Closed 2026-Q1 with a closing balance of $closingBalance")

    val opening = closingSnapshots.executeAndReturnState("period-2026-Q2", SetOpeningBalance(closingBalance), decider)
    println("Opened 2026-Q2 carrying the balance forward, opening balance ${opening.balance}")

    eventStore.deleteEventStream("period-2026-Q1")
    println("Archived the 2026-Q1 events, the opening balance now lives as an event in 2026-Q2")
}
