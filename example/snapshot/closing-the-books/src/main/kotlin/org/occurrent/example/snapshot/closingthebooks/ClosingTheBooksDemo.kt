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
import org.occurrent.dsl.snapshot.blocking.SnapshotDecider
import org.occurrent.dsl.snapshot.blocking.SnapshotDeciderApplicationService
import org.occurrent.dsl.snapshot.blocking.SnapshotPolicies
import org.occurrent.dsl.snapshot.blocking.SnapshotStore
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

    // One facade, reused for every aggregate. Each use case brings its own SnapshotDecider spec.
    val snapshots = SnapshotDeciderApplicationService(applicationService)

    // 1. Technical snapshot: take one every 100 events so a long-lived account does not replay its whole history.
    val technical = SnapshotDecider.from(decider, store, SnapshotOptions.everyNEvents<LedgerState, LedgerEvent>(1, 100))
    repeat(250) { snapshots.execute("account-1", Deposit(1), technical) }
    val technicalSnapshot = store.findLatest("account-1").orElseThrow()
    println("Technical snapshot for account-1 sits at version ${technicalSnapshot.version()} with balance ${technicalSnapshot.state().balance}")
    val resumed = snapshots.executeAndReturnState("account-1", Deposit(5), technical)
    println("Next command resumed from the snapshot and only folded the tail, new balance ${resumed.balance}")

    // 2. Closing the books: snapshot the terminal state, carry the closing balance forward, then archive the old events.
    val onClose = SnapshotDecider.from(decider, store, SnapshotOptions.of(1, SnapshotPolicies.whenTerminal(decider).or(SnapshotPolicy.everyNEvents<LedgerState, LedgerEvent>(100))))
    snapshots.execute("period-2026-Q1", Deposit(100), onClose)
    snapshots.execute("period-2026-Q1", Withdraw(30), onClose)
    snapshots.execute("period-2026-Q1", CloseBooks("2026-Q1"), onClose)
    val closingBalance = store.findLatest("period-2026-Q1").orElseThrow().state().balance
    println("Closed 2026-Q1 with a closing balance of $closingBalance")

    val opening = snapshots.executeAndReturnState("period-2026-Q2", SetOpeningBalance(closingBalance), onClose)
    println("Opened 2026-Q2 carrying the balance forward, opening balance ${opening.balance}")

    eventStore.deleteEventStream("period-2026-Q1")
    println("Archived the 2026-Q1 events, the opening balance now lives as an event in 2026-Q2")
}
