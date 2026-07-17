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

import org.occurrent.dsl.decider.Decider
import org.occurrent.dsl.decider.decider

// The event classes are top-level so the reflection type mapper can resolve them by simple name.
sealed interface LedgerEvent
data class OpeningBalanceSet(val amount: Long) : LedgerEvent
data class MoneyDeposited(val amount: Long) : LedgerEvent
data class MoneyWithdrawn(val amount: Long) : LedgerEvent
data class BooksClosed(val closingBalance: Long, val period: String) : LedgerEvent

sealed interface LedgerCommand
data class SetOpeningBalance(val amount: Long) : LedgerCommand
data class Deposit(val amount: Long) : LedgerCommand
data class Withdraw(val amount: Long) : LedgerCommand
data class CloseBooks(val period: String) : LedgerCommand

data class LedgerState(val balance: Long, val closed: Boolean)

/**
 * A ledger decider. Its state is the running balance, and it becomes terminal once the books are closed. The terminal
 * state is the "close the books" signal that a snapshot policy can react to.
 */
fun ledgerDecider(): Decider<LedgerCommand, LedgerState, LedgerEvent> = decider(
    initialState = LedgerState(balance = 0, closed = false),
    decide = { command, state ->
        require(!state.closed) { "The books are closed, no further commands are accepted" }
        when (command) {
            is SetOpeningBalance -> {
                require(state.balance == 0L) { "Opening balance can only be set on a fresh ledger" }
                listOf(OpeningBalanceSet(command.amount))
            }
            is Deposit -> listOf(MoneyDeposited(command.amount))
            is Withdraw -> {
                require(command.amount <= state.balance) { "Cannot withdraw more than the current balance" }
                listOf(MoneyWithdrawn(command.amount))
            }
            is CloseBooks -> listOf(BooksClosed(state.balance, command.period))
        }
    },
    evolve = { state, event ->
        when (event) {
            is OpeningBalanceSet -> state.copy(balance = event.amount)
            is MoneyDeposited -> state.copy(balance = state.balance + event.amount)
            is MoneyWithdrawn -> state.copy(balance = state.balance - event.amount)
            is BooksClosed -> state.copy(closed = true)
        }
    },
    isTerminal = { state -> state.closed }
)
