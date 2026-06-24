/*
 *
 *  Copyright 2024 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.dsl.decider

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll

// ---------------------------------------------------------------------------
// Tiny self-contained domain
// ---------------------------------------------------------------------------

sealed interface DomainCommand
sealed interface DomainEvent

// Bulb feature
sealed interface BulbCommand : DomainCommand
data object TurnOn : BulbCommand
data object TurnOff : BulbCommand

sealed interface BulbEvent : DomainEvent
data object TurnedOn : BulbEvent
data object TurnedOff : BulbEvent

data class BulbState(val on: Boolean)

// Counter feature — terminal when count >= 2
sealed interface CounterCommand : DomainCommand
data object Increment : CounterCommand

sealed interface CounterEvent : DomainEvent
data object Incremented : CounterEvent

data class CounterState(val count: Int)

// Tag feature (used for Triple / vararg tests)
sealed interface TagCommand : DomainCommand
data class AddTag(val tag: String) : TagCommand

sealed interface TagEvent : DomainEvent
data class TagAdded(val tag: String) : TagEvent

data class TagState(val tags: List<String>)

// ---------------------------------------------------------------------------
// Decider instances (raw, un-adapted)
// ---------------------------------------------------------------------------

private val bulbDecider: Decider<BulbCommand, BulbState, BulbEvent> = decider(
    initialState = BulbState(on = false),
    decide = { cmd, _ ->
        when (cmd) {
            TurnOn  -> listOf(TurnedOn)
            TurnOff -> listOf(TurnedOff)
        }
    },
    evolve = { _, e ->
        when (e) {
            TurnedOn  -> BulbState(on = true)
            TurnedOff -> BulbState(on = false)
        }
    }
)

private val counterDecider: Decider<CounterCommand, CounterState, CounterEvent> = decider(
    initialState = CounterState(count = 0),
    decide = { cmd, _ ->
        when (cmd) {
            Increment -> listOf(Incremented)
        }
    },
    evolve = { state, _ -> CounterState(count = state.count + 1) },
    isTerminal = { it.count >= 2 }
)

private val tagDecider: Decider<TagCommand, TagState, TagEvent> = decider(
    initialState = TagState(tags = emptyList()),
    decide = { cmd, _ ->
        when (cmd) {
            is AddTag -> listOf(TagAdded(cmd.tag))
        }
    },
    evolve = { state, e ->
        when (e) {
            is TagAdded -> TagState(tags = state.tags + e.tag)
        }
    }
)

// Note feature — state is null until the first event, exercising a nullable slice state
sealed interface NoteCommand : DomainCommand
data class WriteNote(val text: String) : NoteCommand

sealed interface NoteEvent : DomainEvent
data class NoteWritten(val text: String) : NoteEvent

private val noteDecider: Decider<NoteCommand, String?, NoteEvent> = decider(
    initialState = null,
    decide = { cmd, _ ->
        when (cmd) {
            is WriteNote -> listOf(NoteWritten(cmd.text))
        }
    },
    evolve = { _, e ->
        when (e) {
            is NoteWritten -> e.text
        }
    }
)

// ---------------------------------------------------------------------------
// Test class
// ---------------------------------------------------------------------------

@DisplayName("Decider Combinators")
@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
class DeciderCombinatorsTest {

    // -----------------------------------------------------------------------
    // adapt
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("adapt")
    inner class Adapt {

        @Test
        fun `foreign command produces no events and leaves state unchanged`() {
            // Given
            val widened: Decider<DomainCommand, BulbState, DomainEvent> = bulbDecider.adapt()

            // When
            val (state, events) = widened.decide(
                state = bulbDecider.initialState(),
                command = Increment  // foreign — not a BulbCommand
            )

            // Then
            assertAll(
                { assertThat(events).isEmpty() },
                { assertThat(state).isEqualTo(BulbState(on = false)) }
            )
        }

        @Test
        fun `own command produces expected event`() {
            // Given
            val widened: Decider<DomainCommand, BulbState, DomainEvent> = bulbDecider.adapt()

            // When
            val (state, events) = widened.decide(
                state = BulbState(on = false),
                command = TurnOn
            )

            // Then
            assertAll(
                { assertThat(events).containsExactly(TurnedOn) },
                { assertThat(state).isEqualTo(BulbState(on = true)) }
            )
        }

        @Test
        fun `foreign events are ignored when folding a mixed stream`() {
            // Given
            val widened: Decider<DomainCommand, BulbState, DomainEvent> = bulbDecider.adapt()

            val mixedHistory: List<DomainEvent> = listOf(TurnedOn, Incremented, TurnedOff, Incremented, TurnedOn)

            // When — fold over mixed stream then turn off
            val (stateFromMixed, _) = widened.decide(events = mixedHistory, command = TurnOff)

            // Compare with folding over only bulb events
            val bulbOnlyHistory: List<DomainEvent> = listOf(TurnedOn, TurnedOff, TurnedOn)
            val (stateFromBulbOnly, _) = widened.decide(events = bulbOnlyHistory, command = TurnOff)

            // Then — both folds must converge to the same final state
            assertThat(stateFromMixed).isEqualTo(stateFromBulbOnly)
        }

        @Test
        fun `isTerminal is preserved through adapt`() {
            // Given
            val widened: Decider<DomainCommand, CounterState, DomainEvent> = counterDecider.adapt()

            // When — a non-terminal state
            val notTerminal = CounterState(count = 1)

            // When — a terminal state
            val terminal = CounterState(count = 2)

            // Then
            assertAll(
                { assertThat(widened.isTerminal(notTerminal)).isFalse() },
                { assertThat(widened.isTerminal(terminal)).isTrue() }
            )
        }
    }

    // -----------------------------------------------------------------------
    // compose — Pair (two deciders)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("compose (Pair)")
    inner class ComposePair {

        private val bulbWidened: Decider<DomainCommand, BulbState, DomainEvent> = bulbDecider.adapt()
        private val counterWidened: Decider<DomainCommand, CounterState, DomainEvent> = counterDecider.adapt()
        private val composed: Decider<DomainCommand, Pair<BulbState, CounterState>, DomainEvent> =
            compose(bulbWidened, counterWidened)

        @Test
        fun `TurnOn routes to bulb slice only — counter slice stays at initial`() {
            // When
            val (state, events) = composed.decide(
                state = composed.initialState(),
                command = TurnOn
            )

            // Then
            assertAll(
                { assertThat(events).containsExactly(TurnedOn) },
                { assertThat(state.first).isEqualTo(BulbState(on = true)) },
                { assertThat(state.second).isEqualTo(CounterState(count = 0)) }
            )
        }

        @Test
        fun `Increment routes to counter slice only — bulb slice stays at initial`() {
            // When
            val (state, events) = composed.decide(
                state = composed.initialState(),
                command = Increment
            )

            // Then
            assertAll(
                { assertThat(events).containsExactly(Incremented) },
                { assertThat(state.first).isEqualTo(BulbState(on = false)) },
                { assertThat(state.second).isEqualTo(CounterState(count = 1)) }
            )
        }

        @Test
        fun `each slice evolves independently when folding a mixed event stream`() {
            // Given — history: bulb turned on then counter incremented once
            val history: List<DomainEvent> = listOf(TurnedOn, Incremented)

            // When
            val (state, events) = composed.decide(events = history, command = TurnOff)

            // Then
            assertAll(
                { assertThat(events).containsExactly(TurnedOff) },
                { assertThat(state.first).isEqualTo(BulbState(on = false)) },
                { assertThat(state.second).isEqualTo(CounterState(count = 1)) }
            )
        }

        @Test
        fun `counter slice is frozen once terminal — further Incremented events have no effect on it`() {
            // Given — two increments bring counter to terminal (count=2)
            val historyToTerminal: List<DomainEvent> = listOf(Incremented, Incremented)

            // When — a third Incremented event arrives in history; TurnOn is the new command
            val (stateAfterThird, _) = composed.decide(
                events = historyToTerminal + listOf(Incremented),
                command = TurnOn
            )

            // When — evolve the composed state directly from a terminal counter to confirm freezing
            val stateAtTerminal = run {
                var s = composed.initialState()
                for (e in historyToTerminal) {
                    s = composed.evolve(s, e)
                }
                s
            }
            val stateAfterExtraEvent = composed.evolve(stateAtTerminal, Incremented)

            // Then — the counter slice stays frozen at exactly 2 in both paths; bulb still evolves
            assertAll(
                { assertThat(stateAfterThird.second).isEqualTo(CounterState(count = 2)) },
                { assertThat(stateAfterExtraEvent.second).isEqualTo(CounterState(count = 2)) },
                { assertThat(stateAfterThird.first).isEqualTo(BulbState(on = true)) }
            )
        }

        @Test
        fun `isTerminal is false when only one sub-decider is terminal`() {
            // Given — counter at terminal state, bulb not terminal
            val partiallyTerminal = BulbState(on = false) to CounterState(count = 2)

            // Then
            assertThat(composed.isTerminal(partiallyTerminal)).isFalse()
        }

        @Test
        fun `isTerminal is true when both sub-deciders are terminal`() {
            // Given — compose two counter deciders so both can be terminal
            val twoCounters: Decider<DomainCommand, Pair<CounterState, CounterState>, DomainEvent> =
                compose(counterWidened, counterWidened)

            val bothTerminal = CounterState(count = 2) to CounterState(count = 2)

            // Then
            assertThat(twoCounters.isTerminal(bothTerminal)).isTrue()
        }
    }

    // -----------------------------------------------------------------------
    // compose — Triple (three deciders)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("compose (Triple)")
    inner class ComposeTriple {

        private val bulbWidened: Decider<DomainCommand, BulbState, DomainEvent> = bulbDecider.adapt()
        private val counterWidened: Decider<DomainCommand, CounterState, DomainEvent> = counterDecider.adapt()
        private val tagWidened: Decider<DomainCommand, TagState, DomainEvent> = tagDecider.adapt()

        private val composed: Decider<DomainCommand, Triple<BulbState, CounterState, TagState>, DomainEvent> =
            compose(bulbWidened, counterWidened, tagWidened)

        @Test
        fun `TurnOn routes to bulb slice — other two slices stay at initial`() {
            // When
            val (state, events) = composed.decide(
                state = composed.initialState(),
                command = TurnOn
            )

            // Then
            assertAll(
                { assertThat(events).containsExactly(TurnedOn) },
                { assertThat(state.first).isEqualTo(BulbState(on = true)) },
                { assertThat(state.second).isEqualTo(CounterState(count = 0)) },
                { assertThat(state.third).isEqualTo(TagState(tags = emptyList())) }
            )
        }

        @Test
        fun `AddTag routes to tag slice — bulb and counter slices stay unchanged`() {
            // When
            val (state, events) = composed.decide(
                state = composed.initialState(),
                command = AddTag("kotlin")
            )

            // Then
            assertAll(
                { assertThat(events).containsExactly(TagAdded("kotlin")) },
                { assertThat(state.first).isEqualTo(BulbState(on = false)) },
                { assertThat(state.second).isEqualTo(CounterState(count = 0)) },
                { assertThat(state.third).isEqualTo(TagState(tags = listOf("kotlin"))) }
            )
        }

        @Test
        fun `Increment routes to counter slice — bulb and tag slices stay unchanged`() {
            // When
            val (state, events) = composed.decide(
                state = composed.initialState(),
                command = Increment
            )

            // Then
            assertAll(
                { assertThat(events).containsExactly(Incremented) },
                { assertThat(state.first).isEqualTo(BulbState(on = false)) },
                { assertThat(state.second).isEqualTo(CounterState(count = 1)) },
                { assertThat(state.third).isEqualTo(TagState(tags = emptyList())) }
            )
        }
    }

    // -----------------------------------------------------------------------
    // compose — list / CompositeState (4 deciders)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("compose (list / CompositeState)")
    inner class ComposeList {

        private val bulbWidened: Decider<DomainCommand, BulbState, DomainEvent> = bulbDecider.adapt()
        private val counterWidened: Decider<DomainCommand, CounterState, DomainEvent> = counterDecider.adapt()
        private val tagWidened: Decider<DomainCommand, TagState, DomainEvent> = tagDecider.adapt()

        // Four-way compose: bulb(0), counter(1), tag(2), bulb again(3)
        private val composed: Decider<DomainCommand, CompositeState, DomainEvent> =
            compose(listOf(bulbWidened, counterWidened, tagWidened, bulbWidened))

        @Test
        fun `initial state has all slices at their own initial state`() {
            // When
            val initial = composed.initialState()

            // Then
            assertAll(
                { assertThat(initial.slice<BulbState>(0)).isEqualTo(BulbState(on = false)) },
                { assertThat(initial.slice<CounterState>(1)).isEqualTo(CounterState(count = 0)) },
                { assertThat(initial.slice<TagState>(2)).isEqualTo(TagState(tags = emptyList())) },
                { assertThat(initial.slice<BulbState>(3)).isEqualTo(BulbState(on = false)) }
            )
        }

        @Test
        fun `TurnOn routes to both bulb slices — counter and tag slices stay at initial`() {
            // When
            val (state, events) = composed.decide(
                state = composed.initialState(),
                command = TurnOn
            )

            // Then — both bulb slices (0 and 3) receive TurnedOn; counter and tag untouched
            assertAll(
                { assertThat(events).containsExactly(TurnedOn, TurnedOn) },
                { assertThat(state.slice<BulbState>(0)).isEqualTo(BulbState(on = true)) },
                { assertThat(state.slice<CounterState>(1)).isEqualTo(CounterState(count = 0)) },
                { assertThat(state.slice<TagState>(2)).isEqualTo(TagState(tags = emptyList())) },
                { assertThat(state.slice<BulbState>(3)).isEqualTo(BulbState(on = true)) }
            )
        }

        @Test
        fun `Increment routes to counter slice only`() {
            // When
            val (state, events) = composed.decide(
                state = composed.initialState(),
                command = Increment
            )

            // Then
            assertAll(
                { assertThat(events).containsExactly(Incremented) },
                { assertThat(state.slice<CounterState>(1)).isEqualTo(CounterState(count = 1)) },
                { assertThat(state.slice<BulbState>(0)).isEqualTo(BulbState(on = false)) },
                { assertThat(state.slice<TagState>(2)).isEqualTo(TagState(tags = emptyList())) },
                { assertThat(state.slice<BulbState>(3)).isEqualTo(BulbState(on = false)) }
            )
        }

        @Test
        fun `states list contains all slices in order`() {
            // When
            val initial = composed.initialState()

            // Then
            assertThat(initial.states()).containsExactly(
                BulbState(on = false),
                CounterState(count = 0),
                TagState(tags = emptyList()),
                BulbState(on = false)
            )
        }

        @Test
        fun `terminal counter slice is frozen while other slices continue to evolve`() {
            // Given — two increments bring counter to terminal
            val history: List<DomainEvent> = listOf(Incremented, Incremented)

            // When — a third Incremented event arrives plus a TurnOn command
            val (state, _) = composed.decide(
                events = history + listOf(Incremented),
                command = TurnOn
            )

            // Then — counter slice stays at 2 (frozen), both bulb slices evolve to on=true
            assertAll(
                { assertThat(state.slice<CounterState>(1)).isEqualTo(CounterState(count = 2)) },
                { assertThat(state.slice<BulbState>(0)).isEqualTo(BulbState(on = true)) },
                { assertThat(state.slice<BulbState>(3)).isEqualTo(BulbState(on = true)) }
            )
        }
    }

    // -----------------------------------------------------------------------
    // edge cases
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("edge cases")
    inner class EdgeCases {

        private val bulbWidened: Decider<DomainCommand, BulbState, DomainEvent> = bulbDecider.adapt()
        private val counterWidened: Decider<DomainCommand, CounterState, DomainEvent> = counterDecider.adapt()
        private val noteWidened: Decider<DomainCommand, String?, DomainEvent> = noteDecider.adapt()

        @Test
        fun `composes a decider whose initial state is null`() {
            // Given — note slice starts null
            val composed: Decider<DomainCommand, Pair<String?, BulbState>, DomainEvent> =
                compose(noteWidened, bulbWidened)

            // Then — the null slice survives construction
            assertThat(composed.initialState().first).isNull()

            // When — write a note
            val (state, events) = composed.decide(state = composed.initialState(), command = WriteNote("hi"))

            // Then — the null slice evolves and the other slice is untouched
            assertAll(
                { assertThat(events).containsExactly(NoteWritten("hi")) },
                { assertThat(state.first).isEqualTo("hi") },
                { assertThat(state.second).isEqualTo(BulbState(on = false)) }
            )
        }

        @Test
        fun `CompositeState exposes its slices as an unmodifiable list`() {
            // Given
            val composed = compose(listOf(bulbWidened, counterWidened, noteWidened, bulbWidened))
            val states = composed.initialState().states()

            // Then — the list cannot be mutated
            @Suppress("UNCHECKED_CAST")
            assertThatThrownBy { (states as MutableList<Any?>).add(BulbState(on = true)) }
                .isInstanceOf(UnsupportedOperationException::class.java)
        }

        @Test
        fun `composing zero deciders yields an empty, vacuously terminal composite`() {
            // Given
            val composed: Decider<DomainCommand, CompositeState, DomainEvent> = compose(emptyList())

            // Then
            assertAll(
                { assertThat(composed.initialState().states()).isEmpty() },
                { assertThat(composed.decide(state = composed.initialState(), command = TurnOn).component2()).isEmpty() },
                { assertThat(composed.isTerminal(composed.initialState())).isTrue() }
            )
        }
    }

    // -----------------------------------------------------------------------
    // auto-adapt (fixed-arity compose adapts narrow deciders itself)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("auto-adapt")
    inner class AutoAdapt {

        @Test
        fun `two narrow deciders compose without an explicit adapt`() {
            // Given — feature deciders over their own narrow command and event types, passed directly
            val composed: Decider<DomainCommand, Pair<BulbState, CounterState>, DomainEvent> =
                compose(bulbDecider, counterDecider)

            // When
            val (state, events) = composed.decide(state = composed.initialState(), command = TurnOn)

            // Then — routed to the bulb slice, counter untouched
            assertAll(
                { assertThat(events).containsExactly(TurnedOn) },
                { assertThat(state.first).isEqualTo(BulbState(on = true)) },
                { assertThat(state.second).isEqualTo(CounterState(count = 0)) }
            )
        }

        @Test
        fun `three narrow deciders compose without an explicit adapt`() {
            // Given
            val composed: Decider<DomainCommand, Triple<BulbState, CounterState, TagState>, DomainEvent> =
                compose(bulbDecider, counterDecider, tagDecider)

            // When
            val (state, events) = composed.decide(state = composed.initialState(), command = AddTag("kotlin"))

            // Then — routed to the tag slice only
            assertAll(
                { assertThat(events).containsExactly(TagAdded("kotlin")) },
                { assertThat(state.first).isEqualTo(BulbState(on = false)) },
                { assertThat(state.second).isEqualTo(CounterState(count = 0)) },
                { assertThat(state.third).isEqualTo(TagState(tags = listOf("kotlin"))) }
            )
        }

        @Test
        fun `auto-adapted slices ignore foreign events when folding a mixed stream`() {
            // Given
            val composed: Decider<DomainCommand, Pair<BulbState, CounterState>, DomainEvent> =
                compose(bulbDecider, counterDecider)

            // When — fold a stream mixing both features' events, then decide
            val (state, _) = composed.decide(events = listOf(TurnedOn, Incremented), command = TurnOff)

            // Then — each slice only advanced on its own events
            assertAll(
                { assertThat(state.first).isEqualTo(BulbState(on = false)) },
                { assertThat(state.second).isEqualTo(CounterState(count = 1)) }
            )
        }
    }
}
