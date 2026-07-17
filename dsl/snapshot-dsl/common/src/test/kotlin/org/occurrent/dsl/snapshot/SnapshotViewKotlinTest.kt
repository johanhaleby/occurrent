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

package org.occurrent.dsl.snapshot

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class SnapshotViewKotlinTest {

    sealed interface LedgerEvent
    data class Deposited(val amount: Int) : LedgerEvent
    data class Withdrawn(val amount: Int) : LedgerEvent

    @Test
    fun `snapshotView dsl builds a folding view with a schema version`() {
        val view = snapshotView<Int, LedgerEvent>(initialState = 0) {
            schemaVersion(3)
            on<Deposited> { balance, e -> balance + e.amount }
            on<Withdrawn> { balance, e -> balance - e.amount }
        }

        assertThat(view.schemaVersion()).isEqualTo(3)
        assertThat(view.eventTypes()).containsExactlyInAnyOrder(Deposited::class.java, Withdrawn::class.java)
        assertThat(view.view().evolve(listOf(Deposited(100), Withdrawn(40)))).isEqualTo(60)
    }
}
