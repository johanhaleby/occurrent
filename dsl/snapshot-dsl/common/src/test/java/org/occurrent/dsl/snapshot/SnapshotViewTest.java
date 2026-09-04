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

package org.occurrent.dsl.snapshot;

import org.junit.jupiter.api.Test;
import org.occurrent.dsl.snapshot.LedgerFixture.BooksClosed;
import org.occurrent.dsl.snapshot.LedgerFixture.Deposited;
import org.occurrent.dsl.snapshot.LedgerFixture.LedgerEvent;
import org.occurrent.dsl.snapshot.LedgerFixture.Withdrawn;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SnapshotViewTest {

    private static SnapshotView<Integer, LedgerEvent> balanceView() {
        return SnapshotView.<Integer, LedgerEvent>builder(0)
                .schemaVersion(2)
                .on(Deposited.class, (balance, e) -> balance + e.amount())
                .on(Withdrawn.class, (balance, e) -> balance - e.amount())
                .build();
    }

    @Test
    void folds_registered_event_types_and_no_ops_the_rest() {
        SnapshotView<Integer, LedgerEvent> view = balanceView();

        Integer balance = view.view().evolve(List.of(new Deposited(100), new Withdrawn(30), new BooksClosed(70)));

        assertThat(balance).isEqualTo(70);
    }

    @Test
    void captures_schema_version_and_handled_event_types() {
        SnapshotView<Integer, LedgerEvent> view = balanceView();

        assertThat(view.schemaVersion()).isEqualTo(2);
        assertThat(view.eventTypes()).containsExactlyInAnyOrder(Deposited.class, Withdrawn.class);
        assertThat(view.filter()).isNull();
    }

    @Test
    void schema_version_defaults_to_one() {
        SnapshotView<Integer, LedgerEvent> view = SnapshotView.<Integer, LedgerEvent>builder(0)
                .on(Deposited.class, (balance, e) -> balance + e.amount())
                .build();

        assertThat(view.schemaVersion()).isEqualTo(1);
    }

    @Test
    void builder_with_no_argument_starts_from_null_like_builder_of_null() {
        SnapshotView<Integer, LedgerEvent> view = SnapshotView.<Integer, LedgerEvent>builder()
                .on(Deposited.class, (balance, e) -> balance + e.amount())
                .build();

        assertThat(view.view().initialState()).isNull();
    }

    @Test
    void adapt_widens_the_event_type_and_ignores_foreign_events() {
        SnapshotView<Integer, Deposited> deposits = SnapshotView.<Integer, Deposited>builder(0)
                .on(Deposited.class, (balance, e) -> balance + e.amount())
                .build();

        SnapshotView<Integer, LedgerEvent> widened = SnapshotView.adapt(deposits, Deposited.class);

        Integer balance = widened.view().evolve(List.of(new Deposited(10), new Withdrawn(999), new Deposited(5)));
        assertThat(balance).isEqualTo(15);
    }
}
