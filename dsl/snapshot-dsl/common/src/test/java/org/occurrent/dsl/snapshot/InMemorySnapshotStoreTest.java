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

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class InMemorySnapshotStoreTest {

    private final SnapshotStore<Integer> store = SnapshotStore.inMemory();

    @Test
    void find_latest_is_empty_for_an_unknown_key() {
        assertThat(store.findLatest("nope")).isEmpty();
    }

    @Test
    void save_then_find_latest_round_trips() {
        store.save("acc:1", new Snapshot<>(42, 7, 1));

        assertThat(store.findLatest("acc:1")).contains(new Snapshot<>(42, 7, 1));
    }

    @Test
    void save_overwrites_the_previous_snapshot_for_the_key() {
        store.save("acc:1", new Snapshot<>(42, 7, 1));
        store.save("acc:1", new Snapshot<>(99, 12, 1));

        assertThat(store.findLatest("acc:1")).contains(new Snapshot<>(99, 12, 1));
    }

    @Test
    void delete_removes_the_snapshot() {
        store.save("acc:1", new Snapshot<>(42, 7, 1));

        store.delete("acc:1");

        assertThat(store.findLatest("acc:1")).isEmpty();
    }

    @Test
    void keys_are_independent() {
        store.save("acc:1", new Snapshot<>(1, 1, 1));
        store.save("acc:2", new Snapshot<>(2, 1, 1));

        assertThat(store.findLatest("acc:1")).contains(new Snapshot<>(1, 1, 1));
        assertThat(store.findLatest("acc:2")).contains(new Snapshot<>(2, 1, 1));
    }

    @Test
    void a_nullable_state_round_trips() {
        SnapshotStore<Optional<String>> nullableStore = SnapshotStore.inMemory();

        nullableStore.save("k", new Snapshot<>(Optional.empty(), 3, 1));

        assertThat(nullableStore.findLatest("k")).contains(new Snapshot<>(Optional.empty(), 3, 1));
    }
}
