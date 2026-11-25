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

package org.occurrent.dsl.saga;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.dsl.saga.codec.SagaCodec;
import org.occurrent.dsl.saga.codec.StoredSaga;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * The in-memory store has no size limit, so it answers that every write fits, whether or not it holds the instance and
 * whatever version the write expects.
 */
@DisplayName("InMemorySagaStateStore asking whether a write without the state fits")
@DisplayNameGeneration(ReplaceUnderscores.class)
class InMemorySagaStateStoreFitsWithoutStateTest {

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

    private final InMemorySagaStateStore<String> store = new InMemorySagaStateStore<>();
    private final SagaCodec<String> codec = SagaCodec.of(store.stateCodec(), null);

    @Test
    void every_write_fits_since_the_store_has_no_size_limit_whether_the_instance_is_stored_at_that_version_at_another_or_not_at_all() {
        store.compareAndSave(codec.encode(active("s1", "state", 1)), 0);
        StoredSaga write = codec.encodeWithoutState(active("s1", "ignored", 2));
        StoredSaga ofAnotherInstance = codec.encodeWithoutState(active("s2", "ignored", 2));

        assertAll(
                () -> assertThat(store.fitsWithoutState(write, 1)).as("stored at the expected version").isTrue(),
                () -> assertThat(store.fitsWithoutState(write, 7)).as("stored at another version").isTrue(),
                () -> assertThat(store.fitsWithoutState(ofAnotherInstance, 1)).as("not stored").isTrue()
        );
    }

    private static SagaEnvelope<String> active(String sagaId, String state, long version) {
        return new SagaEnvelope<>(sagaId, state, SagaStatus.ACTIVE, version, List.of(), Map.of(), null, NOW, NOW, null, null, true, null, Map.of(), null);
    }
}
