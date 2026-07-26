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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("SagaInstances")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaInstancesTest {

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

    /**
     * A store implementing only the contract the executor needs, without the optional
     * {@link SagaStateStoreQueries} capability. This is what an out-of-tree store, or the planned SQL store before it
     * grows enumeration, looks like.
     */
    private static final class CoreOnlyStore implements SagaStateStore<String> {

        private final SagaEnvelope<String> only = new SagaEnvelope<>("s1", "a", SagaStatus.ACTIVE, 1,
                List.of(), Map.of(), null, NOW, NOW, null, null);

        @Override
        public Optional<SagaEnvelope<String>> find(String sagaId) {
            return sagaId.equals("s1") ? Optional.of(only) : Optional.empty();
        }

        @Override
        public boolean compareAndSave(String sagaId, SagaEnvelope<String> envelope, long expectedVersion) {
            return true;
        }

        @Override
        public List<SagaEnvelope<String>> findWithDueTimers(Instant now, int limit) {
            return List.of();
        }

        @Override
        public void delete(String sagaId) {
        }
    }

    @Nested
    class WithoutTheQueriesCapability {

        private final SagaInstances instances = SagaInstances.of(new CoreOnlyStore());

        /**
         * The capability is checked when enumeration is attempted, not when the facade is built, so a store that only
         * lacks enumeration still supports every by-id question.
         */
        @Test
        void a_by_id_lookup_still_works() {
            assertThat(instances.find("s1")).isPresent();
            assertThat(instances.find("missing")).isEmpty();
        }

        @Test
        void enumerating_fails_fast_naming_the_store_and_the_capability_it_lacks() {
            assertThatThrownBy(() -> instances.findByStatus(SagaStatus.ACTIVE, NOW, 10))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("SagaStateStoreQueries")
                    .hasMessageContaining(CoreOnlyStore.class.getName());
        }
    }

    @Nested
    class WithTheQueriesCapability {

        private final SagaStateStore<String> store = SagaStateStore.inMemory();
        private final SagaInstances instances = SagaInstances.of(store);

        @Test
        void enumerates_through_the_capability() {
            store.compareAndSave("s1", new SagaEnvelope<>("s1", "a", SagaStatus.ACTIVE, 1,
                    List.of(), Map.of(), null, NOW, NOW, null, null), 0);

            assertThat(instances.findByStatus(SagaStatus.ACTIVE, NOW.plusSeconds(1), 10))
                    .extracting(SagaInstance::sagaId).containsExactly("s1");
        }
    }
}
