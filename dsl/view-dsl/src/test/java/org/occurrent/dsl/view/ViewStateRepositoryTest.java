/*
 *
 *  Copyright 2026 Johan Haleby
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

package org.occurrent.dsl.view;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class ViewStateRepositoryTest {

    @Test
    void the_default_find_all_by_id_loops_find_by_id_and_omits_ids_with_no_state() {
        Map<String, Integer> store = new ConcurrentHashMap<>(Map.of("a", 1, "b", 2));
        AtomicInteger findByIdCalls = new AtomicInteger();
        ViewStateRepository<Integer, String> repository = new ViewStateRepository<>() {
            @Override
            public Optional<Integer> findById(String id) {
                findByIdCalls.incrementAndGet();
                return Optional.ofNullable(store.get(id));
            }

            @Override
            public void save(String id, Integer state) {
                store.put(id, state);
            }
        };

        Map<String, Integer> result = repository.findAllById(List.of("a", "b", "missing"));

        assertThat(result).containsOnly(Map.entry("a", 1), Map.entry("b", 2));
        assertThat(findByIdCalls.get()).isEqualTo(3);
    }

    @Test
    void the_default_save_all_loops_save_one_id_at_a_time() {
        Map<String, Integer> store = new ConcurrentHashMap<>();
        AtomicInteger saveCalls = new AtomicInteger();
        ViewStateRepository<Integer, String> repository = new ViewStateRepository<>() {
            @Override
            public Optional<Integer> findById(String id) {
                return Optional.ofNullable(store.get(id));
            }

            @Override
            public void save(String id, Integer state) {
                saveCalls.incrementAndGet();
                store.put(id, state);
            }
        };

        repository.saveAll(Map.of("a", 1, "b", 2, "c", 3));

        assertThat(store).containsOnly(Map.entry("a", 1), Map.entry("b", 2), Map.entry("c", 3));
        assertThat(saveCalls.get()).isEqualTo(3);
    }

    @Test
    void a_repository_built_from_two_lambdas_still_gets_the_looping_find_all_by_id_and_save_all_defaults() {
        Map<String, Integer> store = new ConcurrentHashMap<>(Map.of("a", 1));
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(store::get, store::put);

        Map<String, Integer> found = repository.findAllById(List.of("a", "missing"));
        repository.saveAll(Map.of("b", 2));

        assertThat(found).containsOnly(Map.entry("a", 1));
        assertThat(store).containsOnly(Map.entry("a", 1), Map.entry("b", 2));
    }
}
