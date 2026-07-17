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

package org.occurrent.dsl.decider;

import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Decider.evolve on a list of events")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class DeciderEvolveListTest {

    private final Decider<Void, Integer, Integer> counter = new Decider<>() {
        @Override
        public Integer initialState() {
            return 0;
        }

        @NonNull
        @Override
        public List<Integer> decide(@NonNull Void command, Integer state) {
            return List.of();
        }

        @Override
        public Integer evolve(Integer state, @NonNull Integer event) {
            return state + event;
        }

        @Override
        public boolean isTerminal(Integer state) {
            return state >= 100;
        }
    };

    @Test
    void folds_a_tail_onto_a_given_base_state() {
        int result = counter.evolve(10, List.of(1, 2, 3));
        assertThat(result).isEqualTo(16);
    }

    @Test
    void stops_at_the_first_terminal_state() {
        // 90 + 20 = 110 is terminal, so the trailing 5 is never applied.
        int result = counter.evolve(90, List.of(20, 5));
        assertThat(result).isEqualTo(110);
    }
}
