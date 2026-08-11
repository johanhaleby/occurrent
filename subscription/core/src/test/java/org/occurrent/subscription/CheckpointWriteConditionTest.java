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

package org.occurrent.subscription;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
class CheckpointWriteConditionTest {

    @Nested
    class NotOlderThan {

        @ParameterizedTest
        @ValueSource(longs = {0L, 1L, Long.MAX_VALUE})
        void accepts_a_non_negative_write_version(long writeVersion) {
            CheckpointWriteCondition.NotOlderThan condition = (CheckpointWriteCondition.NotOlderThan) CheckpointWriteCondition.notOlderThan(writeVersion);

            assertThat(condition.writeVersion()).isEqualTo(writeVersion);
        }

        @ParameterizedTest
        @ValueSource(longs = {-1L, -2L, Long.MIN_VALUE})
        void refuses_a_negative_write_version_since_a_storage_is_entitled_to_reserve_it_as_an_internal_sentinel(long writeVersion) {
            assertThatThrownBy(() -> CheckpointWriteCondition.notOlderThan(writeVersion))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("writeVersion must be non-negative but was " + writeVersion);
        }

        @Test
        void refuses_a_negative_write_version_constructed_directly_so_the_static_factory_is_not_the_only_gate() {
            assertThatThrownBy(() -> new CheckpointWriteCondition.NotOlderThan(-1L))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("writeVersion must be non-negative but was -1");
        }
    }
}
