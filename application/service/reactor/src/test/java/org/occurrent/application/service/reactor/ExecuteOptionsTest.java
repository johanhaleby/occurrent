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

package org.occurrent.application.service.reactor;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.domain.DomainEvent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("ExecuteOptions")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ExecuteOptionsTest {

    @Nested
    @DisplayName("when setting fromStreamVersion")
    class When_setting_from_stream_version {

        @Test
        void retains_the_value() {
            var executeOptions = ExecuteOptions.<DomainEvent>options().fromStreamVersion(42L);

            assertThat(executeOptions.fromStreamVersion()).isEqualTo(42L);
        }

        @Test
        void rejects_a_negative_value() {
            assertThatThrownBy(() -> ExecuteOptions.<DomainEvent>options().fromStreamVersion(-1L))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("cannot be negative");
        }

        @Test
        void rejects_a_value_above_int_max() {
            assertThatThrownBy(() -> ExecuteOptions.<DomainEvent>options().fromStreamVersion((long) Integer.MAX_VALUE + 1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Integer.MAX_VALUE")
                    .hasMessageContaining("skip");
        }
    }
}
