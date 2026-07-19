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

package org.occurrent.dsl.projection;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.filter.Filter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbProjectionTest {

    sealed interface AccountEvent permits AccountRegistered {
        String accountId();
    }

    record AccountRegistered(String accountId, String username) implements AccountEvent {
    }

    @Test
    void rejects_a_projection_with_an_explicit_filter_because_it_would_silently_be_ignored() {
        Projection<Boolean, AccountEvent, String> projectionWithFilter = Projection.<Boolean, AccountEvent, String>builder(false)
                .id(AccountEvent::accountId)
                .on(AccountRegistered.class, (state, event) -> true)
                .filter(Filter.type("AccountRegistered"))
                .build();

        Throwable thrown = catchThrowable(() -> new DcbProjection<>(projectionWithFilter, DcbCriteria.all()));

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("filter");
    }

    @Test
    void accepts_a_projection_without_an_explicit_filter() {
        Projection<Boolean, AccountEvent, String> projectionWithoutFilter = Projection.<Boolean, AccountEvent, String>builder(false)
                .id(AccountEvent::accountId)
                .on(AccountRegistered.class, (state, event) -> true)
                .build();

        Throwable thrown = catchThrowable(() -> new DcbProjection<>(projectionWithoutFilter, DcbCriteria.all()));

        assertThat(thrown).isNull();
    }
}
