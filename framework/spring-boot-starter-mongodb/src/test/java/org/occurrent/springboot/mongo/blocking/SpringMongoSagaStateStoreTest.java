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

package org.occurrent.springboot.mongo.blocking;

import org.occurrent.dsl.saga.mongodb.spring.SpringMongoSagaStateStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.dsl.saga.flow.FlowState;
import org.springframework.data.mongodb.core.MongoOperations;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * Docker-free unit checks for {@link SpringMongoSagaStateStore}'s constructor guards. The guard runs before any MongoDB
 * access, so a mock {@link MongoOperations} that is never touched is enough.
 */
@DisplayName("SpringMongoSagaStateStore construction")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoSagaStateStoreTest {

    private final MongoOperations mongoOperations = mock(MongoOperations.class);

    @Test
    void the_three_argument_constructor_rejects_a_FlowState_store_because_it_has_no_CloudEventConverter() {
        assertThatThrownBy(() -> new SpringMongoSagaStateStore<>(mongoOperations, "saga-orders", FlowState.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CloudEventConverter")
                .hasMessageContaining("flow saga");
    }

    @Test
    void the_four_argument_constructor_rejects_a_FlowState_store_with_a_null_CloudEventConverter() {
        assertThatThrownBy(() -> new SpringMongoSagaStateStore<>(mongoOperations, "saga-orders", FlowState.class, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CloudEventConverter");
    }

    @Test
    void a_null_stateType_is_rejected_before_the_flow_state_guard() {
        assertThatThrownBy(() -> new SpringMongoSagaStateStore<>(mongoOperations, "saga-orders", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("stateType");
    }

    @Test
    void a_null_mongoOperations_is_rejected() {
        assertThatCode(() -> new SpringMongoSagaStateStore<>(null, "saga-orders", FlowState.class))
                .isInstanceOf(NullPointerException.class);
    }
}
