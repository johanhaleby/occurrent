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

package org.occurrent.dsl.saga.mongodb.spring;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoOperations;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoSagaStateStoreConstructorTest {

    @Test
    void refuses_a_mongo_operations_that_is_not_a_mongo_template_and_names_its_class_and_the_bean_to_pass() {
        MongoOperations notATemplate = mock(MongoOperations.class);

        assertThatThrownBy(() -> new SpringMongoSagaStateStore<>(notATemplate, "sagas", String.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("was given a " + notATemplate.getClass().getName())
                .hasMessageContaining("Pass the MongoTemplate bean.");
    }
}
