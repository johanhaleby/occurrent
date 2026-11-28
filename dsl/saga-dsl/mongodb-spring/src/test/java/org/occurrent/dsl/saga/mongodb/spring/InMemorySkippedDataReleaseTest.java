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
import org.occurrent.dsl.saga.InMemorySagaStateStore;
import org.occurrent.dsl.saga.SagaStateStore;

/**
 * {@code InMemorySagaStateStore} copies a map it can copy, such as one with a {@code BigDecimal}, as a
 * {@code PojoCloudEventData}, and holds every other data it can't copy as its bytes. It has no size limit.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class InMemorySkippedDataReleaseTest extends SkippedDataReleaseContract {

    @Override
    SagaStateStore<String> newStore() {
        return new InMemorySagaStateStore<>();
    }

    @Override
    boolean copiesMapsBsonCannotHold() {
        return true;
    }
}
