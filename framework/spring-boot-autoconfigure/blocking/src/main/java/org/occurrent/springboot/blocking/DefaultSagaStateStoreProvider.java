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

package org.occurrent.springboot.blocking;

import org.occurrent.dsl.saga.SagaStateStore;

/**
 * The zero-config saga state store a store starter contributes for a {@code @Saga} that declares none. Optional: a
 * starter that contributes no such bean simply has no default, and a saga without a store bean fails with an
 * actionable message instead.
 * <p>
 * Where the state is persisted, under what collection or table name, and how a flow saga's received events are
 * serialized, is the implementation's business. This module never names a store.
 */
public interface DefaultSagaStateStoreProvider {

    /**
     * @param sagaId    the {@code @Saga} id, so an implementation can derive a per-saga storage name
     * @param stateType the saga state type, reflected from the factory return type
     */
    <S> SagaStateStore<S> createDefaultSagaStateStore(String sagaId, Class<S> stateType);
}
