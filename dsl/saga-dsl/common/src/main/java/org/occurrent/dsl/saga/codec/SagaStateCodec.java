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

package org.occurrent.dsl.saga.codec;

import org.jspecify.annotations.Nullable;

/**
 * Turns the user state of a saga into a value a store can persist, and back. The store supplies it, because how a user
 * state is represented depends on the store. The state of a flow saga never reaches it, since {@link SagaCodec} encodes
 * that one itself.
 *
 * @param <S> the user state type
 */
public interface SagaStateCodec<S extends @Nullable Object> {

    /** The stored form of {@code state}, which is never {@code null}. */
    @Nullable Object encode(S state);

    /** The state that {@code encoded} stands for, where {@code encoded} is a value {@link #encode(Object)} returned or a store read back. */
    S decode(Object encoded);

    /** A codec that keeps the state object as it is, for a store that holds objects in memory. */
    static <S extends @Nullable Object> SagaStateCodec<S> identity() {
        return new SagaStateCodec<>() {
            @Override
            public @Nullable Object encode(S state) {
                return state;
            }

            @SuppressWarnings("unchecked")
            @Override
            public S decode(Object encoded) {
                return (S) encoded;
            }
        };
    }
}
