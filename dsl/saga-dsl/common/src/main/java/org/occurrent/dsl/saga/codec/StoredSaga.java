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

import static java.util.Objects.requireNonNull;

/**
 * One saga instance as a store keeps it. A store persists the id, the version, the encoded form and the index, and
 * nothing else.
 *
 * @param sagaId  the saga instance id
 * @param version the optimistic-lock version
 * @param encoded the stored form
 * @param index   what the store queries on
 */
public record StoredSaga(String sagaId, long version, EncodedSaga encoded, SagaIndex index) {

    public StoredSaga {
        requireNonNull(sagaId, "sagaId cannot be null");
        requireNonNull(encoded, "encoded cannot be null");
        requireNonNull(index, "index cannot be null");
    }
}
