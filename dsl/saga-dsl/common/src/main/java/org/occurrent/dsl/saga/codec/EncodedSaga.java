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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * The stored form of one saga instance, as fields in the order a store should write them.
 * <p>
 * A value is a {@code String}, {@code Integer}, {@code Long}, {@code Boolean}, {@code List} or {@code Map} of those, or
 * an {@link java.time.Instant} or an {@link io.cloudevents.CloudEvent}. The {@code state} field holds what the {@link SagaStateCodec} produced, which can be
 * any value. A field that has no value is left out.
 *
 * @param fields the fields by name, without the saga id
 */
public record EncodedSaga(Map<String, Object> fields) {

    public EncodedSaga {
        requireNonNull(fields, "fields cannot be null");
        fields = Collections.unmodifiableMap(new LinkedHashMap<>(fields));
    }
}
