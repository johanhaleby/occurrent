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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * The stored form of one saga instance, as fields in the order a store should write them.
 * <p>
 * A value is a {@code String}, {@code Integer}, {@code Long}, {@code Boolean}, {@code List} or {@code Map} of those, or
 * an {@link java.time.Instant} or an {@link io.cloudevents.CloudEvent}. The {@code state} field holds what the {@link SagaStateCodec} produced, which can be
 * any value. A field that has no value is left out.
 * <p>
 * The fields are copied when an {@code EncodedSaga} is created, and so is every {@code List} and {@code Map} in them,
 * at any depth, into a copy that cannot be changed. A change the caller makes afterwards to a map or list it passed in
 * does not reach the {@code EncodedSaga}. The value of the {@code state} field is kept as the {@link SagaStateCodec} returned it, since it can be any
 * object.
 *
 * @param fields the fields by name, without the saga id
 */
public record EncodedSaga(Map<String, Object> fields) {

    public EncodedSaga {
        requireNonNull(fields, "fields cannot be null");
        Map<String, Object> copy = new LinkedHashMap<>();
        fields.forEach((field, value) -> copy.put(field, SagaCodec.STATE.equals(field) ? value : frozen(value)));
        fields = Collections.unmodifiableMap(copy);
    }

    // Copied rather than wrapped, so a list or map the caller keeps cannot change the stored form. Null entries are
    // kept, which List.copyOf and Map.copyOf refuse
    static @Nullable Object frozen(@Nullable Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<Object, @Nullable Object> copy = new LinkedHashMap<>();
            map.forEach((key, entry) -> copy.put(key, frozen(entry)));
            return Collections.unmodifiableMap(copy);
        }
        if (value instanceof List<?> list) {
            List<@Nullable Object> copy = new ArrayList<>(list.size());
            list.forEach(entry -> copy.add(frozen(entry)));
            return Collections.unmodifiableList(copy);
        }
        return value;
    }
}
