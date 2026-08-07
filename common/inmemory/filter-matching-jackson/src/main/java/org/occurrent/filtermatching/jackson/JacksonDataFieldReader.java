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

package org.occurrent.filtermatching.jackson;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import org.jspecify.annotations.Nullable;
import org.occurrent.filtermatching.DataFieldReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A {@link DataFieldReader} that parses the event payload as JSON using Jackson.
 * <p>
 * Only a payload the event itself claims is JSON gets parsed. That follows the same rule MongoDB uses. A missing
 * {@code datacontenttype} means JSON per the CloudEvents spec, and a content type naming {@code json} (for example
 * {@code application/json} or a {@code +json} suffix) also means JSON. Anything else, for example {@code text/plain}, is
 * left alone even if the bytes happen to look like JSON, so a plain-text payload that happens to contain braces never
 * matches a data filter here when it would not match on MongoDB either.
 * <p>
 * A payload that fails to parse, or whose root is not a JSON object, answers {@link Optional#empty()} rather than
 * throwing, because one malformed event must not break a query over the whole store.
 */
public class JacksonDataFieldReader implements DataFieldReader {

    private final ObjectMapper objectMapper;

    /**
     * Create a reader backed by a plain, default-configured {@link ObjectMapper}.
     */
    public JacksonDataFieldReader() {
        this(new ObjectMapper());
    }

    /**
     * Create a reader backed by a caller-supplied {@link ObjectMapper}, so it can share configuration (modules,
     * naming strategy, and so on) with the rest of the application.
     *
     * @param objectMapper the mapper to parse payloads with
     */
    public JacksonDataFieldReader(ObjectMapper objectMapper) {
        this.objectMapper = Objects.requireNonNull(objectMapper, ObjectMapper.class.getSimpleName() + " cannot be null");
    }

    @Override
    public Optional<Object> read(CloudEvent cloudEvent, String path) {
        Objects.requireNonNull(cloudEvent, "cloudEvent cannot be null");
        Objects.requireNonNull(path, "path cannot be null");

        if (!isJson(cloudEvent.getDataContentType())) {
            return Optional.empty();
        }

        byte[] bytes = dataBytes(cloudEvent);
        if (bytes == null) {
            return Optional.empty();
        }

        Object root;
        try {
            root = objectMapper.readValue(bytes, Object.class);
        } catch (IOException e) {
            // Malformed JSON, or bytes that are not JSON at all. A single bad payload must not fail a query.
            return Optional.empty();
        }

        return resolve(root, path.split("\\."), 0);
    }

    // A dotted path is resolved one segment at a time, the way MongoDB resolves it. A Map is stepped into by key.
    // An array is stepped into element by element, the same "any element" reading FilterMatcher's anyElementMatches
    // gives the result, so items.sku against [{"sku":"a"},{"sku":"b"}] reaches into both rather than stopping at the
    // array. Anything else (a bare number, a bare string, ...) is an opaque value with no field to step into, which
    // covers a non-object root on the first segment and a path that continues past a value with no fields of its own
    // on a later one; MongoDB stops the same way, but only for that case, not for the array case above it.
    private static Optional<Object> resolve(@Nullable Object current, String[] pathSegments, int segmentIndex) {
        if (segmentIndex == pathSegments.length) {
            return Optional.ofNullable(current);
        }
        if (current instanceof List<?> list) {
            List<Object> matched = new ArrayList<>();
            for (Object element : list) {
                resolve(element, pathSegments, segmentIndex).ifPresent(matched::add);
            }
            return matched.isEmpty() ? Optional.empty() : Optional.of(matched);
        }
        if (!(current instanceof Map<?, ?> map)) {
            return Optional.empty();
        }
        return resolve(map.get(pathSegments[segmentIndex]), pathSegments, segmentIndex + 1);
    }

    private static @Nullable byte[] dataBytes(CloudEvent cloudEvent) {
        CloudEventData data = cloudEvent.getData();
        return data == null ? null : data.toBytes();
    }

    private static boolean isJson(@Nullable String dataContentType) {
        if (dataContentType == null) {
            // No content-type means application/json per the CloudEvents spec.
            return true;
        }
        String lowerCase = dataContentType.toLowerCase();
        return lowerCase.contains("/json") || lowerCase.contains("+json");
    }
}
