/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.eventstore.api;

import io.cloudevents.SpecVersion;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.condition.Condition;
import org.occurrent.filter.Filter;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static java.util.Objects.requireNonNull;
import static org.occurrent.condition.Condition.eq;
import static org.occurrent.eventstore.api.StreamReadFilter.CompositionOperator.AND;
import static org.occurrent.eventstore.api.StreamReadFilter.CompositionOperator.OR;

/**
 * Filter DSL intended for reading events from a single stream.
 *
 * <p>
 * Differences vs {@link Filter}:
 * - No "all" variant (use overloads without a filter instead).
 * - No streamId or streamVersion convenience methods.
 * - Supports CloudEvent core attributes, CloudEvent extensions, and data predicates.
 */
public sealed interface StreamReadFilter permits StreamReadFilter.AttributeFilter,
        StreamReadFilter.ExtensionFilter,
        StreamReadFilter.DataFilter,
        StreamReadFilter.CompositionFilter {

    // CloudEvent core attributes (mirrors Filter constants)
    String SPEC_VERSION = Filter.SPEC_VERSION;
    String ID = Filter.ID;
    String TYPE = Filter.TYPE;
    String TIME = Filter.TIME;
    String SOURCE = Filter.SOURCE;
    String SUBJECT = Filter.SUBJECT;
    String DATA_SCHEMA = Filter.DATA_SCHEMA;
    String DATA_CONTENT_TYPE = Filter.DATA_CONTENT_TYPE;

    /**
     * CloudEvent core attribute filter.
     */
    record AttributeFilter<T>(String attributeName, Condition<T> condition) implements StreamReadFilter {
        public AttributeFilter {
            requireNonNull(attributeName, "Attribute name cannot be null");
            requireNonNull(condition, "Condition cannot be null");
        }
    }

    /**
     * CloudEvent extension attribute filter.
     */
    record ExtensionFilter<T>(String extensionName, Condition<T> condition) implements StreamReadFilter {
        public ExtensionFilter {
            requireNonNull(extensionName, "Extension name cannot be null");
            requireNonNull(condition, "Condition cannot be null");
        }
    }

    /**
     * CloudEvent data field filter. The path is interpreted the same way as {@link Filter#data(String, Condition)}.
     */
    record DataFilter<T>(String path, Condition<T> condition) implements StreamReadFilter {
        public DataFilter {
            requireNonNull(path, "Data path cannot be null");
            requireNonNull(condition, "Condition cannot be null");
        }
    }

    record CompositionFilter(CompositionOperator operator, List<StreamReadFilter> filters) implements StreamReadFilter {
        public CompositionFilter {
            requireNonNull(operator, "Operator cannot be null");
            requireNonNull(filters, "Filters cannot be null");
        }
    }

    enum CompositionOperator {
        AND, OR
    }


    static <T> StreamReadFilter attribute(String name, Condition<T> condition) {
        validateNotReservedOccurrentStreamField(name, "attribute");
        return new AttributeFilter<>(name, condition);
    }

    static <T> StreamReadFilter extension(String name, Condition<T> condition) {
        validateNotReservedOccurrentStreamField(name, "extension");
        return new ExtensionFilter<>(name, condition);
    }

    static <T> StreamReadFilter data(String path, Condition<T> condition) {
        return new DataFilter<>(path, condition);
    }

    default StreamReadFilter and(StreamReadFilter other, StreamReadFilter... more) {
        return compose(AND, this, other, more);
    }

    default StreamReadFilter or(StreamReadFilter other, StreamReadFilter... more) {
        return compose(OR, this, other, more);
    }

    private static StreamReadFilter compose(CompositionOperator op, StreamReadFilter first, StreamReadFilter second, StreamReadFilter[] more) {
        requireNonNull(first, "First filter cannot be null");
        requireNonNull(second, "Second filter cannot be null");

        List<StreamReadFilter> all = new ArrayList<>(2 + (more == null ? 0 : more.length));
        all.add(first);
        all.add(second);
        if (more != null) {
            Collections.addAll(all, more);
        }
        return new CompositionFilter(op, all);
    }

    // ----------- Convenience methods for core attributes -----------

    static StreamReadFilter id(String value) {
        return id(eq(value));
    }

    static StreamReadFilter id(Condition<String> condition) {
        return attribute(ID, condition);
    }

    static StreamReadFilter type(String value) {
        return type(eq(value));
    }

    static StreamReadFilter type(Condition<String> condition) {
        return attribute(TYPE, condition);
    }

    static StreamReadFilter subject(String value) {
        return subject(eq(value));
    }

    static StreamReadFilter subject(Condition<String> condition) {
        return attribute(SUBJECT, condition);
    }

    static StreamReadFilter time(OffsetDateTime value) {
        return time(eq(value));
    }

    static StreamReadFilter time(Condition<OffsetDateTime> condition) {
        return attribute(TIME, condition);
    }

    static StreamReadFilter source(URI value) {
        return source(eq(value));
    }

    static StreamReadFilter source(Condition<URI> condition) {
        // Match Filter.source mapping to string
        return attribute(SOURCE, condition.map(URI::toString));
    }

    static StreamReadFilter dataSchema(URI value) {
        return dataSchema(eq(value));
    }

    static StreamReadFilter dataSchema(Condition<URI> condition) {
        return attribute(DATA_SCHEMA, condition.map(URI::toString));
    }

    static StreamReadFilter dataContentType(String value) {
        return dataContentType(eq(value));
    }

    static StreamReadFilter dataContentType(Condition<String> condition) {
        return attribute(DATA_CONTENT_TYPE, condition);
    }

    static StreamReadFilter specVersion(SpecVersion value) {
        return specVersion(value.toString());
    }

    static StreamReadFilter specVersion(String value) {
        return specVersion(eq(value));
    }

    static StreamReadFilter specVersion(Condition<String> condition) {
        return attribute(SPEC_VERSION, condition);
    }

    static StreamReadFilter extension(String name, String value) {
        return extension(name, eq(value));
    }

    private static void validateNotReservedOccurrentStreamField(String name, String kind) {
        requireNonNull(name, kind + " name cannot be null");
        String normalizedName = normalize(name);
        String streamIdName = normalize(OccurrentCloudEventExtension.STREAM_ID);
        String streamVersionName = normalize(OccurrentCloudEventExtension.STREAM_VERSION);
        if (normalizedName.equals(streamIdName) || normalizedName.equals(streamVersionName)) {
            throw new IllegalArgumentException(
                    "StreamReadFilter must not constrain " + kind + " '" + normalizedName + "'. " +
                            "streamId is provided by the stream read API, and streamVersion is derived from stream history."
            );
        }
    }

    private static String normalize(String value) {
        return value.trim().toLowerCase(Locale.ROOT);
    }
}
