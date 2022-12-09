/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.filter;

import io.cloudevents.SpecVersion;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.condition.Condition;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.occurrent.condition.Condition.eq;
import static org.occurrent.filter.Filter.CompositionOperator.AND;
import static org.occurrent.filter.Filter.CompositionOperator.OR;

/**
 * Filters that can be applied when querying an event store or subscription that supports querying capabilities
 */
public sealed interface Filter {
    String SPEC_VERSION = "specversion";
    String ID = "id";
    String TYPE = "type";
    String TIME = "time";
    String SOURCE = "source";
    String SUBJECT = "subject";
    String DATA_SCHEMA = "dataschema";
    String DATA_CONTENT_TYPE = "datacontenttype";
    String DATA = "data";

    record All() implements Filter {
    }

    record SingleConditionFilter(String fieldName, Condition<?> condition) implements Filter {
        public SingleConditionFilter {
            requireNonNull(fieldName, "Field name cannot be null");
            requireNonNull(condition, "Condition cannot be null");
        }
    }

    record CompositionFilter(CompositionOperator operator, List<Filter> filters) implements Filter {

        public CompositionFilter {
            requireNonNull(operator, "Operator cannot be null");
            requireNonNull(filters, "Filters cannot be null");
        }
    }

    static <T> Filter filter(Supplier<String> fieldName, Condition<T> condition) {
        return filter(fieldName.get(), condition);
    }

    static <T> Filter filter(String fieldName, Condition<T> condition) {
        return new SingleConditionFilter(fieldName, condition);
    }

    default <T> Filter and(String fieldName, Condition<T> condition) {
        return and(filter(fieldName, condition));
    }

    default <T> Filter or(String fieldName, Condition<T> condition) {
        return or(filter(fieldName, condition));
    }

    default Filter and(Filter filter, Filter... filters) {
        List<Filter> filterList = toList(this, filter, filters);
        return new CompositionFilter(AND, filterList);
    }

    default Filter or(Filter filter, Filter... filters) {
        List<Filter> filterList = toList(this, filter, filters);
        return new CompositionFilter(OR, filterList);
    }

    private List<Filter> toList(Filter firstFilter, Filter secondFilter, Filter[] moreFilters) {
        requireNonNull(secondFilter, "Filter cannot be null");

        List<Filter> allFilters = new ArrayList<>(2 + (moreFilters == null ? 0 : moreFilters.length));
        allFilters.add(firstFilter);
        allFilters.add(secondFilter);
        if (moreFilters != null) {
            Collections.addAll(allFilters, moreFilters);
        }
        return allFilters;
    }

    static Filter all() {
        return new All();
    }

    // Convenience methods
    static Filter id(String value) {
        return id(eq(value));
    }

    static Filter id(Condition<String> condition) {
        return filter(ID, condition);
    }

    static Filter type(String value) {
        return type(eq(value));
    }

    static Filter type(Condition<String> condition) {
        return filter(TYPE, condition);
    }

    static Filter source(URI condition) {
        return source(eq(condition));
    }

    static Filter source(Condition<URI> condition) {
        return filter(SOURCE, condition.map(URI::toString));
    }

    static Filter subject(String value) {
        return subject(eq(value));
    }

    static Filter subject(Condition<String> condition) {
        return filter(SUBJECT, condition);
    }

    static Filter dataSchema(URI value) {
        return dataSchema(eq(value));
    }

    static Filter dataSchema(Condition<URI> condition) {
        return filter(DATA_SCHEMA, condition.map(URI::toString));
    }

    static Filter dataContentType(String value) {
        return dataContentType(eq(value));
    }

    static Filter dataContentType(Condition<String> condition) {
        return filter(DATA_CONTENT_TYPE, condition);
    }

    static Filter time(OffsetDateTime value) {
        return time(eq(value));
    }

    static Filter time(Condition<OffsetDateTime> condition) {
        return filter(TIME, condition);
    }

    static Filter streamId(String value) {
        return streamId(eq(value));
    }

    static Filter streamId(Condition<String> condition) {
        return filter(OccurrentCloudEventExtension.STREAM_ID, condition);
    }

    static Filter streamVersion(long value) {
        return streamVersion(eq(value));
    }

    static Filter streamVersion(Condition<Long> condition) {
        return filter(OccurrentCloudEventExtension.STREAM_VERSION, condition);
    }

    static Filter specVersion(SpecVersion value) {
        return specVersion(value.toString());
    }

    static Filter specVersion(String value) {
        return specVersion(eq(value));
    }

    static Filter specVersion(Condition<String> condition) {
        return filter(SPEC_VERSION, condition);
    }

    static <T> Filter data(String name, Condition<T> condition) {
        requireNonNull(name, "Data name cannot be null");
        return filter(DATA + "." + name, condition);
    }

    /**
     * Find a unique cloud event
     *
     * @param id     The id of the cloud event
     * @param source The source of the cloud event
     * @return A filter list describing the query
     */
    static Filter cloudEvent(String id, URI source) {
        return id(id).and(source(source));
    }

    enum CompositionOperator {
        AND, OR
    }
}