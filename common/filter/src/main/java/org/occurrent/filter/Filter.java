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
import io.cloudevents.core.v1.CloudEventV1;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.condition.Condition;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.occurrent.condition.Condition.eq;
import static org.occurrent.filter.Filter.CompositionOperator.AND;
import static org.occurrent.filter.Filter.CompositionOperator.OR;

/**
 * Filters that can be applied when querying an event store or subscription that supports querying capabilities
 */
public abstract class Filter {
    public static final String SPEC_VERSION = "specversion";
    public static final String ID = "id";
    public static final String TYPE = "type";
    public static final String TIME = "time";
    public static final String SOURCE = "source";
    public static final String SUBJECT = "subject";
    public static final String DATA_SCHEMA = "dataschema";
    public static final String DATA_CONTENT_TYPE = "datacontenttype";
    public static final String DATA = "data";

    private Filter() {
    }


    public static final class All extends Filter {
        private All() {
        }
    }

    public static final class SingleConditionFilter extends Filter {
        public final String fieldName;
        public final Condition<?> condition;

        private SingleConditionFilter(String fieldName, Condition<?> condition) {
            requireNonNull(fieldName, "Field name cannot be null");
            requireNonNull(condition, "Condition cannot be null");
            this.fieldName = fieldName;
            this.condition = condition;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SingleConditionFilter)) return false;
            SingleConditionFilter that = (SingleConditionFilter) o;
            return Objects.equals(fieldName, that.fieldName) &&
                    Objects.equals(condition, that.condition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldName, condition);
        }

        @Override
        public String toString() {
            return "SingleConditionFilter{" +
                    "fieldName='" + fieldName + '\'' +
                    ", condition=" + condition +
                    '}';
        }
    }

    public static final class CompositionFilter extends Filter {
        public final CompositionOperator operator;
        public final List<Filter> filters;

        private CompositionFilter(CompositionOperator operator, List<Filter> filters) {
            this.operator = operator;
            this.filters = filters;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof CompositionFilter)) return false;
            CompositionFilter that = (CompositionFilter) o;
            return operator == that.operator &&
                    Objects.equals(filters, that.filters);
        }

        @Override
        public int hashCode() {
            return Objects.hash(operator, filters);
        }

        @Override
        public String toString() {
            return "ComposedFilter{" +
                    "operator=" + operator +
                    ", filters=" + filters +
                    '}';
        }
    }

    public static <T> Filter filter(Supplier<String> fieldName, Condition<T> condition) {
        return filter(fieldName.get(), condition);
    }

    public static <T> Filter filter(String fieldName, Condition<T> condition) {
        return new SingleConditionFilter(fieldName, condition);
    }

    public <T> Filter and(String fieldName, Condition<T> condition) {
        return and(filter(fieldName, condition));
    }

    public <T> Filter or(String fieldName, Condition<T> condition) {
        return or(filter(fieldName, condition));
    }

    public Filter and(Filter filter, Filter... filters) {
        List<Filter> filterList = toList(this, filter, filters);
        return new CompositionFilter(AND, filterList);
    }

    public Filter or(Filter filter, Filter... filters) {
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

    public static Filter all() {
        return new All();
    }

    // Convenience methods
    public static Filter id(String value) {
        return id(eq(value));
    }

    public static Filter id(Condition<String> condition) {
        return filter(ID, condition);
    }

    public static Filter type(String value) {
        return type(eq(value));
    }

    public static Filter type(Condition<String> condition) {
        return filter(TYPE, condition);
    }

    public static Filter source(URI condition) {
        return source(eq(condition));
    }

    public static Filter source(Condition<URI> condition) {
        return filter(SOURCE, condition.map(URI::toString));
    }

    public static Filter subject(String value) {
        return subject(eq(value));
    }

    public static Filter subject(Condition<String> condition) {
        return filter(SUBJECT, condition);
    }

    public static Filter dataSchema(URI value) {
        return dataSchema(eq(value));
    }

    public static Filter dataSchema(Condition<URI> condition) {
        return filter(DATA_SCHEMA, condition.map(URI::toString));
    }

    public static Filter dataContentType(String value) {
        return dataContentType(eq(value));
    }

    public static Filter dataContentType(Condition<String> condition) {
        return filter(DATA_CONTENT_TYPE, condition);
    }

    public static Filter time(OffsetDateTime value) {
        return time(eq(value));
    }

    public static Filter time(Condition<OffsetDateTime> condition) {
        return filter(TIME, condition);
    }

    public static Filter streamId(String value) {
        return streamId(eq(value));
    }

    public static Filter streamId(Condition<String> condition) {
        return filter(OccurrentCloudEventExtension.STREAM_ID, condition);
    }

    public static Filter streamVersion(long value) {
        return streamVersion(eq(value));
    }

    public static Filter streamVersion(Condition<Long> condition) {
        return filter(OccurrentCloudEventExtension.STREAM_VERSION, condition);
    }

    public static Filter specVersion(SpecVersion value) {
        return specVersion(value.toString());
    }

    public static Filter specVersion(String value) {
        return specVersion(eq(value));
    }

    public static Filter specVersion(Condition<String> condition) {
        return filter(SPEC_VERSION, condition);
    }

    public static <T> Filter data(String name, Condition<T> condition) {
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
    public static Filter cloudEvent(String id, URI source) {
        return id(id).and(source(source));
    }

    public enum CompositionOperator {
        AND, OR
    }
}