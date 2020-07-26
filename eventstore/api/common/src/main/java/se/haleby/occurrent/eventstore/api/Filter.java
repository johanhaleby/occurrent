package se.haleby.occurrent.eventstore.api;

import io.cloudevents.SpecVersion;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static se.haleby.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_ID;
import static se.haleby.occurrent.eventstore.api.Condition.eq;

/**
 * Filters that can be applied when queriying an event store
 */
public class Filter {
    public static final String SPEC_VERSION = "specversion";
    public static final String ID = "id";
    public static final String TYPE = "type";
    public static final String TIME = "time";
    public static final String SOURCE = "source";
    public static final String SUBJECT = "subject";
    public static final String DATA_SCHEMA = "dataschema";
    public static final String DATA_CONTENT_TYPE = "datacontenttype";

    public final String fieldName;
    public final Condition<?> condition;

    private Filter(String fieldName, Condition<?> condition) {
        requireNonNull(fieldName, "Field name cannot be null");
        requireNonNull(condition, "Condition cannot be null");
        this.fieldName = fieldName;
        this.condition = condition;
    }

    public static <T> Filter filter(Supplier<String> fieldName, Condition<T> condition) {
        return filter(fieldName.get(), condition);
    }

    public static <T> Filter filter(String fieldName, Condition<T> condition) {
        return new Filter(fieldName, condition);
    }

    public <T> List<Filter> and(String fieldName, Condition<T> condition) {
        return and(Filter.filter(fieldName, condition));
    }

    public List<Filter> and(Filter filter, Filter... filters) {
        requireNonNull(filter, "Filter cannot be null");
        requireNonNull(filters, "Filters cannot be null");

        List<Filter> allFilters = new ArrayList<>(2 + filters.length);
        allFilters.add(this);
        Collections.addAll(allFilters, filters);
        return Collections.unmodifiableList(allFilters);
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


    public static Filter time(ZonedDateTime value) {
        return time(eq(value));
    }

    public static Filter time(Condition<ZonedDateTime> condition) {
        return date(condition.map(zdt -> Date.from(zdt.toInstant())));
    }

    public static Filter date(Date value) {
        return date(eq(value));
    }

    public static Filter date(Condition<Date> condition) {
        return filter(TIME, condition);
    }

    public static Filter occurrentStreamId(String value) {
        return occurrentStreamId(eq(value));
    }

    public static Filter occurrentStreamId(Condition<String> condition) {
        return filter(STREAM_ID, condition);
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

    /**
     * Find a unique cloud event
     *
     * @param id     The id of the cloud event
     * @param source The source of the cloud event
     * @return A filter list describing the query
     */
    public static List<Filter> cloudEvent(String id, URI source) {
        return id(id).and(source(source));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Filter)) return false;
        Filter filter = (Filter) o;
        return Objects.equals(fieldName, filter.fieldName) &&
                Objects.equals(condition, filter.condition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, condition);
    }

    @Override
    public String toString() {
        return "Filter{" +
                "fieldName='" + fieldName + '\'' +
                ", condition=" + condition +
                '}';
    }
}