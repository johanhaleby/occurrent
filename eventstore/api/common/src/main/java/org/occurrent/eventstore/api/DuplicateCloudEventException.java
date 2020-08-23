package org.occurrent.eventstore.api;

import java.net.URI;
import java.util.Objects;

/**
 * An exception thrown if a cloud event already exists in the event store or if it violates some unique indexing rules.
 */
public class DuplicateCloudEventException extends RuntimeException {
    private final String id;
    private final URI source;

    public DuplicateCloudEventException(String id, URI source, Throwable cause) {
        super("Duplicate CloudEvent detected with id " + unknownIfNull(id) + " and source " + unknownIfNull(source == null ? null : source.toString()), cause);
        this.id = id;
        this.source = source;
    }

    public String getId() {
        return id;
    }

    public URI getSource() {
        return source;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DuplicateCloudEventException)) return false;
        DuplicateCloudEventException that = (DuplicateCloudEventException) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(source, that.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, source);
    }

    @Override
    public String toString() {
        return "DuplicateCloudEventException{" +
                "id='" + id + '\'' +
                ", source=" + source +
                '}';
    }

    private static String unknownIfNull(String str) {
        return str == null ? "<unknown>" : str;
    }
}
