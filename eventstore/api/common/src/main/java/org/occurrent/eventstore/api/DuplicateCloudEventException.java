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

package org.occurrent.eventstore.api;

import java.net.URI;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * An exception thrown if a cloud event already exists in the event store or if it violates some unique indexing rules.
 */
public class DuplicateCloudEventException extends RuntimeException {
    private final String id;
    private final URI source;
    private final String details;

    public DuplicateCloudEventException(String id, URI source, String details, Throwable cause) {
        super("Duplicate CloudEvent detected with id " + unknownIfNull(id) + " and source " + unknownIfNull(source == null ? null : source.toString()) + ", details: " + unknownIfNull(details), cause);
        this.id = id;
        this.source = source;
        this.details = details;
    }

    public String getId() {
        return id;
    }

    public URI getSource() {
        return source;
    }

    public String getDetails() {
        return details;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DuplicateCloudEventException)) return false;
        DuplicateCloudEventException that = (DuplicateCloudEventException) o;
        return Objects.equals(id, that.id) && Objects.equals(source, that.source) && Objects.equals(details, that.details);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, source, details);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", DuplicateCloudEventException.class.getSimpleName() + "[", "]")
                .add("id='" + id + "'")
                .add("source=" + source)
                .add("details='" + details + "'")
                .toString();
    }

    private static String unknownIfNull(String str) {
        return str == null ? "<unknown>" : str;
    }
}
