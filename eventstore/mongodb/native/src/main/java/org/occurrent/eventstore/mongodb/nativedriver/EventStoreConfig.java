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

package org.occurrent.eventstore.mongodb.nativedriver;

import com.mongodb.TransactionOptions;
import com.mongodb.client.FindIterable;
import io.cloudevents.CloudEvent;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.dcb.DcbStreamIdGenerator;
import org.occurrent.eventstore.api.dcb.PartitionedDcbStreamIdGenerator;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * Configuration for the synchronous java driver MongoDB EventStore
 */
@NullMarked
public class EventStoreConfig {
    private static final Function<FindIterable<Document>, FindIterable<Document>> DEFAULT_QUERY_OPTIONS_FUNCTION = Function.identity();
    private static final Set<EventStoreCapability> DEFAULT_EVENT_STORE_CAPABILITIES = Set.of(STREAM);

    public final TransactionOptions transactionOptions;
    public final TimeRepresentation timeRepresentation;
    public final Function<FindIterable<Document>, FindIterable<Document>> queryOptions;
    public final Set<EventStoreCapability> eventStoreCapabilities;
    public final DcbStreamIdGenerator dcbStreamIdGenerator;

    /**
     * Create an {@link EventStoreConfig} indicating to the event store that it should represent time according to the supplied
     * {@code timeRepresentation}. It'll use default {@link TransactionOptions}.
     *
     * @param timeRepresentation How the time field in the {@link CloudEvent} should be represented.
     * @see #EventStoreConfig(TimeRepresentation, TransactionOptions)
     * @see TimeRepresentation
     */
    public EventStoreConfig(TimeRepresentation timeRepresentation) {
        this(timeRepresentation, null);
    }

    /**
     * Create an {@link EventStoreConfig} indicating to the event store that it should represent time according to the supplied
     * {@code timeRepresentation}. Also configure the default {@link TransactionOptions} that the event store will use
     * when starting transactions.
     *
     * @param timeRepresentation How the time field in the {@link CloudEvent} should be represented.
     * @param transactionOptions The default {@link TransactionOptions} that the event store will use when starting transactions.
     * @see #EventStoreConfig(TimeRepresentation, TransactionOptions)
     * @see TimeRepresentation
     */
    public EventStoreConfig(TimeRepresentation timeRepresentation, @Nullable TransactionOptions transactionOptions) {
        this(timeRepresentation, transactionOptions, DEFAULT_QUERY_OPTIONS_FUNCTION, DEFAULT_EVENT_STORE_CAPABILITIES, new PartitionedDcbStreamIdGenerator());
    }

    private EventStoreConfig(TimeRepresentation timeRepresentation, @Nullable TransactionOptions transactionOptions, Function<FindIterable<Document>, FindIterable<Document>> queryOptions, Set<EventStoreCapability> eventStoreCapabilities, DcbStreamIdGenerator dcbStreamIdGenerator) {
        Objects.requireNonNull(timeRepresentation, "Time representation cannot be null");
        requireNonNull(eventStoreCapabilities, "Event store capabilities cannot be null");
        if (eventStoreCapabilities.isEmpty()) {
            throw new IllegalArgumentException("Event store capabilities cannot be empty");
        }
        requireNonNull(dcbStreamIdGenerator, DcbStreamIdGenerator.class.getSimpleName() + " cannot be null");
        this.transactionOptions = Objects.requireNonNullElseGet(transactionOptions, () -> TransactionOptions.builder().build());
        this.timeRepresentation = timeRepresentation;
        this.queryOptions = queryOptions;
        this.eventStoreCapabilities = Set.copyOf(eventStoreCapabilities);
        this.dcbStreamIdGenerator = dcbStreamIdGenerator;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventStoreConfig)) return false;
        EventStoreConfig that = (EventStoreConfig) o;
        return Objects.equals(transactionOptions, that.transactionOptions) && timeRepresentation == that.timeRepresentation && Objects.equals(queryOptions, that.queryOptions) && Objects.equals(eventStoreCapabilities, that.eventStoreCapabilities) && Objects.equals(dcbStreamIdGenerator, that.dcbStreamIdGenerator);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionOptions, timeRepresentation, queryOptions, eventStoreCapabilities, dcbStreamIdGenerator);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", EventStoreConfig.class.getSimpleName() + "[", "]")
                .add("transactionOptions=" + transactionOptions)
                .add("timeRepresentation=" + timeRepresentation)
                .add("queryOptions=" + queryOptions)
                .add("eventStoreCapabilities=" + eventStoreCapabilities)
                .add("dcbStreamIdGenerator=" + dcbStreamIdGenerator)
                .toString();
    }

    @NullUnmarked
    public static final class Builder {
        private TransactionOptions transactionOptions;
        private TimeRepresentation timeRepresentation;
        private Function<FindIterable<Document>, FindIterable<Document>> queryOptions = DEFAULT_QUERY_OPTIONS_FUNCTION;
        private Set<EventStoreCapability> eventStoreCapabilities = DEFAULT_EVENT_STORE_CAPABILITIES;
        private DcbStreamIdGenerator dcbStreamIdGenerator = new PartitionedDcbStreamIdGenerator();

        /**
         * @param transactionOptions The default {@link TransactionOptions} that the event store will use when starting transactions. May be <code>null</code>.
         * @return The builder instance
         */
        @NullMarked
        public Builder transactionOptions(TransactionOptions transactionOptions) {
            this.transactionOptions = transactionOptions;
            return this;
        }

        /**
         * @param timeRepresentation Configure how the event store should represent time in MongoDB
         * @return The builder instance
         */
        @NullMarked
        public Builder timeRepresentation(TimeRepresentation timeRepresentation) {
            this.timeRepresentation = timeRepresentation;
            return this;
        }

        /**
         * Specify a function that can be used to configure the query options used for {@link org.occurrent.eventstore.api.blocking.EventStore#read(String)} and {@link org.occurrent.eventstore.api.blocking.EventStoreQueries}.
         * This is an advanced feature and should be used sparingly. For example, you can configure cursor timeout, whether slave is OK, etc. By default, mongodb default query options are used.
         * <br><br>
         * Note that you must <i>not</i> use this to change the query itself, i.e. don't use the {@link FindIterable#sort(Bson)} etc. Only use options such as {@link FindIterable#batchSize(int)} that doesn't change
         * the actual query or sort order.
         *
         * @param queryOptions The query options function to use, it cannot return null.
         * @return A same {@code Builder instance}
         */
        @NullMarked
        public Builder queryOptions(Function<FindIterable<Document>, FindIterable<Document>> queryOptions) {
            this.queryOptions = queryOptions;
            return this;
        }

        /**
         * Select the event-store capabilities that should be enabled for this store.
         * <p>
         * The default is {@link EventStoreCapability#STREAM}. Add {@link EventStoreCapability#DCB} to enable Dynamic
         * Consistency Boundary reads and appends, or enable both at once.
         *
         * @param eventStoreCapabilities The non-empty capability set to enable.
         * @return The same {@code Builder} instance.
         */
        @NullMarked
        public Builder eventStoreCapabilities(Set<EventStoreCapability> eventStoreCapabilities) {
            requireNonNull(eventStoreCapabilities, "Event store capabilities cannot be null");
            if (eventStoreCapabilities.isEmpty()) {
                throw new IllegalArgumentException("Event store capabilities cannot be empty");
            }
            this.eventStoreCapabilities = Set.copyOf(eventStoreCapabilities);
            return this;
        }

        /**
         * Select the event-store capabilities that should be enabled for this store.
         * <p>
         * This vararg form is convenient when composing capabilities, for example
         * {@code eventStoreCapabilities(STREAM, DCB)}.
         *
         * @param eventStoreCapability            The first capability to enable.
         * @param additionalEventStoreCapabilities Additional capabilities to enable.
         * @return The same {@code Builder} instance.
         */
        @NullMarked
        public Builder eventStoreCapabilities(EventStoreCapability eventStoreCapability, EventStoreCapability... additionalEventStoreCapabilities) {
            requireNonNull(eventStoreCapability, "Event store capability cannot be null");
            requireNonNull(additionalEventStoreCapabilities, "Additional event store capabilities cannot be null");
            Set<EventStoreCapability> capabilities = new LinkedHashSet<>();
            capabilities.add(eventStoreCapability);
            Stream.of(additionalEventStoreCapabilities)
                    .map(capability -> requireNonNull(capability, "Event store capability cannot be null"))
                    .forEach(capabilities::add);
            return eventStoreCapabilities(capabilities);
        }

        /**
         * Configure how the storage stream id is derived from a DCB append's boundary tags.
         *
         * @param dcbStreamIdGenerator The generator that derives the storage stream id from the events' DCB tags.
         * @return The same {@code Builder} instance.
         */
        @NullMarked
        public Builder dcbStreamIdGenerator(DcbStreamIdGenerator dcbStreamIdGenerator) {
            this.dcbStreamIdGenerator = requireNonNull(dcbStreamIdGenerator, DcbStreamIdGenerator.class.getSimpleName() + " cannot be null");
            return this;
        }

        @NullMarked
        public EventStoreConfig build() {
            return new EventStoreConfig(timeRepresentation, transactionOptions, queryOptions, eventStoreCapabilities, dcbStreamIdGenerator);
        }
    }
}
