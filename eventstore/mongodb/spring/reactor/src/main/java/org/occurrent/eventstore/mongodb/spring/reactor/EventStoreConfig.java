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

package org.occurrent.eventstore.mongodb.spring.reactor;

import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.dcb.DcbStreamIdGenerator;
import org.occurrent.eventstore.api.dcb.PartitionedDcbStreamIdGenerator;
import org.occurrent.eventstore.api.reactor.EventStoreQueries;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * Configuration for the <a href="https://projectreactor.io/">project reactor</a> Spring java driver for MongoDB EventStore
 */
public class EventStoreConfig {
    private static final Function<Query, Query> DEFAULT_QUERY_OPTIONS_FUNCTION = Function.identity();
    private static final Function<Query, Query> DEFAULT_READ_OPTIONS_FUNCTION = Function.identity();
    private static final Set<EventStoreCapability> DEFAULT_EVENT_STORE_CAPABILITIES = Set.of(STREAM);

    public final String eventStoreCollectionName;
    public final TransactionalOperator transactionalOperator;
    public final TimeRepresentation timeRepresentation;
    public final Function<Query, Query> queryOptions;
    public final Function<Query, Query> readOptions;
    public final Set<EventStoreCapability> eventStoreCapabilities;
    public final DcbStreamIdGenerator dcbStreamIdGenerator;

    /**
     * Create a new instance of {@code EventStoreConfig}.
     *
     * @param eventStoreCollectionName The collection in which the events are persisted
     * @param transactionalOperator    The transactional operator responsible to starting MongoDB transactions (see {@link Builder} for overloads).
     * @param timeRepresentation       How time should be represented in the database
     */
    public EventStoreConfig(String eventStoreCollectionName, TransactionalOperator transactionalOperator, TimeRepresentation timeRepresentation) {
        this(eventStoreCollectionName, transactionalOperator, timeRepresentation, DEFAULT_QUERY_OPTIONS_FUNCTION, DEFAULT_READ_OPTIONS_FUNCTION, DEFAULT_EVENT_STORE_CAPABILITIES, new PartitionedDcbStreamIdGenerator());
    }

    private EventStoreConfig(String eventStoreCollectionName, TransactionalOperator transactionalOperator, TimeRepresentation timeRepresentation, Function<Query, Query> queryOptions, Function<Query, Query> readOptions, Set<EventStoreCapability> eventStoreCapabilities, DcbStreamIdGenerator dcbStreamIdGenerator) {
        requireNonNull(eventStoreCollectionName, "Event store collection name cannot be null");
        requireNonNull(transactionalOperator, TransactionalOperator.class.getSimpleName() + " cannot be null");
        requireNonNull(timeRepresentation, TimeRepresentation.class.getSimpleName() + " cannot be null");
        requireNonNull(eventStoreCapabilities, "Event store capabilities cannot be null");
        if (eventStoreCapabilities.isEmpty()) {
            throw new IllegalArgumentException("Event store capabilities cannot be empty");
        }
        requireNonNull(dcbStreamIdGenerator, DcbStreamIdGenerator.class.getSimpleName() + " cannot be null");
        this.eventStoreCollectionName = eventStoreCollectionName;
        this.transactionalOperator = transactionalOperator;
        this.timeRepresentation = timeRepresentation;
        this.queryOptions = queryOptions == null ? DEFAULT_QUERY_OPTIONS_FUNCTION : queryOptions;
        this.readOptions = readOptions == null ? DEFAULT_READ_OPTIONS_FUNCTION : readOptions;
        this.eventStoreCapabilities = Set.copyOf(eventStoreCapabilities);
        this.dcbStreamIdGenerator = dcbStreamIdGenerator;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventStoreConfig)) return false;
        EventStoreConfig that = (EventStoreConfig) o;
        return Objects.equals(eventStoreCollectionName, that.eventStoreCollectionName) && Objects.equals(transactionalOperator, that.transactionalOperator) && timeRepresentation == that.timeRepresentation && Objects.equals(queryOptions, that.queryOptions) && Objects.equals(readOptions, that.readOptions) && Objects.equals(eventStoreCapabilities, that.eventStoreCapabilities) && Objects.equals(dcbStreamIdGenerator, that.dcbStreamIdGenerator);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventStoreCollectionName, transactionalOperator, timeRepresentation, queryOptions, readOptions, eventStoreCapabilities, dcbStreamIdGenerator);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", EventStoreConfig.class.getSimpleName() + "[", "]")
                .add("eventStoreCollectionName='" + eventStoreCollectionName + "'")
                .add("transactionalOperator=" + transactionalOperator)
                .add("timeRepresentation=" + timeRepresentation)
                .add("queryOptions=" + queryOptions)
                .add("readOptions=" + readOptions)
                .add("eventStoreCapabilities=" + eventStoreCapabilities)
                .add("dcbStreamIdGenerator=" + dcbStreamIdGenerator)
                .toString();
    }

    public static final class Builder {
        private String eventStoreCollectionName;
        private TransactionalOperator transactionalOperator;
        private TimeRepresentation timeRepresentation;
        private Function<Query, Query> queryOptions;
        private Function<Query, Query> readOptions;
        private Set<EventStoreCapability> eventStoreCapabilities = DEFAULT_EVENT_STORE_CAPABILITIES;
        private DcbStreamIdGenerator dcbStreamIdGenerator = new PartitionedDcbStreamIdGenerator();

        /**
         * @param eventStoreCollectionName The collection in which the events are persisted
         * @return A same {@code Builder instance}
         */
        public Builder eventStoreCollectionName(String eventStoreCollectionName) {
            this.eventStoreCollectionName = eventStoreCollectionName;
            return this;
        }

        /**
         * @param transactionalOperator The transactional operator responsible to starting MongoDB transactions
         * @return A same {@code Builder instance}
         */
        public Builder transactionConfig(TransactionalOperator transactionalOperator) {
            this.transactionalOperator = transactionalOperator;
            return this;
        }

        /**
         * @param reactiveTransactionManager Create a {@link TransactionalOperator} from the supplied {@code reactiveTransactionManager}
         * @return A same {@code Builder instance}
         */
        public Builder transactionConfig(ReactiveTransactionManager reactiveTransactionManager) {
            this.transactionalOperator = TransactionalOperator.create(reactiveTransactionManager);
            return this;
        }

        /**
         * @param timeRepresentation How time should be represented in the database
         * @return A same {@code Builder instance}
         */
        public Builder timeRepresentation(TimeRepresentation timeRepresentation) {
            this.timeRepresentation = timeRepresentation;
            return this;
        }

        /**
         * Specify a function that can be used to configure the query options used for {@link EventStoreQueries}, i.e. for query use cases.
         * This is an advanced feature and should be used sparingly. For example, you can configure cursor timeout, whether reads from secondaries are OK, etc. By default, mongodb default query options are used.
         * <br><br>
         * Note that you must <i>not</i> use this to change the query itself, i.e. don't use the {@link Query#with(Sort)} etc. Only use options such as {@link Query#cursorBatchSize(int)} that doesn't change
         * the actual query or sort order.
         *
         * @param queryOptions The query options function to use, it cannot return null.
         * @return A same {@code Builder instance}
         */
        public Builder queryOptions(Function<Query, Query> queryOptions) {
            this.queryOptions = queryOptions;
            return this;
        }


        /**
         * Specify a function that can be used to configure the query options used for {@link org.occurrent.eventstore.api.reactor.EventStore#read(String)}, i.e. for transactional scenarios.
         * This is an advanced feature and should be used sparingly. For example, you can configure cursor timeout, whether reads from secondaries are OK, etc. By default, mongodb default query options are used.
         * <br><br>
         * Note that you must <i>not</i> use this to change the query itself, i.e. don't use the {@link Query#with(Sort)} etc. Only use options such as {@link Query#cursorBatchSize(int)} that doesn't change
         * the actual query or sort order.
         *
         * @param readOptions The read options function to use, it cannot return null.
         * @return A same {@code Builder instance}
         */
        public Builder readOptions(Function<Query, Query> readOptions) {
            this.readOptions = readOptions;
            return this;
        }

        /**
         * Select the event-store capabilities that should be enabled for this store. The default is {@link EventStoreCapability#STREAM}.
         * Add {@link EventStoreCapability#DCB} to enable Dynamic Consistency Boundary reads and appends.
         *
         * @param eventStoreCapabilities The non-empty capability set to enable.
         * @return A same {@code Builder instance}
         */
        public Builder eventStoreCapabilities(Set<EventStoreCapability> eventStoreCapabilities) {
            requireNonNull(eventStoreCapabilities, "Event store capabilities cannot be null");
            if (eventStoreCapabilities.isEmpty()) {
                throw new IllegalArgumentException("Event store capabilities cannot be empty");
            }
            this.eventStoreCapabilities = Set.copyOf(eventStoreCapabilities);
            return this;
        }

        /**
         * Select the event-store capabilities as a vararg, for example {@code eventStoreCapabilities(STREAM, DCB)}.
         *
         * @param eventStoreCapability             The first capability to enable.
         * @param additionalEventStoreCapabilities Additional capabilities to enable.
         * @return A same {@code Builder instance}
         */
        public Builder eventStoreCapabilities(EventStoreCapability eventStoreCapability, EventStoreCapability... additionalEventStoreCapabilities) {
            requireNonNull(eventStoreCapability, "Event store capability cannot be null");
            requireNonNull(additionalEventStoreCapabilities, "Additional event store capabilities cannot be null");
            Set<EventStoreCapability> capabilities = new LinkedHashSet<>();
            Stream.concat(Stream.of(eventStoreCapability), Arrays.stream(additionalEventStoreCapabilities))
                    .map(capability -> requireNonNull(capability, "Event store capability cannot be null"))
                    .forEach(capabilities::add);
            return eventStoreCapabilities(capabilities);
        }

        /**
         * @param dcbStreamIdGenerator The generator that derives the storage stream id from the events' DCB tags.
         * @return A same {@code Builder instance}
         */
        public Builder dcbStreamIdGenerator(DcbStreamIdGenerator dcbStreamIdGenerator) {
            this.dcbStreamIdGenerator = requireNonNull(dcbStreamIdGenerator, DcbStreamIdGenerator.class.getSimpleName() + " cannot be null");
            return this;
        }

        public EventStoreConfig build() {
            return new EventStoreConfig(eventStoreCollectionName, transactionalOperator, timeRepresentation, queryOptions, readOptions, eventStoreCapabilities, dcbStreamIdGenerator);
        }
    }
}
