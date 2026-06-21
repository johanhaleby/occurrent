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

package org.occurrent.eventstore.mongodb.spring.blocking;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.NullUnmarked;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.dcb.DcbStreamIdGenerator;
import org.occurrent.eventstore.api.dcb.PartitionedDcbStreamIdGenerator;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStoreCapability.STREAM;

/**
 * Configuration for the blocking Spring java driver for MongoDB EventStore
 */
@NullMarked
public class EventStoreConfig {
    private static final Function<Query, Query> DEFAULT_QUERY_OPTIONS_FUNCTION = Function.identity();
    private static final Function<Query, Query> DEFAULT_READ_OPTIONS_FUNCTION = Function.identity();
    private static final Set<SpringMongoEventStoreCapability> DEFAULT_EVENT_STORE_CAPABILITIES = Set.of(STREAM);

    public final String eventStoreCollectionName;
    public final TransactionTemplate transactionTemplate;
    public final TimeRepresentation timeRepresentation;
    public final Function<Query, Query> queryOptions;
    public final Function<Query, Query> readOptions;
    public final Set<SpringMongoEventStoreCapability> eventStoreCapabilities;
    public final DcbStreamIdGenerator dcbStreamIdGenerator;

    /**
     * Create a new instance of {@code EventStoreConfig}.
     *
     * @param eventStoreCollectionName The collection in which the events are persisted
     * @param transactionTemplate      The transaction template responsible to starting MongoDB transactions (see {@link Builder} for overloads).
     * @param timeRepresentation       How time should be represented in the database
     */
    public EventStoreConfig(String eventStoreCollectionName, TransactionTemplate transactionTemplate, TimeRepresentation timeRepresentation) {
        this(eventStoreCollectionName, transactionTemplate, timeRepresentation, DEFAULT_QUERY_OPTIONS_FUNCTION, DEFAULT_READ_OPTIONS_FUNCTION, DEFAULT_EVENT_STORE_CAPABILITIES, new PartitionedDcbStreamIdGenerator());
    }

    private EventStoreConfig(String eventStoreCollectionName, TransactionTemplate transactionTemplate, TimeRepresentation timeRepresentation, Function<Query, Query> queryOptions, Function<Query, Query> readOptions, Set<SpringMongoEventStoreCapability> eventStoreCapabilities, DcbStreamIdGenerator dcbStreamIdGenerator) {
        requireNonNull(eventStoreCollectionName, "Event store collection name cannot be null");
        requireNonNull(transactionTemplate, TransactionTemplate.class.getSimpleName() + " cannot be null");
        requireNonNull(timeRepresentation, TimeRepresentation.class.getSimpleName() + " cannot be null");
        requireNonNull(eventStoreCapabilities, "Event store capabilities cannot be null");
        if (eventStoreCapabilities.isEmpty()) {
            throw new IllegalArgumentException("Event store capabilities cannot be empty");
        }
        requireNonNull(dcbStreamIdGenerator, DcbStreamIdGenerator.class.getSimpleName() + " cannot be null");
        // Note that we deliberately allow the WriteConcern to be null in order to be able to use the default MongoTemplate settings
        this.eventStoreCollectionName = eventStoreCollectionName;
        this.transactionTemplate = transactionTemplate;
        this.timeRepresentation = timeRepresentation;
        this.queryOptions = queryOptions;
        this.readOptions = readOptions;
        this.eventStoreCapabilities = Set.copyOf(eventStoreCapabilities);
        this.dcbStreamIdGenerator = dcbStreamIdGenerator;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventStoreConfig)) return false;
        EventStoreConfig that = (EventStoreConfig) o;
        return Objects.equals(eventStoreCollectionName, that.eventStoreCollectionName) && Objects.equals(transactionTemplate, that.transactionTemplate) && timeRepresentation == that.timeRepresentation && Objects.equals(queryOptions, that.queryOptions) && Objects.equals(readOptions, that.readOptions) && Objects.equals(eventStoreCapabilities, that.eventStoreCapabilities) && Objects.equals(dcbStreamIdGenerator, that.dcbStreamIdGenerator);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventStoreCollectionName, transactionTemplate, timeRepresentation, queryOptions, readOptions, eventStoreCapabilities, dcbStreamIdGenerator);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", EventStoreConfig.class.getSimpleName() + "[", "]")
                .add("eventStoreCollectionName='" + eventStoreCollectionName + "'")
                .add("transactionTemplate=" + transactionTemplate)
                .add("timeRepresentation=" + timeRepresentation)
                .add("queryOptions=" + queryOptions)
                .add("readOptions=" + readOptions)
                .add("eventStoreCapabilities=" + eventStoreCapabilities)
                .add("dcbStreamIdGenerator=" + dcbStreamIdGenerator)
                .toString();
    }

    @NullUnmarked
    public static final class Builder {
        private String eventStoreCollectionName;
        private TransactionTemplate transactionTemplate;
        private TimeRepresentation timeRepresentation;
        private Function<Query, Query> queryOptions = DEFAULT_QUERY_OPTIONS_FUNCTION;
        private Function<Query, Query> readOptions = DEFAULT_READ_OPTIONS_FUNCTION;
        private Set<SpringMongoEventStoreCapability> eventStoreCapabilities = DEFAULT_EVENT_STORE_CAPABILITIES;
        private DcbStreamIdGenerator dcbStreamIdGenerator = new PartitionedDcbStreamIdGenerator();

        /**
         * @param eventStoreCollectionName The collection in which the events are persisted
         * @return The same {@code Builder} instance.
         */
        @NullMarked
        public Builder eventStoreCollectionName(String eventStoreCollectionName) {
            this.eventStoreCollectionName = eventStoreCollectionName;
            return this;
        }

        /**
         * @param transactionTemplate The transaction template responsible to starting MongoDB transactions
         * @return The same {@code Builder} instance.
         */

        @NullMarked
        public Builder transactionConfig(TransactionTemplate transactionTemplate) {
            this.transactionTemplate = transactionTemplate;
            return this;
        }

        /**
         * @param mongoTransactionManager Create a {@link TransactionTemplate} from the supplied {@code mongoTransactionManager}
         * @return The same {@code Builder} instance.
         */
        @NullMarked
        public Builder transactionConfig(MongoTransactionManager mongoTransactionManager) {
            this.transactionTemplate = new TransactionTemplate(mongoTransactionManager);
            return this;
        }

        /**
         * @param timeRepresentation How time should be represented in the database
         * @return The same {@code Builder} instance.
         */
        @NullMarked
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
         * @return The same {@code Builder} instance.
         */
        public Builder queryOptions(Function<Query, Query> queryOptions) {
            this.queryOptions = queryOptions;
            return this;
        }


        /**
         * Specify a function that can be used to configure the query options used for {@link org.occurrent.eventstore.api.blocking.EventStore#read(String)}, i.e. for transactional scenarios.
         * This is an advanced feature and should be used sparingly. For example, you can configure cursor timeout, whether reads from secondaries are OK, etc. By default, mongodb default query options are used.
         * <br><br>
         * Note that you must <i>not</i> use this to change the query itself, i.e. don't use the {@link Query#with(Sort)} etc. Only use options such as {@link Query#cursorBatchSize(int)} that doesn't change
         * the actual query or sort order.
         *
         * @param readOptions The read options function to use, it cannot return null.
         * @return The same {@code Builder} instance.
         */
        @NullMarked
        public Builder readOptions(Function<Query, Query> readOptions) {
            this.readOptions = readOptions;
            return this;
        }

        /**
         * Select the event-store capabilities that should be enabled for this store.
         * <p>
         * The default is {@link SpringMongoEventStoreCapability#STREAM}. Add {@link SpringMongoEventStoreCapability#DCB}
         * to enable Dynamic Consistency Boundary indexes, support collections, and API operations.
         *
         * @param eventStoreCapabilities The non-empty capability set to enable.
         * @return The same {@code Builder} instance.
         */
        @NullMarked
        public Builder eventStoreCapabilities(Set<SpringMongoEventStoreCapability> eventStoreCapabilities) {
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
         * @param eventStoreCapability The first capability to enable.
         * @param additionalEventStoreCapabilities Additional capabilities to enable.
         * @return The same {@code Builder} instance.
         */
        @NullMarked
        public Builder eventStoreCapabilities(SpringMongoEventStoreCapability eventStoreCapability, SpringMongoEventStoreCapability... additionalEventStoreCapabilities) {
            requireNonNull(eventStoreCapability, "Event store capability cannot be null");
            requireNonNull(additionalEventStoreCapabilities, "Additional event store capabilities cannot be null");
            Set<SpringMongoEventStoreCapability> capabilities = new LinkedHashSet<>();
            capabilities.add(eventStoreCapability);
            Arrays.stream(additionalEventStoreCapabilities)
                    .map(capability -> requireNonNull(capability, "Event store capability cannot be null"))
                    .forEach(capabilities::add);
            return eventStoreCapabilities(capabilities);
        }

        /**
         * Choose how DCB-written events are placed into Occurrent storage streams. The default partitions them
         * across a fixed number of streams keyed by the events' DCB tags. The placement does not affect DCB read or
         * append-condition semantics, which are global by sequence position and tags.
         *
         * @param dcbStreamIdGenerator The generator that derives the storage stream id from the events' DCB tags.
         * @return The same {@code Builder} instance.
         */
        @NullMarked
        public Builder dcbStreamIdGenerator(DcbStreamIdGenerator dcbStreamIdGenerator) {
            this.dcbStreamIdGenerator = requireNonNull(dcbStreamIdGenerator, DcbStreamIdGenerator.class.getSimpleName() + " cannot be null");
            return this;
        }

        public EventStoreConfig build() {
            return new EventStoreConfig(eventStoreCollectionName, transactionTemplate, timeRepresentation, queryOptions, readOptions, eventStoreCapabilities, dcbStreamIdGenerator);
        }
    }
}
