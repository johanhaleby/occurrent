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

import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Configuration for the blocking Spring java driver for MongoDB EventStore
 */
public class EventStoreConfig {
    private static final Function<Query, Query> DEFAULT_QUERY_OPTIONS_FUNCTION = Function.identity();
    private static final Function<Query, Query> DEFAULT_READ_OPTIONS_FUNCTION = Function.identity();

    public final String eventStoreCollectionName;
    public final TransactionTemplate transactionTemplate;
    public final TimeRepresentation timeRepresentation;
    public final Function<Query, Query> queryOptions;
    public final Function<Query, Query> readOptions;

    /**
     * Create a new instance of {@code EventStoreConfig}.
     *
     * @param eventStoreCollectionName The collection in which the events are persisted
     * @param transactionTemplate      The transaction template responsible to starting MongoDB transactions (see {@link Builder} for overloads).
     * @param timeRepresentation       How time should be represented in the database
     */
    public EventStoreConfig(String eventStoreCollectionName, TransactionTemplate transactionTemplate, TimeRepresentation timeRepresentation) {
        this(eventStoreCollectionName, transactionTemplate, timeRepresentation, DEFAULT_QUERY_OPTIONS_FUNCTION, DEFAULT_READ_OPTIONS_FUNCTION);
    }

    private EventStoreConfig(String eventStoreCollectionName, TransactionTemplate transactionTemplate, TimeRepresentation timeRepresentation, Function<Query, Query> queryOptions, Function<Query, Query> readOptions) {
        requireNonNull(eventStoreCollectionName, "Event store collection name cannot be null");
        requireNonNull(transactionTemplate, TransactionTemplate.class.getSimpleName() + " cannot be null");
        requireNonNull(timeRepresentation, TimeRepresentation.class.getSimpleName() + " cannot be null");
        // Note that we deliberately allow the WriteConcern to be null in order to be able to use the default MongoTemplate settings
        this.eventStoreCollectionName = eventStoreCollectionName;
        this.transactionTemplate = transactionTemplate;
        this.timeRepresentation = timeRepresentation;
        this.queryOptions = queryOptions == null ? DEFAULT_QUERY_OPTIONS_FUNCTION : queryOptions;
        this.readOptions = readOptions == null ? DEFAULT_READ_OPTIONS_FUNCTION : readOptions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventStoreConfig)) return false;
        EventStoreConfig that = (EventStoreConfig) o;
        return Objects.equals(eventStoreCollectionName, that.eventStoreCollectionName) && Objects.equals(transactionTemplate, that.transactionTemplate) && timeRepresentation == that.timeRepresentation && Objects.equals(queryOptions, that.queryOptions) && Objects.equals(readOptions, that.readOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventStoreCollectionName, transactionTemplate, timeRepresentation, queryOptions, readOptions);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", EventStoreConfig.class.getSimpleName() + "[", "]")
                .add("eventStoreCollectionName='" + eventStoreCollectionName + "'")
                .add("transactionTemplate=" + transactionTemplate)
                .add("timeRepresentation=" + timeRepresentation)
                .add("queryOptions=" + queryOptions)
                .add("readOptions=" + readOptions)
                .toString();
    }

    public static final class Builder {
        private String eventStoreCollectionName;
        private TransactionTemplate transactionTemplate;
        private TimeRepresentation timeRepresentation;
        private Function<Query, Query> queryOptions = DEFAULT_QUERY_OPTIONS_FUNCTION;
        private Function<Query, Query> readOptions = DEFAULT_READ_OPTIONS_FUNCTION;

        /**
         * @param eventStoreCollectionName The collection in which the events are persisted
         * @return A same {@code Builder instance}
         */
        public Builder eventStoreCollectionName(String eventStoreCollectionName) {
            this.eventStoreCollectionName = eventStoreCollectionName;
            return this;
        }

        /**
         * @param transactionTemplate The transaction template responsible to starting MongoDB transactions
         * @return A same {@code Builder instance}
         */
        public Builder transactionConfig(TransactionTemplate transactionTemplate) {
            this.transactionTemplate = transactionTemplate;
            return this;
        }

        /**
         * @param mongoTransactionManager Create a {@link TransactionTemplate} from the supplied {@code mongoTransactionManager}
         * @return A same {@code Builder instance}
         */
        public Builder transactionConfig(MongoTransactionManager mongoTransactionManager) {
            this.transactionTemplate = new TransactionTemplate(mongoTransactionManager);
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
         * Specify a function that can be used to configure the query options used for {@link org.occurrent.eventstore.api.blocking.EventStore#read(String)}, i.e. for transactional scenarios.
         * This is an advanced feature and should be used sparingly. For example, you can configure cursor timeout, whether reads from secondaries are OK, etc. By default, mongodb default query options are used.
         * <br><br>
         * Note that you must <i>not</i> use this to change the query itself, i.e. don't use the {@link Query#with(Sort)} etc. Only use options such as {@link Query#cursorBatchSize(int)} that doesn't change
         * the actual query or sort order.
         *
         * @param readOptions The query options function to use, it cannot return null.
         * @return A same {@code Builder instance}
         */
        public Builder readOptions(Function<Query, Query> readOptions) {
            this.readOptions = readOptions;
            return this;
        }


        public EventStoreConfig build() {
            return new EventStoreConfig(eventStoreCollectionName, transactionTemplate, timeRepresentation, queryOptions, readOptions);
        }
    }
}