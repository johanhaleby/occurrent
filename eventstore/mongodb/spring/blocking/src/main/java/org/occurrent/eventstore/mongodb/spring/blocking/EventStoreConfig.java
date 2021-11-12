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

import com.mongodb.WriteConcern;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.lang.Nullable;
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
    private static final WriteConcern DEFAULT_WRITE_CONCERN = WriteConcern.MAJORITY;

    public final String eventStoreCollectionName;
    public final TransactionTemplate transactionTemplate;
    public final TimeRepresentation timeRepresentation;
    public final WriteConcern writeConcern;
    public final Function<Query, Query> queryOptions;

    /**
     * Create a new instance of {@code EventStoreConfig}.
     *
     * @param eventStoreCollectionName The collection in which the events are persisted
     * @param transactionTemplate      The transaction template responsible to starting MongoDB transactions (see {@link Builder} for overloads).
     * @param timeRepresentation       How time should be represented in the database
     */
    public EventStoreConfig(String eventStoreCollectionName, TransactionTemplate transactionTemplate, TimeRepresentation timeRepresentation) {
        this(eventStoreCollectionName, transactionTemplate, timeRepresentation, DEFAULT_QUERY_OPTIONS_FUNCTION, DEFAULT_WRITE_CONCERN);
    }

    private EventStoreConfig(String eventStoreCollectionName, TransactionTemplate transactionTemplate, TimeRepresentation timeRepresentation, Function<Query, Query> queryOptions, @Nullable WriteConcern writeConcern) {
        requireNonNull(eventStoreCollectionName, "Event store collection name cannot be null");
        requireNonNull(transactionTemplate, TransactionTemplate.class.getSimpleName() + " cannot be null");
        requireNonNull(timeRepresentation, TimeRepresentation.class.getSimpleName() + " cannot be null");
        // Note that we deliberately allow the WriteConcern to be null in order to be able to use the default MongoTemplate settings
        this.eventStoreCollectionName = eventStoreCollectionName;
        this.transactionTemplate = transactionTemplate;
        this.timeRepresentation = timeRepresentation;
        this.queryOptions = queryOptions == null ? DEFAULT_QUERY_OPTIONS_FUNCTION : queryOptions;
        this.writeConcern = writeConcern;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventStoreConfig)) return false;
        EventStoreConfig that = (EventStoreConfig) o;
        return Objects.equals(eventStoreCollectionName, that.eventStoreCollectionName) && Objects.equals(transactionTemplate, that.transactionTemplate) && timeRepresentation == that.timeRepresentation && Objects.equals(writeConcern, that.writeConcern) && Objects.equals(queryOptions, that.queryOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventStoreCollectionName, transactionTemplate, timeRepresentation, writeConcern, queryOptions);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", EventStoreConfig.class.getSimpleName() + "[", "]")
                .add("eventStoreCollectionName='" + eventStoreCollectionName + "'")
                .add("transactionTemplate=" + transactionTemplate)
                .add("timeRepresentation=" + timeRepresentation)
                .add("writeConcern=" + writeConcern)
                .add("queryOptions=" + queryOptions)
                .toString();
    }

    public static final class Builder {
        private String eventStoreCollectionName;
        private TransactionTemplate transactionTemplate;
        private TimeRepresentation timeRepresentation;
        private Function<Query, Query> queryOptions = DEFAULT_QUERY_OPTIONS_FUNCTION;
        private WriteConcern writeConcern = DEFAULT_WRITE_CONCERN;

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
         * @param writeConcern The {@code WriteConcern} to use when writing to the "events" collection. Writes to other collections will use the default setting from {@code MongoTemplate}.
         * @return A same {@code Builder instance}
         */
        public Builder writeConcern(WriteConcern writeConcern) {
            Objects.requireNonNull(writeConcern, WriteConcern.class.getSimpleName() + " cannot be null");
            this.writeConcern = writeConcern;
            return this;
        }

        /**
         * Use the default {@code WriteConcern} preference determined by {@code MongoTemplate}. If it has not yet been specified through the driver at a higher level (such as {@code com.mongodb.client.MongoClient}),
         * you can set the {@code com.mongodb.WriteConcern} property that the MongoTemplate uses for write operations.
         * If the WriteConcern property is not set, it defaults to the one set in the MongoDB driver’s DB or Collection setting.
         * <p>
         * (This piece of documentation is copied from <a href="https://docs.spring.io/spring-data/mongodb/docs/3.2.6/reference/html/#mongo-template.writeconcern">Spring MongoDB documentation</a>).
         *
         * @return A same {@code Builder instance}
         */
        public Builder useDefaultWriteConcern() {
            this.writeConcern = null;
            return this;
        }

        /**
         * Specify a function that can be used to configure the query options used for {@link org.occurrent.eventstore.api.blocking.EventStore#read(String)} and {@link org.occurrent.eventstore.api.blocking.EventStoreQueries}.
         * This is an advanced feature and should be used sparingly. For example, you can configure cursor timeout, whether slave is OK, etc. By default, mongodb default query options are used.
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


        public EventStoreConfig build() {
            return new EventStoreConfig(eventStoreCollectionName, transactionTemplate, timeRepresentation, queryOptions, writeConcern);
        }
    }
}