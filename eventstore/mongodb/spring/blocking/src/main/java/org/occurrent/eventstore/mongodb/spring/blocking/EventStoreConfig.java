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

import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Objects;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

/**
 * Configuration for the blocking Spring java driver for MongoDB EventStore
 */
public class EventStoreConfig {
    public final String eventStoreCollectionName;
    public final TransactionTemplate transactionTemplate;
    public final TimeRepresentation timeRepresentation;
    public final boolean enableTransactionalReads;

    /**
     * Create a new instance of {@code EventStoreConfig}.
     *
     * @param eventStoreCollectionName The collection in which the events are persisted
     * @param transactionTemplate      The transaction template responsible to starting MongoDB transactions (see {@link Builder} for overloads).
     * @param timeRepresentation       How time should be represented in the database
     */
    public EventStoreConfig(String eventStoreCollectionName, TransactionTemplate transactionTemplate, TimeRepresentation timeRepresentation) {
        this(eventStoreCollectionName, transactionTemplate, timeRepresentation, true);
    }

    private EventStoreConfig(String eventStoreCollectionName, TransactionTemplate transactionTemplate, TimeRepresentation timeRepresentation, boolean enableTransactionalReads) {
        requireNonNull(eventStoreCollectionName, "Event store collection name cannot be null");
        requireNonNull(transactionTemplate, TransactionTemplate.class.getSimpleName() + " cannot be null");
        requireNonNull(timeRepresentation, TimeRepresentation.class.getSimpleName() + " cannot be null");
        this.eventStoreCollectionName = eventStoreCollectionName;
        this.transactionTemplate = transactionTemplate;
        this.timeRepresentation = timeRepresentation;
        this.enableTransactionalReads = enableTransactionalReads;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventStoreConfig)) return false;
        EventStoreConfig that = (EventStoreConfig) o;
        return enableTransactionalReads == that.enableTransactionalReads && Objects.equals(eventStoreCollectionName, that.eventStoreCollectionName) && Objects.equals(transactionTemplate, that.transactionTemplate) && timeRepresentation == that.timeRepresentation;
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventStoreCollectionName, transactionTemplate, timeRepresentation, enableTransactionalReads);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", EventStoreConfig.class.getSimpleName() + "[", "]")
                .add("eventStoreCollectionName='" + eventStoreCollectionName + "'")
                .add("transactionTemplate=" + transactionTemplate)
                .add("timeRepresentation=" + timeRepresentation)
                .add("enableTransactionalReads=" + enableTransactionalReads)
                .toString();
    }

    public static final class Builder {
        private String eventStoreCollectionName;
        private TransactionTemplate transactionTemplate;
        private TimeRepresentation timeRepresentation;
        private boolean enableTransactionalReads;

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
         * Toggle whether to use transactions when reading ({@link org.occurrent.eventstore.api.blocking.EventStore#read(String)} etc) from the event store.
         * This is an advanced feature, and you almost always want to have it enabled. There are two reasons for disabling it:
         *
         * <ol>
         *     <li>There's a bug/limitation on Atlas free tier clusters which yields an exception when reading large number of events in a stream in a transaction.
         *     To workaround this you could disable transactional reads. The exception takes this form:
         *     <pre>
         *         java.lang.IllegalStateException: state should be: open
         * 	       at com.mongodb.assertions.Assertions.isTrue(Assertions.java:79)
         * 	       at com.mongodb.internal.session.BaseClientSessionImpl.getServerSession(BaseClientSessionImpl.java:101)
         * 	       at com.mongodb.internal.session.ClientSessionContext.getSessionId(ClientSessionContext.java:44)
         * 	       at com.mongodb.internal.connection.ClusterClockAdvancingSessionContext.getSessionId(ClusterClockAdvancingSessionContext.java:46)
         * 	       at com.mongodb.internal.connection.CommandMessage.getExtraElements(CommandMessage.java:265)
         * 	       at com.mongodb.internal.connection.CommandMessage.encodeMessageBodyWithMetadata(CommandMessage.java:155)
         * 	       at com.mongodb.internal.connection.RequestMessage.encode(RequestMessage.java:138)
         * 	       at com.mongodb.internal.connection.CommandMessage.encode(CommandMessage.java:59)
         * 	       at com.mongodb.internal.connection.InternalStreamConnection.sendAndReceive(InternalStreamConnection.java:268)
         * 	       at com.mongodb.internal.connection.UsageTrackingInternalConnection.sendAndReceive(UsageTrackingInternalConnection.java:100)
         * 	       at com.mongodb.internal.connection.DefaultConnectionPool$PooledConnection.sendAndReceive(DefaultConnectionPool.java:490)
         * 	       at com.mongodb.internal.connection.CommandProtocolImpl.execute(CommandProtocolImpl.java:71)
         * 	       at com.mongodb.internal.connection.DefaultServer$DefaultServerProtocolExecutor.execute(DefaultServer.java:253)
         * 	       at com.mongodb.internal.connection.DefaultServerConnection.executeProtocol(DefaultServerConnection.java:202)
         * 	       at com.mongodb.internal.connection.DefaultServerConnection.command(DefaultServerConnection.java:118)
         * 	       at com.mongodb.internal.connection.DefaultServerConnection.command(DefaultServerConnection.java:110)
         * 	       at com.mongodb.internal.operation.QueryBatchCursor.getMore(QueryBatchCursor.java:268)
         * 	       at com.mongodb.internal.operation.QueryBatchCursor.hasNext(QueryBatchCursor.java:141)
         * 	       at com.mongodb.client.internal.MongoBatchCursorAdapter.hasNext(MongoBatchCursorAdapter.java:54)
         * 	       at java.base/java.util.Iterator.forEachRemaining(Iterator.java:132)
         * 	       at java.base/java.util.Spliterators$IteratorSpliterator.forEachRemaining(Spliterators.java:1801)
         * 	       at java.base/java.util.stream.AbstractPipeline.copyInto(AbstractPipeline.java:484)
         * 	       at java.base/java.util.stream.AbstractPipeline.wrapAndCopyInto(AbstractPipeline.java:474)
         * 	       at java.base/java.util.stream.ReduceOps$ReduceOp.evaluateSequential(ReduceOps.java:913)
         * 	       at java.base/java.util.stream.AbstractPipeline.evaluate(AbstractPipeline.java:234)
         *     </pre>
         *     It's possible that this would work if you enable "no cursor timeout" on the query, but this is not allowed on Atlas free tier.
         *     </li>
         *     <li>You're set back by the performance penalty of transactions and are willing to sacrifice read consistency</li>
         * </ol>
         * <p>
         * If you disable transactional reads, you <i>may</i> end up with a mismatch between the version number in the {@link org.occurrent.eventstore.api.blocking.EventStream} and
         * the last event returned from the event stream. This is because Occurrent does two reads to MongoDB when reading an event stream. First it finds the current version number of the stream (A),
         * and secondly it queries for all events (B). If you disable transactional reads, then another thread might have written more events before the call to B has been made. Thus, the version number
         * received from query A might be stale. This may or may not be a problem for your domain, but it's generally recommended having transactional reads enabled.
         * <br>
         * <br>
         * <p>
         * Note that this will only affect the {@code read} methods in the event store, the {@code query} methods doesn't use transactions.
         * </p>
         *
         * @param enableTransactionalReads <code>true</code> to enable, <code>false</code> to disable.
         * @return A same {@code Builder instance}
         */
        public Builder transactionalReads(boolean enableTransactionalReads) {
            this.enableTransactionalReads = enableTransactionalReads;
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


        public EventStoreConfig build() {
            return new EventStoreConfig(eventStoreCollectionName, transactionTemplate, timeRepresentation);
        }
    }
}