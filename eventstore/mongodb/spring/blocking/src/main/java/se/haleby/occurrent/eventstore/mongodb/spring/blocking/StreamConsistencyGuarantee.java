package se.haleby.occurrent.eventstore.mongodb.spring.blocking;

import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.stream.Stream;

public abstract class StreamConsistencyGuarantee {
    private StreamConsistencyGuarantee() {
    }

    public static final class None extends StreamConsistencyGuarantee {
        private static final None INSTANCE = new None();

        private None() {
        }
    }

    public static final class TransactionAlreadyStarted extends StreamConsistencyGuarantee {
        public final String streamVersionCollectionName;

        private TransactionAlreadyStarted(String streamVersionCollectionName) {
            this.streamVersionCollectionName = streamVersionCollectionName;
        }
    }

    /**
     * Will participate in ongoing Spring Transactions.
     */
    public static final class Transactional extends StreamConsistencyGuarantee {
        public final TransactionTemplate transactionTemplate;
        public final String streamVersionCollectionName;

        private Transactional(TransactionTemplate transactionTemplate, String streamVersionCollectionName) {
            this.transactionTemplate = transactionTemplate;
            this.streamVersionCollectionName = streamVersionCollectionName;
        }
    }

    public static None none() {
        return None.INSTANCE;
    }

    /**
     * Maintain stream consistency by using transactions maintained by a <code>mongoTransactionManager</code>. The <code>mongoTransactionManager</code>
     * will simply be wrapped in a new <code>TransactionTemplate</code>. Will start a new transaction if none is available and join an existing
     * if present.
     *
     * @param streamVersionCollectionName The name of the collection that maintains the version number for the stream
     * @param mongoTransactionManager     The transaction manager to use
     * @return A {@link Transactional} instance with the supplied settings
     */
    public static Transactional transactional(String streamVersionCollectionName, MongoTransactionManager mongoTransactionManager) {
        return new Transactional(new TransactionTemplate(mongoTransactionManager), streamVersionCollectionName);
    }

    /**
     * Maintain stream consistency by using transactions maintained by a <code>mongoTransactionManager</code>. The <code>mongoTransactionManager</code>
     * will simply be wrapped in a new <code>TransactionTemplate</code>. Will start a new transaction if none is available and join an existing
     * if present.
     *
     * @param streamVersionCollectionName The name of the collection that maintains the version number for the stream
     * @param transactionTemplate         The transaction template to use
     * @return A {@link Transactional} instance with the supplied settings
     */
    public static Transactional transactional(String streamVersionCollectionName, TransactionTemplate transactionTemplate) {
        return new Transactional(transactionTemplate, streamVersionCollectionName);
    }


    /**
     * Will use transactions if the method calling {@link SpringBlockingMongoEventStore#write(String, long, Stream)}
     * is annotated with {@link org.springframework.transaction.annotation.Transactional}.
     * If the call is not annotated with `@Transactional` then the event store will make two writes
     * to mongodb, one to <code>streamVersionCollectionName</code> for updating the version of the stream,
     * and one to <code>eventCollectionName</code> for writing the events. If the latter fails the updated
     * stream version will _not_ be reverted. This is probably not what you want, consider {@link #none()}
     * if stream/aggregate consistency is not a concern in your application.
     */
    public static TransactionAlreadyStarted transactionAlreadyStarted(String streamVersionCollectionName) {
        return new TransactionAlreadyStarted(streamVersionCollectionName);
    }
}