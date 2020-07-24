package se.haleby.occurrent.eventstore.mongodb.spring.blocking;

import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

public abstract class StreamConsistencyGuarantee {
    private StreamConsistencyGuarantee() {
    }

    public static final class None extends StreamConsistencyGuarantee {
        private static final None INSTANCE = new None();

        private None() {
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
}