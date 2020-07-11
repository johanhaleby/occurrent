package se.haleby.occurrent.eventstore.mongodb.spring.blocking;

import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

public abstract class StreamWriteConsistencyGuarantee {
    private StreamWriteConsistencyGuarantee() {
    }

    public static class None extends StreamWriteConsistencyGuarantee {
        private static final None INSTANCE = new None();

        private None() {
        }
    }

    public static class Transactional extends StreamWriteConsistencyGuarantee {
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

    public static Transactional transactional(String streamVersionCollectionName, MongoTransactionManager mongoTransactionManager) {
        return new Transactional(new TransactionTemplate(mongoTransactionManager), streamVersionCollectionName);
    }

    public static Transactional transactional(String streamVersionCollectionName, TransactionTemplate transactionTemplate) {
        return new Transactional(transactionTemplate, streamVersionCollectionName);
    }
}