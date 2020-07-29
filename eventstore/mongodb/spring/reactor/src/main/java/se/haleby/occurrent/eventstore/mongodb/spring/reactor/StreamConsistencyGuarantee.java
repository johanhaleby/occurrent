package se.haleby.occurrent.eventstore.mongodb.spring.reactor;

import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;

import static java.util.Objects.requireNonNull;

public abstract class StreamConsistencyGuarantee {
    private StreamConsistencyGuarantee() {
    }

    public static final class None extends StreamConsistencyGuarantee {
        private static final None INSTANCE = new None();

        private None() {
        }
    }

    public static final class TransactionInsertsOnly extends StreamConsistencyGuarantee {
        public final TransactionalOperator transactionalOperator;

        private TransactionInsertsOnly(TransactionalOperator transactionalOperator) {
            this.transactionalOperator = requireNonNull(transactionalOperator, "transactionalOperator cannot be null");
        }
    }

    /**
     * Will participate in ongoing Spring Transactions.
     */
    public static final class Transactional extends StreamConsistencyGuarantee {
        public final TransactionalOperator transactionalOperator;
        public final String streamVersionCollectionName;

        private Transactional(TransactionalOperator transactionOperator, String streamVersionCollectionName) {
            this.transactionalOperator = transactionOperator;
            this.streamVersionCollectionName = streamVersionCollectionName;
        }
    }

    public static None none() {
        return None.INSTANCE;
    }

    /**
     * Don't enforce a stream consistency but make sure that inserts (of events) is made in a transaction.
     * This will enforce that all events written using {@link SpringReactorMongoEventStore#write(String, Flux)}
     * will be inserted _or_ no events will be inserted if there's a failure (such as a duplicate). If using {@link #none()} then
     * failing inserts will behave in the manner described <a href="https://docs.mongodb.com/manual/reference/method/db.collection.insertMany/#execution-of-operations">here</a>.
     * Note that this mode is implied in {@link #transactional(String, ReactiveTransactionManager)} mode.
     */
    public static TransactionInsertsOnly transactionalInsertsOnly(ReactiveTransactionManager reactiveTransactionManager) {
        return new TransactionInsertsOnly(TransactionalOperator.create(reactiveTransactionManager));
    }

    /**
     * Don't enforce a stream consistency but make sure that inserts (of events) is made in a transaction.
     * This will enforce that all events written using {@link SpringReactorMongoEventStore#write(String, Flux)}
     * will be inserted _or_ no events will be inserted if there's a failure (such as a duplicate). If using {@link #none()} then
     * failing inserts will behave in the manner described <a href="https://docs.mongodb.com/manual/reference/method/db.collection.insertMany/#execution-of-operations">here</a>.
     * Note that this mode is implied in {@link #transactional(String, ReactiveTransactionManager)} mode.
     */
    public static TransactionInsertsOnly transactionalInsertsOnly(TransactionalOperator transactionalOperator) {
        return new TransactionInsertsOnly(transactionalOperator);
    }

    /**
     * Maintain stream consistency by using transactions maintained by a <code>transactionalOperator</code>. The <code>transactionalOperator</code>
     * will simply be wrapped in a new <code>TransactionalOperator</code>. Will start a new transaction if none is available and join an existing
     * if present.
     *
     * @param streamVersionCollectionName The name of the collection that maintains the version number for the stream
     * @param reactiveTransactionManager  The transaction manager to use
     * @return A {@link Transactional} instance with the supplied settings
     */
    public static Transactional transactional(String streamVersionCollectionName, ReactiveTransactionManager reactiveTransactionManager) {
        return new Transactional(TransactionalOperator.create(reactiveTransactionManager), streamVersionCollectionName);
    }

    /**
     * Maintain stream consistency by using transactions maintained by a <code>transactionalOperator</code>. The <code>transactionalOperator</code>
     * will simply be wrapped in a new <code>TransactionalOperator</code>. Will start a new transaction if none is available and join an existing
     * if present.
     *
     * @param streamVersionCollectionName The name of the collection that maintains the version number for the stream
     * @param transactionalOperator       The transactional operator to use
     * @return A {@link Transactional} instance with the supplied settings
     */
    public static Transactional transactional(String streamVersionCollectionName, TransactionalOperator transactionalOperator) {
        return new Transactional(transactionalOperator, streamVersionCollectionName);
    }
}