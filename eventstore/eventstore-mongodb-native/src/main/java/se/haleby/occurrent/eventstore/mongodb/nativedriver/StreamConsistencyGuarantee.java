package se.haleby.occurrent.eventstore.mongodb.nativedriver;


import com.mongodb.TransactionOptions;

public abstract class StreamConsistencyGuarantee {
    private StreamConsistencyGuarantee() {
    }

    public static final class None extends StreamConsistencyGuarantee {
        private static final None INSTANCE = new None();

        private None() {
        }
    }

    public static final class Transactional extends StreamConsistencyGuarantee {
        public final TransactionOptions transactionOptions;
        public final String streamVersionCollectionName;

        private Transactional(TransactionOptions transactionOptions, String streamVersionCollectionName) {
            if (transactionOptions == null) {
                this.transactionOptions = TransactionOptions.builder().build();
            } else {
                this.transactionOptions = transactionOptions;
            }
            this.streamVersionCollectionName = streamVersionCollectionName;
        }
    }

    public static None none() {
        return None.INSTANCE;
    }


    public static Transactional transactional(String streamVersionCollectionName, TransactionOptions transactionOptions) {
        return new Transactional(transactionOptions, streamVersionCollectionName);
    }

    public static Transactional transactional(String streamVersionCollectionName) {
        return new Transactional(null, streamVersionCollectionName);
    }

}