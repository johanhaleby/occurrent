package org.occurrent.subscription.util.blocking.catchup.subscription;

import io.cloudevents.CloudEvent;
import org.occurrent.subscription.api.blocking.BlockingSubscriptionPositionStorage;
import org.occurrent.subscription.util.predicate.EveryN;

import java.util.Objects;
import java.util.function.Predicate;

/**
 * Configures if and how subscription position persistence should be handled during the catch-up phase.
 */
public abstract class SubscriptionPositionStorageConfig {
    private SubscriptionPositionStorageConfig() {
    }

    public static DontUseSubscriptionPositionInStorage dontSubscriptionPositionStorage() {
        return new DontUseSubscriptionPositionInStorage();
    }

    public static UseSubscriptionPositionInStorage useSubscriptionPositionStorage(BlockingSubscriptionPositionStorage storage) {
        return new UseSubscriptionPositionInStorage(storage);
    }

    static final class DontUseSubscriptionPositionInStorage extends SubscriptionPositionStorageConfig {
    }


    static class UseSubscriptionPositionInStorage extends SubscriptionPositionStorageConfig {
        public final BlockingSubscriptionPositionStorage storage;

        UseSubscriptionPositionInStorage(BlockingSubscriptionPositionStorage storage) {
            Objects.requireNonNull(storage, BlockingSubscriptionPositionStorage.class.getSimpleName() + " cannot be null");
            this.storage = storage;
        }

        /**
         * @param persistCloudEventPositionPredicate A predicate that evaluates to <code>true</code> if the cloud event position should be persisted for the <i>catch-up</i> subscription.
         *                                           See {@link EveryN}. Supply a predicate that always returns {@code false} to never store the position.
         * @return An instance of {@link PersistSubscriptionPositionDuringCatchupPhase}
         * @see EveryN
         */
        public PersistSubscriptionPositionDuringCatchupPhase andPersistSubscriptionPositionDuringCatchupPhaseWhen(Predicate<CloudEvent> persistCloudEventPositionPredicate) {
            return new PersistSubscriptionPositionDuringCatchupPhase(storage, persistCloudEventPositionPredicate);
        }

        /**
         * @param persistPositionForEveryNCloudEvent Persist the position of every N cloud event so that it's possible to avoid restarting from scratch when the <i>catch-up</i> subscription is restarted.
         * @return An instance of {@link PersistSubscriptionPositionDuringCatchupPhase}
         */
        public PersistSubscriptionPositionDuringCatchupPhase andPersistSubscriptionPositionDuringCatchupPhaseForEveryNEvents(int persistPositionForEveryNCloudEvent) {
            return new PersistSubscriptionPositionDuringCatchupPhase(storage, EveryN.every(persistPositionForEveryNCloudEvent));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof UseSubscriptionPositionInStorage)) return false;
            UseSubscriptionPositionInStorage that = (UseSubscriptionPositionInStorage) o;
            return Objects.equals(storage, that.storage);
        }

        @Override
        public int hashCode() {
            return Objects.hash(storage);
        }

        @Override
        public String toString() {
            return "UseSubscriptionPositionInStorage{" +
                    "storage=" + storage +
                    '}';
        }
    }

    static final class PersistSubscriptionPositionDuringCatchupPhase extends UseSubscriptionPositionInStorage {
        public final Predicate<CloudEvent> persistCloudEventPositionPredicate;

        /*
         * @param storage                            The storage that will maintain the subscription position during catch-up mode.
         */
        PersistSubscriptionPositionDuringCatchupPhase(BlockingSubscriptionPositionStorage storage) {
            this(storage, EveryN.every(10));
        }

        /**
         * @param storage                            The storage that will maintain the subscription position during catch-up mode.
         * @param persistCloudEventPositionPredicate A predicate that evaluates to <code>true</code> if the cloud event position should be persisted. See {@link EveryN}.
         *                                           Supply a predicate that always returns {@code false} to never store the position.
         */
        PersistSubscriptionPositionDuringCatchupPhase(BlockingSubscriptionPositionStorage storage, Predicate<CloudEvent> persistCloudEventPositionPredicate) {
            super(storage);
            Objects.requireNonNull(persistCloudEventPositionPredicate, "persistCloudEventPositionPredicate cannot be null");
            this.persistCloudEventPositionPredicate = persistCloudEventPositionPredicate;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof PersistSubscriptionPositionDuringCatchupPhase)) return false;
            PersistSubscriptionPositionDuringCatchupPhase that = (PersistSubscriptionPositionDuringCatchupPhase) o;
            return Objects.equals(storage, that.storage) &&
                    Objects.equals(persistCloudEventPositionPredicate, that.persistCloudEventPositionPredicate);
        }

        @Override
        public int hashCode() {
            return Objects.hash(storage, persistCloudEventPositionPredicate);
        }

        @Override
        public String toString() {
            return "PersistSubscriptionPositionDuringCatchupPhase{" +
                    "storage=" + storage +
                    ", persistCloudEventPositionPredicate=" + persistCloudEventPositionPredicate +
                    '}';
        }
    }
}