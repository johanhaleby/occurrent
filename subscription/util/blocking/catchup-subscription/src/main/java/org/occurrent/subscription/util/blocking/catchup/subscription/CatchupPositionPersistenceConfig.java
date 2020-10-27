package org.occurrent.subscription.util.blocking.catchup.subscription;

import io.cloudevents.CloudEvent;
import org.occurrent.subscription.api.blocking.BlockingSubscriptionPositionStorage;
import org.occurrent.subscription.util.predicate.EveryN;

import java.util.Objects;
import java.util.function.Predicate;

/**
 * Configures if and how subscription position persistence should be handled during the catch-up phase.
 */
public abstract class CatchupPositionPersistenceConfig {
    private CatchupPositionPersistenceConfig() {
    }


    public static final class DontPersistSubscriptionPositionDuringCatchupPhase extends CatchupPositionPersistenceConfig {
    }

    public static final class PersistSubscriptionPositionDuringCatchupPhase extends CatchupPositionPersistenceConfig {
        public final BlockingSubscriptionPositionStorage storage;
        public final Predicate<CloudEvent> persistCloudEventPositionPredicate;

        /*
         * @param storage                            The storage that will maintain the subscription position during catch-up mode.
         */
        public PersistSubscriptionPositionDuringCatchupPhase(BlockingSubscriptionPositionStorage storage) {
            this(storage, EveryN.every(10));
        }

        /**
         * @param storage                            The storage that will maintain the subscription position during catch-up mode.
         * @param persistCloudEventPositionPredicate A predicate that evaluates to <code>true</code> if the cloud event position should be persisted. See {@link EveryN}.
         *                                           Supply a predicate that always returns {@code false} to never store the position.
         */
        public PersistSubscriptionPositionDuringCatchupPhase(BlockingSubscriptionPositionStorage storage, Predicate<CloudEvent> persistCloudEventPositionPredicate) {
            Objects.requireNonNull(storage, BlockingSubscriptionPositionStorage.class.getSimpleName() + " cannot be null");
            Objects.requireNonNull(persistCloudEventPositionPredicate, "persistCloudEventPositionPredicate cannot be null");
            this.storage = storage;
            this.persistCloudEventPositionPredicate = persistCloudEventPositionPredicate;
        }

        /**
         * @param storage                            The storage that will maintain the subscription position during catch-up mode.
         * @param persistPositionForEveryNCloudEvent Persist the position of every N cloud event so that it's possible to avoid restarting from scratch when subscription is restarted.
         */
        public PersistSubscriptionPositionDuringCatchupPhase(BlockingSubscriptionPositionStorage storage, int persistPositionForEveryNCloudEvent) {
            this(storage, EveryN.every(persistPositionForEveryNCloudEvent));
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