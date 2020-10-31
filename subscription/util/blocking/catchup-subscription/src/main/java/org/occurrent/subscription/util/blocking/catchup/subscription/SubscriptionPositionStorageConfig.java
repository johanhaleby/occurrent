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

    /**
     * Don't use a subscription position storage. The catch-up subscription will start from beginning of time each time it is started (for example
     * each time the application is restarted).
     *
     * @return An instance of {@link DontUseSubscriptionPositionInStorage}.
     */
    public static DontUseSubscriptionPositionInStorage dontSubscriptionPositionStorage() {
        return new DontUseSubscriptionPositionInStorage();
    }

    /**
     * Use a specific storage instance. The catch-up subscription will use this storage to check if a position has already been persisted,
     * and if so the catch-up subscription, will continue from this position. The catch-up subscription will delegate to the wrapping subscription
     * if the position belongs to it.
     * <br><br>
     * This is really useful if you want to start-off with a catch-up subscription but then automatically continue with from the wrapped subscription
     * position once the events have caught up.
     * <br><br>
     * Note that if this setting is not combined with {@link UseSubscriptionPositionInStorage#andPersistSubscriptionPositionDuringCatchupPhaseForEveryNEvents(int)}
     * or {@link UseSubscriptionPositionInStorage#andPersistSubscriptionPositionDuringCatchupPhaseWhen(Predicate)} the subscription position
     * is will not be stored during the catch-up phase. This means that if the application crashes during catch-up it'll restart from the beginning
     * when the application is restarted. Combine this settings with any of the two methods defined above to alleviate this, if deemed required.
     *
     * @param storage The storage to use. Must be the same instance as used by the wrapped subscription in order to allow continuing from the subscription position
     *                on application restart.
     * @return A {@link UseSubscriptionPositionInStorage} instance.
     */
    public static UseSubscriptionPositionInStorage useSubscriptionPositionStorage(BlockingSubscriptionPositionStorage storage) {
        return new UseSubscriptionPositionInStorage(storage);
    }

    static final class DontUseSubscriptionPositionInStorage extends SubscriptionPositionStorageConfig {
    }


    public static class UseSubscriptionPositionInStorage extends SubscriptionPositionStorageConfig {
        public final BlockingSubscriptionPositionStorage storage;

        UseSubscriptionPositionInStorage(BlockingSubscriptionPositionStorage storage) {
            Objects.requireNonNull(storage, BlockingSubscriptionPositionStorage.class.getSimpleName() + " cannot be null");
            this.storage = storage;
        }

        /**
         * Configure the catch-up subscription to periodically store store the event position in a storage in case
         * the application is restarted during the catch-up phase. On restart the application will continue from the
         * last stored position, instead of starting from the beginning. This is useful if you have lot's of events
         * and don't want to risk starting from the beginning on failure!
         *
         * @param persistCloudEventPositionPredicate A predicate that evaluates to <code>true</code> if the cloud event position should be persisted for the <i>catch-up</i> subscription.
         *                                           See {@link EveryN}. Supply a predicate that always returns {@code false} to never store the position.
         * @return An instance of {@link PersistSubscriptionPositionDuringCatchupPhase}
         * @see EveryN
         */
        public PersistSubscriptionPositionDuringCatchupPhase andPersistSubscriptionPositionDuringCatchupPhaseWhen(Predicate<CloudEvent> persistCloudEventPositionPredicate) {
            return new PersistSubscriptionPositionDuringCatchupPhase(storage, persistCloudEventPositionPredicate);
        }

        /**
         * Configure the catch-up subscription to periodically store store the event position in a storage in case
         * the application is restarted during the catch-up phase. On restart the application will continue from the
         * last stored position, instead of starting from the beginning. This is useful if you have lot's of events
         * and don't want to risk starting from the beginning on failure!
         *
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

    /**
     * @see UseSubscriptionPositionInStorage#andPersistSubscriptionPositionDuringCatchupPhaseWhen(Predicate)
     * @see UseSubscriptionPositionInStorage#andPersistSubscriptionPositionDuringCatchupPhaseForEveryNEvents(int)
     */
    public static final class PersistSubscriptionPositionDuringCatchupPhase extends UseSubscriptionPositionInStorage {
        public final Predicate<CloudEvent> persistCloudEventPositionPredicate;

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