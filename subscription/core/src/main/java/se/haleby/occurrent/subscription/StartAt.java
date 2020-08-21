package se.haleby.occurrent.subscription;

import java.util.Objects;

/**
 * Specifies in which position a subscription should start when subscribing to it
 */
public abstract class StartAt {

    public boolean isNow() {
        return this instanceof Now;
    }

    public static class Now extends StartAt {
        private Now() {
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName();
        }
    }

    public static class StartAtSubscriptionPosition extends StartAt {
        public final SubscriptionPosition subscriptionPosition;

        private StartAtSubscriptionPosition(SubscriptionPosition subscriptionPosition) {
            Objects.requireNonNull(subscriptionPosition, SubscriptionPosition.class.getSimpleName() + " cannot be null");
            this.subscriptionPosition = subscriptionPosition;
        }

        @Override
        public String toString() {
            return subscriptionPosition.asString();
        }
    }

    /**
     * Start subscribing to the subscription at this moment in time
     */
    public static StartAt.Now now() {
        return new Now();
    }

    /**
     * Start subscribing to the subscription from the given subscription position
     */
    public static StartAt subscriptionPosition(SubscriptionPosition subscriptionPosition) {
        return new StartAtSubscriptionPosition(subscriptionPosition);
    }
}
