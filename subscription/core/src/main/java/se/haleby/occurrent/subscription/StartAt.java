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

    public static class StartAtStreamPosition extends StartAt {
        public final SubscriptionPosition changeStreamPosition;

        private StartAtStreamPosition(SubscriptionPosition changeStreamPosition) {
            this.changeStreamPosition = changeStreamPosition;
            Objects.requireNonNull(changeStreamPosition, SubscriptionPosition.class.getSimpleName() + " cannot be null");
        }

        @Override
        public String toString() {
            return changeStreamPosition.asString();
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
    public static StartAt streamPosition(SubscriptionPosition changeStreamPosition) {
        return new StartAt.StartAtStreamPosition(changeStreamPosition);
    }
}
