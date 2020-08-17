package se.haleby.occurrent.subscription.api.blocking;

import java.time.Duration;

/**
 * Represents a unique subscription to a subscription. Subscriptions are typically started in a background thread
 * and you may wish to wait ({@link #waitUntilStarted(Duration)} for them to start before continuing.
 */
public interface Subscription {

    /**
     * @return The id of the subscription
     */
    String id();

    /**
     * Synchronous, <strong>blocking</strong> call returns once the {@link Subscription} has started.
     */
    void waitUntilStarted();

    /**
     * Synchronous, <strong>blocking</strong> call returns once the {@link Subscription} has started or
     * {@link Duration timeout} exceeds.
     *
     * @param timeout must not be <code>null</code>
     * @return <code>true</code> if the subscription was started within the given Duration, <code>false</code> otherwise.
     */
    boolean waitUntilStarted(Duration timeout);
}
