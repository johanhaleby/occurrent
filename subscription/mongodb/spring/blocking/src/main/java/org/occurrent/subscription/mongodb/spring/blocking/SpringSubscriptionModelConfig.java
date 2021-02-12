package org.occurrent.subscription.mongodb.spring.blocking;

import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;

import java.time.Duration;

import static java.util.Objects.requireNonNull;

public class SpringSubscriptionModelConfig {

    final String eventCollection;
    final TimeRepresentation timeRepresentation;
    final RetryStrategy retryStrategy;
    final boolean restartSubscriptionsOnChangeStreamHistoryLost;

    /**
     * @param eventCollection    The collection that contains the events
     * @param timeRepresentation How time is represented in the database, must be the same as what's specified for the EventStore that stores the events.
     */
    public SpringSubscriptionModelConfig(String eventCollection, TimeRepresentation timeRepresentation) {
        this(eventCollection, timeRepresentation, RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f), false);
    }

    private SpringSubscriptionModelConfig(String eventCollection, TimeRepresentation timeRepresentation, RetryStrategy retryStrategy, boolean restartSubscriptionsOnChangeStreamHistoryLost) {
        requireNonNull(eventCollection, "eventCollection cannot be null");
        requireNonNull(timeRepresentation, TimeRepresentation.class.getSimpleName() + " cannot be null");
        requireNonNull(retryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
        this.eventCollection = eventCollection;
        this.timeRepresentation = timeRepresentation;
        this.retryStrategy = retryStrategy;
        this.restartSubscriptionsOnChangeStreamHistoryLost = restartSubscriptionsOnChangeStreamHistoryLost;
    }

    /**
     * @param eventCollection    The collection that contains the events
     * @param timeRepresentation How time is represented in the database, must be the same as what's specified for the EventStore that stores the events.
     */
    public static SpringSubscriptionModelConfig withConfig(String eventCollection, TimeRepresentation timeRepresentation) {
        return new SpringSubscriptionModelConfig(eventCollection, timeRepresentation);
    }

    public SpringSubscriptionModelConfig restartSubscriptionsOnChangeStreamHistoryLost(boolean restartSubscriptionsOnChangeStreamHistoryLost) {
        return new SpringSubscriptionModelConfig(eventCollection, timeRepresentation, retryStrategy, restartSubscriptionsOnChangeStreamHistoryLost);
    }

    public SpringSubscriptionModelConfig retryStrategy(RetryStrategy retryStrategy) {
        return new SpringSubscriptionModelConfig(eventCollection, timeRepresentation, retryStrategy, restartSubscriptionsOnChangeStreamHistoryLost);
    }
}
