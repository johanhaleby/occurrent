package se.haleby.occurrent.subscription.api.reactor;

import io.cloudevents.CloudEvent;
import reactor.core.publisher.Flux;
import se.haleby.occurrent.subscription.SubscriptionFilter;
import se.haleby.occurrent.subscription.SubscriptionPosition;
import se.haleby.occurrent.subscription.StartAt;

/**
 * Common interface for reactor (reactive) subscriptions. The purpose of a subscription is to read events from an event store
 * and react to these events. Typically a subscription will forward the event to another piece of infrastructure such as
 * a message bus or to create views from the events (such as projections, sagas, snapshots etc).
 *
 * @param <T> The type of the {@link CloudEvent} that the subscription produce. It's common that subscriptions
 *            produce "wrappers" around {@code CloudEvent}'s that includes the subscription position if the event store
 *            doesn't maintain this.
 */
public interface ReactorSubscription<T extends CloudEvent> {

    /**
     * Stream events from the event store as they arrive and provide a function which allows to configure the
     * {@link T} that is used. Use this method if want to start streaming from a specific
     * position.
     *
     * @return A {@link Flux} with cloud events which also includes the {@link SubscriptionPosition} that can be used to resume the stream from the current position.
     */
    Flux<T> subscribe(SubscriptionFilter filter, StartAt startAt);

    /**
     * Stream events from the event store as they arrive but filter only events that matches the <code>filter</code>.
     *
     * @return A {@link Flux} with cloud events which also includes the {@link SubscriptionPosition} that can be used to resume the stream from the current position.
     */
    default Flux<T> subscribe(SubscriptionFilter filter) {
        return subscribe(filter, StartAt.now());
    }


    /**
     * Stream events from the event store as they arrive from the given start position ({@code startAt}).
     *
     * @return A {@link Flux} with cloud events which also includes the {@link SubscriptionPosition} that can be used to resume the stream from the current position.
     */
    default Flux<T> subscribe(StartAt startAt) {
        return subscribe(null, startAt);
    }

    /**
     * Stream events from the event store as they arrive.
     *
     * @return A {@link Flux} with cloud events which also includes the {@link SubscriptionPosition} that can be used to resume the stream from the current position.
     */
    default Flux<T> subscribe() {
        return subscribe(null, StartAt.now());
    }
}