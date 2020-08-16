package se.haleby.occurrent.changestreamer.api.reactor;

import io.cloudevents.CloudEvent;
import reactor.core.publisher.Flux;
import se.haleby.occurrent.changestreamer.ChangeStreamFilter;
import se.haleby.occurrent.changestreamer.ChangeStreamPosition;
import se.haleby.occurrent.changestreamer.StartAt;

/**
 * Common interface for reactor (reactive) change streamers. The purpose of a change streamer is to read events from an event store
 * and react to these events. Typically a change streamer will forward the event to another piece of infrastructure such as
 * a message bus or to create views from the events (such as projections, sagas, snapshots etc).
 *
 * @param <T> The type of the {@link CloudEvent} that the change streamer produce. It's common that change streamers
 *            produce "wrappers" around {@code CloudEvent}'s that includes the change stream position if the event store
 *            doesn't maintain this.
 */
public interface ReactorChangeStreamer<T extends CloudEvent> {

    /**
     * Stream events from the event store as they arrive and provide a function which allows to configure the
     * {@link T} that is used. Use this method if want to start streaming from a specific
     * position.
     *
     * @return A {@link Flux} with cloud events which also includes the {@link ChangeStreamPosition} that can be used to resume the stream from the current position.
     */
    Flux<T> stream(ChangeStreamFilter filter, StartAt startAt);

    /**
     * Stream events from the event store as they arrive but filter only events that matches the <code>filter</code>.
     *
     * @return A {@link Flux} with cloud events which also includes the {@link ChangeStreamPosition} that can be used to resume the stream from the current position.
     */
    default Flux<T> stream(ChangeStreamFilter filter) {
        return stream(filter, StartAt.now());
    }


    /**
     * Stream events from the event store as they arrive from the given start position ({@code startAt}).
     *
     * @return A {@link Flux} with cloud events which also includes the {@link ChangeStreamPosition} that can be used to resume the stream from the current position.
     */
    default Flux<T> stream(StartAt startAt) {
        return stream(null, startAt);
    }

    /**
     * Stream events from the event store as they arrive.
     *
     * @return A {@link Flux} with cloud events which also includes the {@link ChangeStreamPosition} that can be used to resume the stream from the current position.
     */
    default Flux<T> stream() {
        return stream(null, StartAt.now());
    }
}