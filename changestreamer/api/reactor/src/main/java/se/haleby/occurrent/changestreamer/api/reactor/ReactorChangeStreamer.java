package se.haleby.occurrent.changestreamer.api.reactor;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import se.haleby.occurrent.changestreamer.ChangeStreamFilter;
import se.haleby.occurrent.changestreamer.ChangeStreamPosition;
import se.haleby.occurrent.changestreamer.CloudEventWithChangeStreamPosition;
import se.haleby.occurrent.changestreamer.StartAt;

public interface ReactorChangeStreamer {

    /**
     * Stream events from the event store as they arrive and provide a function which allows to configure the
     * {@link CloudEventWithChangeStreamPosition} that is used. Use this method if want to start streaming from a specific
     * position.
     *
     * @return A {@link Flux} with cloud events which also includes the {@link ChangeStreamPosition} that can be used to resume the stream from the current position.
     */
    Flux<CloudEventWithChangeStreamPosition> stream(ChangeStreamFilter filter, StartAt startAt);

    /**
     * Stream events from the event store as they arrive but filter only events that matches the <code>filter</code>.
     *
     * @return A {@link Flux} with cloud events which also includes the {@link ChangeStreamPosition} that can be used to resume the stream from the current position.
     */
    default Flux<CloudEventWithChangeStreamPosition> stream(ChangeStreamFilter filter) {
        return stream(filter, StartAt.now());
    }


    /**
     * Stream events from the event store as they arrive from the given start position ({@code startAt}).
     *
     * @return A {@link Flux} with cloud events which also includes the {@link ChangeStreamPosition} that can be used to resume the stream from the current position.
     */
    default Flux<CloudEventWithChangeStreamPosition> stream(StartAt startAt) {
        return stream(null, startAt);
    }

    /**
     * Stream events from the event store as they arrive.
     *
     * @return A {@link Flux} with cloud events which also includes the {@link ChangeStreamPosition} that can be used to resume the stream from the current position.
     */
    default Flux<CloudEventWithChangeStreamPosition> stream() {
        return stream(null, StartAt.now());
    }

    /**
     * The global change stream position might be e.g. the wall clock time of the server, vector clock, number of events consumed etc.
     * This is useful to get the initial position of a subscription before any message has been consumed by the subscription
     * (and thus no {@link ChangeStreamPosition} has been persisted for the subscription). The reason for doing this would be
     * to make sure that a subscription doesn't loose the very first message if there's an error consuming the first event.
     *
     * @return The global change stream position for the database.
     */
    Mono<ChangeStreamPosition> globalChangeStreamPosition();
}