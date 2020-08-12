package se.haleby.occurrent.changestreamer.api.reactor;

import reactor.core.publisher.Flux;
import se.haleby.occurrent.changestreamer.ChangeStreamFilter;
import se.haleby.occurrent.changestreamer.CloudEventWithStreamPosition;
import se.haleby.occurrent.changestreamer.StartAt;

public interface ReactorChangeStreamer {

    /**
     * Stream events from the event store as they arrive and provide a function which allows to configure the
     * {@link CloudEventWithStreamPosition} that is used. Use this method if want to start streaming from a specific
     * position.
     *
     * @return A {@link Flux} with cloud events which also includes the {@link se.haleby.occurrent.changestreamer.StreamPosition} that can be used to resume the stream from the current position.
     */
    Flux<CloudEventWithStreamPosition> stream(ChangeStreamFilter filter, StartAt startAtSupplier);

    /**
     * Stream events from the event store as they arrive but filter only events that matches the <code>filter</code>.
     *
     * @return A {@link Flux} with cloud events which also includes the {@link se.haleby.occurrent.changestreamer.StreamPosition} that can be used to resume the stream from the current position.
     */
    default Flux<CloudEventWithStreamPosition> stream(ChangeStreamFilter filter) {
        return stream(filter, StartAt.now());
    }


    /**
     * Stream events from the event store as they arrive from the given start position ({@code startAt}).
     *
     * @return A {@link Flux} with cloud events which also includes the {@link se.haleby.occurrent.changestreamer.StreamPosition} that can be used to resume the stream from the current position.
     */
    default Flux<CloudEventWithStreamPosition> stream(StartAt startAt) {
        return stream(null, startAt);
    }

    /**
     * Stream events from the event store as they arrive.
     *
     * @return A {@link Flux} with cloud events which also includes the {@link se.haleby.occurrent.changestreamer.StreamPosition} that can be used to resume the stream from the current position.
     */
    default Flux<CloudEventWithStreamPosition> stream() {
        return stream(null, StartAt.now());
    }
}