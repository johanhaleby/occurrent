package se.haleby.occurrent.changestreamer;

import java.util.Objects;

/**
 * Specifies in which position a change stream should start when subscribing to it
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
        public final StreamPosition streamPosition;

        private StartAtStreamPosition(StreamPosition streamPosition) {
            this.streamPosition = streamPosition;
            Objects.requireNonNull(streamPosition, StreamPosition.class.getSimpleName() + " cannot be null");
        }

        @Override
        public String toString() {
            return streamPosition.asString();
        }
    }

    /**
     * Start subscribing to the change stream at this moment in time
     */
    public static StartAt.Now now() {
        return new Now();
    }

    /**
     * Start subscribing to the change stream from the given stream position
     */
    public static StartAt streamPosition(StreamPosition streamPosition) {
        return new StartAt.StartAtStreamPosition(streamPosition);
    }
}
