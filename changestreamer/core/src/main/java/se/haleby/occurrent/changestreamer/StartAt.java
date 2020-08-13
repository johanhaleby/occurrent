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
        public final ChangeStreamPosition changeStreamPosition;

        private StartAtStreamPosition(ChangeStreamPosition changeStreamPosition) {
            this.changeStreamPosition = changeStreamPosition;
            Objects.requireNonNull(changeStreamPosition, ChangeStreamPosition.class.getSimpleName() + " cannot be null");
        }

        @Override
        public String toString() {
            return changeStreamPosition.asString();
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
    public static StartAt streamPosition(ChangeStreamPosition changeStreamPosition) {
        return new StartAt.StartAtStreamPosition(changeStreamPosition);
    }
}
