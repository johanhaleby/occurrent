package se.haleby.occurrent.changestreamer;

/**
 * Represents the position for a change stream. Note that this is not the same as a CloudEvent position in an event stream.
 * Rather the change stream position is the position for a particular subscriber that streams events from a change streamer.
 * <p>
 * There can be multiple implementations of a {@code ChangeStreamPosition} for a database but each implementation
 * must be able to return the change stream position as a {@link String}. Implementations can provide optimized data structures
 * for a particular database. For example if you're streaming changes from a MongoDB event store <i>and</i> persists the position in
 * MongoDB one could return the MongoDB "Document" after having casted the {@code ChangeStreamPosition} to a MongoDB specific implementation.
 * Thus there's no need to serialize the "Document" representing the change stream position to a String and back to a Document again.
 * </p>
 */
public interface ChangeStreamPosition {
    String asString();
}