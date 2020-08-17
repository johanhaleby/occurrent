package se.haleby.occurrent.subscription;

/**
 * Represents the position for a subscription. Note that this is not the same as a CloudEvent position in an event stream.
 * Rather the subscription position is the position for a particular subscriber that streams events from a subscription.
 * <p>
 * There can be multiple implementations of a {@code SubscriptionPosition} for a database but each implementation
 * must be able to return the subscription position as a {@link String}. Implementations can provide optimized data structures
 * for a particular database. For example if you're streaming changes from a MongoDB event store <i>and</i> persists the position in
 * MongoDB one could return the MongoDB "Document" after having casted the {@code SubscriptionPosition} to a MongoDB specific implementation.
 * Thus there's no need to serialize the "Document" representing the subscription position to a String and back to a Document again.
 * </p>
 */
public interface SubscriptionPosition {
    String asString();
}