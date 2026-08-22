package org.occurrent.subscription.api.blocking;

import org.jspecify.annotations.NullMarked;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.SubscriptionNotRunningException;
import org.occurrent.subscription.UnknownSubscriptionException;

/**
 * Defines life-cycle methods for subscription models and subscriptions. Cancellation lives in
 * {@link CancellableSubscriptions}, which this extends, because a register-only model can cancel a subscription
 * without having anything to start, stop, or pause.
 */
@NullMarked
public interface SubscriptionModelLifeCycle extends CancellableSubscriptions {

    /**
     * Temporarily stop the subscription model so that none of its subscriptions will receive any events.
     * It can be started again using {@link #start}.
     * <p>
     * Every subscription that was running is left <i>paused</i>, so {@link #isPaused(String)} returns {@code true} for it,
     * and {@link #resumeSubscription(String)} can bring it back on its own without starting the rest: every other
     * subscription {@code stop()} paused stays paused (its own {@link #isPaused(String)} and {@link #isRunning(String)}
     * are unaffected) until it too is resumed or {@link #start} is called.
     * <p>
     * For a model that owns a single running/stopped flag, resuming that one subscription also reopens the model as
     * a whole: {@link #isRunning()} reports {@code true} again as soon as the first subscription is resumed, since
     * such a model has no state between "everything is stopped" and "the gate is open, and each subscription's own
     * paused/running flag decides whether it uses it". Do not read a {@code true} {@link #isRunning()} after a
     * partial resume as "every subscription that was running before {@code stop()} is delivering again" on such a
     * model, check each one individually with {@link #isRunning(String)}. {@link ManualStartSubscriptionModel} answers
     * the same way, even though it holds registrations back on top of the model it wraps. Resuming one subscription
     * after {@code stop()} makes its {@link #isRunning()} report {@code true} again.
     */
    void stop();

    /**
     * Start a subscription model that was previously stopped, and resume all its subscriptions.
     * <p>
     * Calling this on a model that is already started is allowed. It brings up whatever is not running yet and leaves
     * everything else as it is, so a caller that cannot observe the current state, a leader election or a health check
     * for example, can call it without checking {@link #isRunning()} first.
     *
     * @see #stop()
     * @see #start(boolean)
     */
    default void start() {
        start(true);
    }

    /**
     * Start a subscription model that was previously stopped.
     * <p>
     * Calling this on a model that is already started is allowed, and brings up whatever is not running yet.
     *
     * @param resumeSubscriptionsAutomatically Whether to automatically resume all subscriptions when starting. If <code>false</code>, then the subscriptions must be resumed manually using {@link #resumeSubscription(String)}.
     * @see #stop()
     * @see #start()
     */
    void start(boolean resumeSubscriptionsAutomatically);

    /**
     * @return {@code true} if the subscription model is running, {@code false} otherwise.
     */
    boolean isRunning();

    /**
     * Check if a particular subscription is running.
     *
     * @param subscriptionId The id of the  subscription to check whether it's running or not
     * @return {@code true} if the subscription is running, {@code false} otherwise.
     */
    boolean isRunning(String subscriptionId);

    /**
     * Check if a particular subscription is paused.
     *
     * @param subscriptionId The id of the  subscription to check whether it's paused or not
     * @return {@code true} if the subscription is paused, {@code false} otherwise.
     */
    boolean isPaused(String subscriptionId);

    /**
     * Resume a paused ({@link #pauseSubscription(String)} or {@link #stop()}) subscription. This is useful for testing purposes when you want
     * to write events to an event store and you want a particular subscription to receive these events (but you may have paused
     * others).
     * <p>
     * Resuming a subscription that {@link #stop()} paused makes {@link #isRunning()} report {@code true} again, even
     * though every other subscription {@code stop()} paused is left exactly as {@code stop()} left it, individually
     * paused and not running, until it too is resumed or {@link #start} is called. See {@link #stop()} for why a
     * model has no state in between.
     *
     * @param subscriptionId The id of the subscription to resume.
     * @throws UnknownSubscriptionException       If this subscription model has no subscription with that id.
     * @throws SubscriptionAlreadyRunningException If the subscription is already running. Resuming is a transition of
     *                                             one subscription rather than a goal, so a redundant call is a
     *                                             mistake worth reporting. Starting the whole model is the opposite
     *                                             and accepts being called twice.
     * @throws IllegalStateException              If the subscription cannot be resumed right now for a reason that is
     *                                             not the caller's doing, which on a competing consumer model means
     *                                             another node currently holds the subscription.
     */
    Subscription resumeSubscription(String subscriptionId);

    /**
     * Pause an individual subscription. It'll be paused <i>temporarily</i>, which means that it can be
     * resumed later ({@link #resumeSubscription(String)}). This is useful for testing purposes when you want
     * to write events to an event store without triggering this particular subscription.
     * <p>
     * What happens to an event published while a subscription is paused is up to the model, so read the one you
     * use rather than assuming either answer. A model reading a log or a change stream can resume from the
     * position it had reached and deliver that event, at the price of handing over an event a second time if
     * something else consumed it in the meantime. A model that dispatches events as they arrive has nowhere to
     * hold them, so the event never reaches that handler at all.
     * <p>
     * Not currently delivering is not by itself a reason to refuse this call. A subscription that has started
     * can be paused even while it is not delivering right now, for example a competing consumer that is still
     * waiting for its lock
     * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0112-a-competing-consumer-can-be-paused-while-still-waiting-for-the-lock.md">ADR 112</a>).
     * A registration that has not started yet is a different case, and a model can still refuse that one.
     *
     * @param subscriptionId The id of the subscription to pause.
     * @throws UnknownSubscriptionException   If this subscription model has no subscription with that id.
     * @throws SubscriptionNotRunningException If the subscription exists here but has not started, or is
     *                                         already paused.
     */
    void pauseSubscription(String subscriptionId);

    /**
     * Shutdown the subscription model and close all subscriptions (they can be resumed later if you start from a durable checkpoint).
     * A subscription model that is shutdown cannot be started again, since it closes resources such as database connections,
     * thread pools etc.
     */
    default void shutdown() {
    }
}
