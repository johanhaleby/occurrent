/*
 * Copyright 2026 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.subscription.api.blocking;

import org.jspecify.annotations.NullMarked;
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.subscription.UnsupportedStartAtException;

import java.util.Optional;

/**
 * A subscription model that can refuse a subscription's start position after {@code subscribe} has returned, on a
 * thread of its own, and tells a listener when it does.
 * <p>
 * The MongoDB subscription models work out a {@link org.occurrent.subscription.StartAt#dynamic(java.util.function.Function)}
 * start position when they open the change stream, and refuse a catch-up's checkpoint they have no change stream
 * position for. They refuse a start at the present too when MongoDB gives them no position for it, and remove the
 * subscription. With {@code restartSubscriptionsOnChangeStreamHistoryLost} off they also refuse a subscription whose
 * change stream history is lost, which MongoDB can report on any read, so also for a subscription that has run for a
 * long time. By then {@code subscribe} has returned, so the caller only learns about it from
 * {@link Subscription#waitUntilStarted(java.time.Duration)}. A model that passes its subscriptions to such a model and
 * keeps state for them, such as a lease, needs to know so it can let go of that state. A restart from the present
 * after a loss of history, and a resume at the present, that gets no position isn't refused. The model holds the
 * subscription paused and keeps its id instead, and tells the listeners through
 * {@link StartRefusalListener#handleHeldPaused(String, UnsupportedStartAtException)}.
 * <p>
 * A catch-up subscription model holds a subscription paused on its replay thread when the history the subscription
 * would go live from is gone, and tells a listener through
 * {@link StartRefusalListener#handleHeldPaused(String, CatchupCannotResumeException)}.
 * <p>
 * Not every subscription model implements this, so reach it with {@link #findIn(SubscriptionModelCapability)}.
 */
@NullMarked
public interface StartRefusalReportingSubscriptions extends SubscriptionModelCapability {

    /**
     * Adds a listener that is told about each start position this model refuses after {@code subscribe} has returned,
     * and about each subscription it refuses after a loss of its history.
     *
     * @param listener The listener to add.
     */
    void addStartRefusalListener(StartRefusalListener listener);

    /**
     * Removes a listener added with {@link #addStartRefusalListener(StartRefusalListener)}, so the model no longer
     * tells it anything or keeps a reference to it. Removing a listener that was never added does nothing.
     *
     * @param listener The listener to remove, the same instance that was added.
     */
    void removeStartRefusalListener(StartRefusalListener listener);

    /**
     * Whether this model still holds {@code subscriptionId} paused for the hold it told
     * {@link StartRefusalListener#handleHeldPaused(String, CatchupCannotResumeException)} about with {@code refusal}. A
     * resume or a {@code start(true)} that runs the subscription again ends that hold, and so does a cancel, so this
     * answers {@code false} from then on, also once a pause or a stop holds the subscription paused again. A listener
     * that acts on the hold later than the call can ask this first. A model that never holds a subscription paused this
     * way answers {@code false}, which this default does.
     *
     * @param subscriptionId The subscription the model reported as held paused.
     * @param refusal        The exact instance the model passed to
     *                       {@link StartRefusalListener#handleHeldPaused(String, CatchupCannotResumeException)}.
     * @return {@code true} while that hold lasts.
     */
    default boolean isHeldPausedFor(String subscriptionId, CatchupCannotResumeException refusal) {
        return false;
    }

    /**
     * Whether this model still holds {@code subscriptionId} paused for the hold it told
     * {@link StartRefusalListener#handleHeldPaused(String, UnsupportedStartAtException)} about with {@code refusal}. A
     * resume or a {@code start(true)} that runs the subscription again ends that hold, and so do a cancel and a
     * shutdown, so this answers {@code false} from then on. That includes once a pause or a stop holds the subscription
     * paused again, and once a {@code subscribe} with the same id made a new subscription. A listener that acts on the
     * hold later than the call can ask this first. A model that never holds a subscription paused this way answers
     * {@code false}, which this default does.
     *
     * @param subscriptionId The subscription the model reported as held paused.
     * @param refusal        The instance the model passed to this listener's
     *                       {@link StartRefusalListener#handleHeldPaused(String, UnsupportedStartAtException)}.
     * @return {@code true} while that hold lasts.
     */
    default boolean isHeldPausedFor(String subscriptionId, UnsupportedStartAtException refusal) {
        return false;
    }

    /**
     * Whether this model tells a listener through {@link StartRefusalListener#handleStartRefusal} about a start position
     * it refuses. A catch-up subscription model refuses none itself. It passes each listener on to the model it wraps
     * and reports only the subscriptions it holds paused, so it answers what that model answers, and {@code false} when
     * that model doesn't implement this interface. A listener that waits for a refusal report after a handle threw the
     * refusal waits only when this answers {@code true}, since nothing would report it otherwise. This default answers
     * {@code true}.
     *
     * @return {@code true} when this model calls {@link StartRefusalListener#handleStartRefusal} for the start positions
     * it refuses after {@code subscribe} has returned.
     */
    default boolean reportsStartRefusals() {
        return true;
    }

    /**
     * Finds the {@link StartRefusalReportingSubscriptions} capability behind {@code subscriptionModel}, unwrapping a
     * {@link SubscriptionModelWrapper} chain until one is found.
     *
     * @param subscriptionModel The subscription model to look in.
     * @return The capability, or empty if nothing in the chain implements it.
     */
    static Optional<StartRefusalReportingSubscriptions> findIn(SubscriptionModelCapability subscriptionModel) {
        return subscriptionModel.capability(StartRefusalReportingSubscriptions.class);
    }

    /**
     * Told about a start position the model refused after {@code subscribe} returned, and about a subscription the model
     * holds paused, because it got no position at the present or because the history it would go live from is gone.
     */
    interface StartRefusalListener {
        /**
         * Called once the model has refused the start position of {@code subscriptionId}. The model removed the
         * subscription and freed its id before calling, so a {@code subscribe} with the same id and a start position the
         * model accepts makes a new subscription, and one may already have done so by the time the listener runs. The
         * model doesn't try the refused start again, and a handle of the refused subscription throws the refusal from
         * its next wait on, also one that answered earlier that the subscription started. A MongoDB subscription model
         * calls this also for a subscription whose change stream history is lost while restarts after that are off,
         * which can come long after the subscription started.
         * <p>
         * Called on the thread that would have read the subscription, without any lock of the model. The model calls
         * each listener in the order they were added. A listener that throws a {@link RuntimeException} is logged, and
         * the model goes on to the next listener. What the model did about the refusal stays as it is either way.
         *
         * @param subscriptionId The subscription whose start position was refused.
         * @param refusal        What the model refused the start position with.
         */
        void handleStartRefusal(String subscriptionId, UnsupportedStartAtException refusal);

        /**
         * Called once the model holds {@code subscriptionId} paused because it got no position to open its change
         * stream at the present, after a loss of history or at a resume. Unlike {@link #handleStartRefusal}, the model
         * keeps the subscription and its id, a {@link SubscriptionModelLifeCycle#resumeSubscription(String)} asks for
         * the present again, and a handle that still waits for the held run to start throws {@code refusal}. A resume or
         * a cancel may already have run by the time the listener runs, and once the model no longer has the
         * subscription a {@code subscribe} with the same id may have made a new one. A listener that acts on the hold
         * later than the call can ask
         * {@link StartRefusalReportingSubscriptions#isHeldPausedFor(String, UnsupportedStartAtException)} first.
         * <p>
         * Called the same way as {@link #handleStartRefusal}, on the thread that would have read the subscription,
         * without any lock of the model, in the order the listeners were added, and a listener that throws a
         * {@link RuntimeException} is logged.
         *
         * @param subscriptionId The subscription the model holds paused.
         * @param refusal        What a handle that waits for the held run to start throws.
         */
        void handleHeldPaused(String subscriptionId, UnsupportedStartAtException refusal);

        /**
         * Called once the model holds {@code subscriptionId} paused because the subscription model under it no longer
         * has the history from the position the subscription would go live from, as a catch-up does when it finds that
         * during or after its replay. Unlike {@link #handleStartRefusal}, the model keeps the subscription and its id, so a
         * {@code subscribe} with the same id is refused until the subscription is cancelled. A handle of the
         * subscription that waits for it to start throws {@code refusal}. Whether a resume runs it again depends on the
         * model. A catch-up replays again when its replay started at the beginning, since that skips nothing, and
         * refuses with {@link CatchupCannotResumeException} otherwise.
         * <p>
         * Called the same way as {@link #handleStartRefusal}: on the thread that would have read the subscription,
         * without any lock of the model, in the order the listeners were added, and a listener that throws a
         * {@link RuntimeException} is logged.
         *
         * @param subscriptionId The subscription the model holds paused.
         * @param refusal        What a handle that waits for the subscription to start throws.
         */
        void handleHeldPaused(String subscriptionId, CatchupCannotResumeException refusal);
    }
}
