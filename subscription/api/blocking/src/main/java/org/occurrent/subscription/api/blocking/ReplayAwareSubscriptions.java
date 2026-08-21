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
import org.occurrent.subscription.CatchupListener;

import java.util.Optional;

/**
 * A subscription model that replays history before it delivers live events, and can say which of its subscriptions are
 * still in that replay.
 * <p>
 * Deliberately not answerable from {@link SubscriptionModelLifeCycle#isRunning(String)}, which is {@code true}
 * throughout a replay. A caller that needs the handover specifically has to be able to ask for it: a saga fed by such a
 * model gates its timers on being live, and firing a timeout mid-replay would decide against state that is only half
 * folded up.
 * <p>
 * Not every subscription model replays, so reach it with {@link #findIn(SubscriptionModelCapability)} rather than assuming a concrete class.
 */
@NullMarked
public interface ReplayAwareSubscriptions extends SubscriptionModelCapability {

    /**
     * Whether {@code subscriptionId} is still replaying history and has not yet handed over to live delivery.
     * <p>
     * {@code false} for an id this model has never seen, so never-subscribed and handed-over read the same way, which
     * is what a poll wants. {@code false} once a replay has ended, whether it finished, was stopped, or failed: a
     * failed subscription keeps its registration and refuses events, so ask {@link SubscriptionModelLifeCycle} about
     * that rather than this.
     *
     * @param subscriptionId The subscription to ask about.
     * @return {@code true} while a replay for this id is in flight.
     */
    boolean isCatchingUp(String subscriptionId);

    /**
     * Registers {@code listener} for {@code subscriptionId}'s catch-up boundaries, replacing any listener already
     * registered for that id, and answers whether this model sends them at all.
     * <p>
     * Told rather than asked, because a caller that samples this model has to work out what happened between two of
     * its own readings, and a catch-up that started and finished in between looks like no catch-up at all. A model
     * that sends them calls {@link CatchupListener#catchupStarted(Object)} before the catch-up delivers anything and
     * {@link CatchupListener#historyRead(Object)} once the history it set out to read has been read, both naming
     * that catch-up.
     * <p>
     * Register before subscribing. A listener registered after a catch-up has begun misses its start, and a
     * recording projection behind it would then record the history that catch-up is replaying.
     * <p>
     * The default answers {@code false} and registers nothing, which is the honest answer for a model that cannot
     * tell its catch-ups apart. A caller then falls back to polling {@link #isCatchingUp(String)}, which cannot tell
     * the history a catch-up replays from what was written while it ran, so a recording projection behind such a
     * model records nothing for the whole catch-up
     * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>,
     * decision 6). Override it if your model replays.
     *
     * @param subscriptionId The subscription whose catch-ups the listener wants.
     * @param listener       Told when a catch-up begins and when its history has been read.
     * @return {@code true} when this model sends those, {@code false} when it does not and nothing was registered.
     */
    default boolean listenForCatchup(String subscriptionId, CatchupListener listener) {
        return false;
    }

    /**
     * The replay-aware model behind {@code subscriptionModel}, unwrapping a {@link SubscriptionModelWrapper} until
     * one is found. An empty result means the model cannot say whether it is replaying, which is not the same as
     * having handed over.
     *
     * @param subscriptionModel A {@link Subscribable}, a {@link SubscriptionModelLifeCycle}, a whole {@link SubscriptionModel},
     *                          or a {@link SubscriptionModelWrapper} around one of these. Typed as {@link SubscriptionModelCapability}
     *                          because callers hold different subsets of a subscription model's capabilities, and no
     *                          existing type names their union.
     * @return The replay-aware model, or empty if nothing in the chain implements this.
     */
    static Optional<ReplayAwareSubscriptions> findIn(SubscriptionModelCapability subscriptionModel) {
        return subscriptionModel.capability(ReplayAwareSubscriptions.class);
    }
}
