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
import org.occurrent.subscription.CatchupSnapshot;

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
     * Whether {@code subscriptionId} is still being given the events that already existed when its replay started,
     * rather than the ones written since. A catch-up usually has both parts. It reads the history it set out to read,
     * then reconciles whatever arrived while it was doing that, and only then hands over, so this answers
     * {@code false} while {@link #isCatchingUp(String)} still answers {@code true}.
     * <p>
     * The default answers {@link #isCatchingUp(String)}, which is the safe answer for a model that cannot tell the
     * two apart. A recording projection then records nothing until the handover
     * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>,
     * decision 6), which costs it the appends the reconciliation delivered, since for some of those the
     * reconciliation is the only delivery they get. Override this if your model reconciles, and answer {@code false}
     * once its history read is done.
     *
     * @param subscriptionId The subscription to ask about.
     * @return {@code true} while this id is reading history it already had.
     */
    default boolean isReplayingHistory(String subscriptionId) {
        return isCatchingUp(subscriptionId);
    }

    /**
     * Which catch-up {@code subscriptionId} is in, as a value that changes when a new one starts and is {@code 0}
     * when none is running. Lets a caller that only samples this model tell one catch-up from the next even when it
     * never sampled the gap between them, which a poll routinely misses for a catch-up whose history read matches
     * nothing.
     * <p>
     * The default cannot tell two catch-ups apart, since it has only {@link #isCatchingUp(String)} to go on. Override
     * it alongside {@link #isReplayingHistory(String)} if your model replays.
     *
     * @param subscriptionId The subscription to ask about.
     * @return A value that changes per catch-up, or {@code 0} while none is running.
     */
    default long catchupGeneration(String subscriptionId) {
        return isCatchingUp(subscriptionId) ? 1L : 0L;
    }

    /**
     * Everything a recorder needs about {@code subscriptionId}'s catch-up, read as one value under whatever the model
     * uses to keep that state consistent. Prefer this to the three questions above when acting on the answer, because
     * a catch-up can finish between two separate calls and produce a pair that never existed.
     * <p>
     * The default composes the three, which is honest for a model whose state is a single map read anyway, and is the
     * one to override when yours is not.
     *
     * @param subscriptionId The subscription to ask about.
     * @return One reading of this subscription's catch-up.
     */
    default CatchupSnapshot catchupSnapshot(String subscriptionId) {
        boolean catchingUp = isCatchingUp(subscriptionId);
        return catchingUp ? new CatchupSnapshot(true, isReplayingHistory(subscriptionId), catchupGeneration(subscriptionId)) : CatchupSnapshot.LIVE;
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
