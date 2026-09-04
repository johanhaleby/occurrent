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

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;

import java.util.Optional;

/**
 * A subscription model that can say whether an event it delivered is still obtainable, which is what a caller has to
 * know before it hands that event back unprocessed.
 * <p>
 * The saga executor is the caller this exists for. Quarantining an instance returns normally, which acknowledges the
 * event to whatever fed it, so on a source that no longer holds it the one copy the saga could ever be given is gone
 * the moment it is quarantined.
 * <p>
 * Asked per event rather than once per model, because for some models the answer genuinely differs between two
 * events. A model that replays a store on the way up and takes live events from somewhere else holds whichever of
 * them the store was written with, and it cannot know that its live source writes there at all. Answering from the
 * store beats asking whoever wired it, since a promise about a broker and an event store on someone else's machine
 * is a promise nobody can check and the failure is silent.
 * <p>
 * A model that keeps nothing does not implement this. There is no answer meaning never, so absence is the only way
 * to say it, and a caller that finds nothing has to read that as the event being gone once it returns.
 *
 * @see #retains(CloudEvent)
 */
@NullMarked
public interface HistoryRetainingSubscriptions extends SubscriptionModelCapability {

    /**
     * Whether {@code event} is still obtainable from the source this model reads, so that returning normally does not
     * lose it.
     * <p>
     * Answer {@code false} rather than throwing when the source cannot be reached or cannot be asked, since a caller
     * uses this to decide whether it may drop an event and an unanswerable question has to read the same way as a no.
     *
     * @param event The event a caller is about to stop retrying.
     * @return {@code true} when the event can still be obtained, {@code false} when it cannot or cannot be checked.
     */
    boolean retains(CloudEvent event);

    /**
     * Whether every event this model delivers is obtainable, so {@link #retains(CloudEvent)} is a formality rather
     * than a real question. A model reading the event store's own change stream answers {@code true}, since the store
     * it reads is the store the events are in.
     * <p>
     * Told rather than worked out, because a caller cannot infer it from one {@code true} answer, and it changes what
     * is worth saying at startup. A model whose answer can vary is worth warning about before an incident. One that
     * always keeps everything is not, and warning about it anyway would be noise on every saga that is fine.
     * <p>
     * The default is {@code false}, which is the answer that costs a message rather than the one that suppresses it.
     *
     * @return {@code true} when this model retains everything it delivers.
     */
    default boolean retainsEveryEvent() {
        return false;
    }

    /**
     * The retaining model behind {@code subscriptionModel}, unwrapping a {@link SubscriptionModelWrapper} until one is
     * found. An empty result means nothing in the chain keeps a delivered event.
     * <p>
     * A wrapper that declares nothing is answered by what it wraps, which is how a catch-up model over one of the
     * MongoDB models reaches a yes.
     *
     * @param subscriptionModel A {@link Subscribable}, a {@link SubscriptionModelLifeCycle}, a whole {@link SubscriptionModel},
     *                          or a {@link SubscriptionModelWrapper} around one of these. Typed as {@link SubscriptionModelCapability}
     *                          because callers hold different subsets of a subscription model's capabilities, and no
     *                          existing type names their union.
     * @return The retaining model, or empty if nothing in the chain retains.
     */
    static Optional<HistoryRetainingSubscriptions> findIn(SubscriptionModelCapability subscriptionModel) {
        return subscriptionModel.capability(HistoryRetainingSubscriptions.class);
    }
}
