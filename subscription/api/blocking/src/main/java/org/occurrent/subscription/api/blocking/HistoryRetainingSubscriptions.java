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
 * A subscription model that can say whether acknowledging an event it delivered would destroy the last copy of it,
 * which is what a caller has to know before it hands that event back unprocessed.
 * <p>
 * That is a question about what the acknowledgement costs rather than about what the source happens to hold at this
 * instant, and the two differ. A model reading a store that keeps its own events answers yes whatever later becomes
 * of one, because acknowledging is not what would remove it. Implement it against the cost, not against presence,
 * and read {@link #retains(CloudEvent)} before deciding what your model should answer.
 * <p>
 * The saga executor is the caller this exists for. Quarantining an instance returns normally, which acknowledges the
 * event to whatever fed it, so where that acknowledgement is the thing that drops it, the one copy the saga could
 * ever be given is gone the moment it is quarantined.
 * <p>
 * Asked per event rather than once per model, because for some models the answer genuinely differs between two
 * events. A model that replays a store on the way up and takes live events from somewhere else keeps whichever of
 * them something wrote there, and it cannot know that its live source writes there at all. Answering from the store
 * beats asking whoever wired it, since a promise about a broker and an event store on someone else's machine is a
 * promise nobody can check and the failure is silent.
 * <p>
 * A model that cannot answer the question at all does not implement this, and absence therefore means unable to say
 * rather than a no. The two are different and a caller treats them differently. Nothing found means the question
 * cannot be put to this model, and a caller has to give up on quarantining anything it delivers. A model that is
 * found and answers {@code false} has answered, about one event, and the next event may well get a yes.
 * <p>
 * Implement it only where the answer can be worked out. A model that would have to guess is more useful saying
 * nothing, since a caller can then tell that nobody knows rather than being told no by something that never knew.
 *
 * @see #retains(CloudEvent)
 */
@NullMarked
public interface HistoryRetainingSubscriptions extends SubscriptionModelCapability {

    /**
     * Whether acknowledging {@code event} to the source this model reads leaves it obtainable, rather than destroying
     * the only copy of it that exists.
     * <p>
     * The question is what the acknowledgement costs, not whether the event happens to be present at this instant, and
     * the two come apart in one direction worth naming. A model reading a store that keeps its own events answers
     * {@code true} whatever later becomes of the event, because acknowledging is not what would remove it. An event an
     * operator erases through {@code EventStoreOperations} is gone by that erasure, and answering {@code false} for it
     * would leave an instance blocked forever on an event nobody can supply.
     * <p>
     * Where the acknowledgement is what removes it, a broker committing an offset for an event no store here holds,
     * the answer is {@code false} and the caller has to keep retrying instead.
     * <p>
     * Answer {@code false} rather than throwing when the source cannot be reached or cannot be asked, since a caller
     * uses this to decide whether it may drop an event and an unanswerable question has to read the same way as a no.
     *
     * @param event The event a caller is about to stop retrying.
     * @return {@code true} when acknowledging costs nothing, {@code false} when it destroys the last copy or cannot be checked.
     */
    boolean retains(CloudEvent event);

    /**
     * Whether acknowledging costs nothing for every event this model delivers, so {@link #retains(CloudEvent)} is a
     * formality rather than a real question. A model reading the event store's own change stream answers {@code true},
     * since acknowledging advances a checkpoint and removes nothing.
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
     * found. An empty result means nothing in the chain declares that it can answer, which is not a finding that the
     * event is gone. A caller has to act the same way on both, keeping the event, since a model that cannot be asked
     * cannot be relied on to still have it.
     * <p>
     * A wrapper that declares nothing is answered by what it wraps, which is how a catch-up model over one of the
     * MongoDB models reaches a yes.
     *
     * @param subscriptionModel A {@link Subscribable}, a {@link SubscriptionModelLifeCycle}, a whole {@link SubscriptionModel},
     *                          or a {@link SubscriptionModelWrapper} around one of these. Typed as {@link SubscriptionModelCapability}
     *                          because callers hold different subsets of a subscription model's capabilities, and no
     *                          existing type names their union.
     * @return The model that can answer, or empty when nothing in the chain can be asked, which is not a finding
     *         that the chain retains nothing.
     */
    static Optional<HistoryRetainingSubscriptions> findIn(SubscriptionModelCapability subscriptionModel) {
        return subscriptionModel.capability(HistoryRetainingSubscriptions.class);
    }
}
