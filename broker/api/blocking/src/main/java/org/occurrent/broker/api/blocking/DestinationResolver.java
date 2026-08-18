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

package org.occurrent.broker.api.blocking;

import io.cloudevents.CloudEvent;
import org.occurrent.subscription.SubscriptionFilter;

import java.util.Optional;
import java.util.Set;

/**
 * Derives where an event goes on a broker, so a publisher and a consumer agree on routing by reading one mapping
 * instead of by matching two hand written strings. A shipped resolver derives the destination from the cloud event
 * type through a {@code CloudEventTypeMapper}, the same mapping an application already uses to convert between a
 * domain class and its cloud event type.
 *
 * @param <D> The transport's own {@link EventDestination} implementation.
 */
public interface DestinationResolver<D extends EventDestination> {

    /**
     * The destination a given event publishes to. Every component of the returned destination is populated.
     */
    D destinationFor(CloudEvent cloudEvent);

    /**
     * The destinations a consumer should bind to narrow what arrives, derived from the event-type part of
     * {@code filter}, which is the only part of a filter a destination mapping can see. A stream id, a data field,
     * a time range and a DCB criteria are all invisible to it.
     * <p>
     * An empty result means the resolver could not narrow this filter to a set of destinations, not that no
     * destination matches, and the caller should bind {@link #catchAllDestination()} instead rather than treat the
     * empty result as "listen to nothing". A binding derived this way only narrows what is delivered to a filter
     * evaluated elsewhere. It never decides what is handled.
     */
    Optional<Set<D>> destinationsFor(SubscriptionFilter filter);

    /**
     * The destination that receives every event this resolver could route, for a consumer that wants no narrowing
     * or whose filter could not be resolved to a set of destinations. Taking the catch-all is always safe, since
     * bindings are a topology decision and narrowing them further is something an application asks for rather
     * than something guessed on its behalf.
     */
    D catchAllDestination();
}
