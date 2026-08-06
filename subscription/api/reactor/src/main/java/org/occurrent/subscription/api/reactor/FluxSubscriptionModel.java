/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.subscription.api.reactor;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import reactor.core.publisher.Flux;

/**
 * The bare reactive subscription primitive. {@link #subscribe(SubscriptionFilter, StartAt)} hands back a {@link Flux}
 * of cloud events that starts when the caller subscribes to it and stops when the caller disposes it. A subscription
 * reads events from an event store and reacts to them, typically by forwarding each event to another piece of
 * infrastructure such as a message bus, or by using it to build a view such as a projection, saga, or snapshot.
 * <p>
 * This is the counterpart to {@link SubscriptionModel}, which tracks a subscription by id so it can be paused, resumed
 * and cancelled. A model fed by a push source rather than by reading a change stream cannot honour this primitive, so
 * it implements {@link SubscriptionModel} alone.
 */
@NullMarked
public interface FluxSubscriptionModel {

    /**
     * Stream events from the event store as they arrive, matching {@code filter} and starting from {@code startAt}.
     * Use this overload when you need to both filter events and choose a specific start position.
     *
     * @return A {@link Flux} of cloud events.
     */
    Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt);

    /**
     * Stream events from the event store as they arrive, matching {@code filter} only.
     *
     * @return A {@link Flux} of cloud events, each carrying the {@link Checkpoint} you can use to resume the stream from that position.
     */
    default Flux<CloudEvent> subscribe(SubscriptionFilter filter) {
        return subscribe(filter, StartAt.subscriptionModelDefault());
    }


    /**
     * Stream events from the event store as they arrive, starting from {@code startAt}.
     *
     * @return A {@link Flux} of cloud events, each carrying the {@link Checkpoint} you can use to resume the stream from that position.
     */
    default Flux<CloudEvent> subscribe(StartAt startAt) {
        return subscribe(null, startAt);
    }

    /**
     * Stream every event from the event store as it arrives.
     *
     * @return A {@link Flux} of cloud events, each carrying the {@link Checkpoint} you can use to resume the stream from that position.
     */
    default Flux<CloudEvent> subscribe() {
        return subscribe(null, StartAt.subscriptionModelDefault());
    }
}