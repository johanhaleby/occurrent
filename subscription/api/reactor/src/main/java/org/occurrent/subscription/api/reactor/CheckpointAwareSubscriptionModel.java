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
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.Checkpoint;
import reactor.core.publisher.Mono;

/**
 * A {@link FluxSubscriptionModel} that produces {@link CheckpointAwareCloudEvent} compatible {@link CloudEvent}'s.
 * This is useful for subscriptions that want to persist the position for a given event if the event store doesn't
 * maintain the position for subscriptions automatically.
 */
@NullMarked
public interface CheckpointAwareSubscriptionModel extends FluxSubscriptionModel {

    /**
     * The global checkpoint might be e.g. the wall clock time of the server, vector clock, number of events consumed etc.
     * This is useful to get the initial position of a subscription before any message has been consumed by the subscription
     * (and thus no {@link Checkpoint} has been persisted for the subscription). The reason for doing this would be
     * to make sure that a subscription doesn't lose the very first message if there's an error consuming the first event.
     * <p>
     * Completing empty is a documented answer, not a hypothetical one: it means there's an unresolvable problem,
     * the same condition the blocking {@code CheckpointAwareSubscriptionModel} reports as a {@code null} checkpoint.
     * A model that completes empty here cannot seed a catch-up handover from this position, but otherwise remains a
     * working, live subscription.
     *
     * @return A {@link Mono} that emits the global checkpoint for the database, or completes empty if there's an
     * unresolvable problem.
     */
    Mono<Checkpoint> globalCheckpoint();
}
