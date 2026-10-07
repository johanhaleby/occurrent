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
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.GlobalCheckpointSource;
import reactor.core.publisher.Mono;

/**
 * A {@link FluxSubscriptionModel} that produces {@link CheckpointAwareCloudEvent} compatible {@link CloudEvent}'s.
 * This is useful for subscriptions that want to persist the position for a given event if the event store doesn't
 * maintain the position for subscriptions automatically.
 */
@NullMarked
public interface CheckpointAwareSubscriptionModel extends FluxSubscriptionModel, GlobalCheckpointSource<Mono<Checkpoint>> {

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
    @Override
    Mono<Checkpoint> globalCheckpoint();

    /**
     * The global checkpoint at the moment this method is called. Unlike most {@link Mono}s, the one returned doesn't wait for
     * a subscriber before it does its work. The moment is taken when the method is called, and the returned {@code Mono}
     * works out the position for that moment later, when it's subscribed to. A subscription started from the answer
     * receives every event written after the call, even when the {@code Mono} is subscribed to long after it.
     * <p>
     * The answer may be earlier than the call, so a subscription started from it can also receive some events written
     * just before the call. A model that overrides this method should fail the {@code Mono} when it can't answer for
     * the moment of the call, rather than answer with a later position.
     * <p>
     * The default implementation returns {@link #globalCheckpoint()}, which works out the position when that read runs,
     * so events written between the call and the moment the position is worked out can be skipped. A
     * {@code globalCheckpoint()} that answers asynchronously can work it out after the returned {@code Mono} is
     * subscribed to, so subscribing to it at the call doesn't keep those events from being skipped.
     * <p>
     * An implementation should not block when the returned {@code Mono} is subscribed to. For some start positions, the
     * subscription-model default for one, {@code ReactorDurableSubscriptionModel} subscribes to it on the thread that
     * calls its {@code subscribe(..)}, so that a model keeping the default implementation starts its read before that
     * call returns. A {@code Mono} that blocks when subscribed to then blocks that {@code subscribe(..)},
     * also on a thread that must not block, such as a Netty event loop thread.
     *
     * @return A {@link Mono} that emits the global checkpoint as of the call, fails when the position can't be
     * worked out, or, for a model that doesn't override this method, behaves like {@link #globalCheckpoint()}.
     */
    default Mono<Checkpoint> globalCheckpointAsOfNow() {
        return globalCheckpoint();
    }
}
