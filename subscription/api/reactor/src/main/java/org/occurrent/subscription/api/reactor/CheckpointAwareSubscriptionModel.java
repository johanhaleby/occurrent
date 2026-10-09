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
     * The global checkpoint at the moment this method is called. An implementation answers with a position no later
     * than where its feed was when this method was called, however late the returned {@code Mono} is subscribed to or
     * answers, within the limits it documents. A subscription started from such an answer receives every event written
     * after the call. Unlike most {@link Mono}s, the one returned doesn't wait for a subscriber before it fixes the
     * moment it answers for.
     * <p>
     * The answer may be earlier than the call, so a subscription started from it can also receive some events written
     * just before the call. When an implementation knows it can't work out the position at the call, it fails the
     * {@code Mono} rather than answer with a later position.
     * <p>
     * An implementation that answers with a later position, for example by returning {@link #globalCheckpoint()} where
     * that works out where the feed is only when it runs, makes {@code ReactorDurableSubscriptionModel} skip the events
     * written after its {@code subscribe(..)} returned and before that position. A model that wraps another one passes
     * the call on to the model it wraps.
     * <p>
     * An implementation should not block when the returned {@code Mono} is subscribed to. For some start positions, the
     * subscription-model default for one, {@code ReactorDurableSubscriptionModel} subscribes to it on the thread that
     * calls its {@code subscribe(..)}. A {@code Mono} that blocks when subscribed to then blocks that call, also on a
     * thread that must not block, such as a Netty event loop thread.
     *
     * @return A {@link Mono} that emits a position no later than where the feed was at the call, within the limits the
     * implementation documents, fails when the implementation knows it can't work out that position, or completes
     * empty for a problem it can't resolve, as {@link #globalCheckpoint()} does.
     */
    Mono<Checkpoint> globalCheckpointAsOfNow();

    /**
     * Whether a subscription started from {@code checkpoint} would get every event written after it. It would not when
     * the feed no longer has the history back to {@code checkpoint}, for example when it is a MongoDB change-stream
     * position older than the oldest entry left in the oplog. A catch-up subscription asks this before it goes live from
     * a position it stored before a restart, and replays history again instead when the answer is {@code false}.
     * <p>
     * The default answers {@code true}, which is right for a model whose feed never drops history. A model that wraps
     * another one passes the call on to the model it wraps.
     *
     * @param checkpoint A position this model, or the model it wraps, returned from {@link #globalCheckpoint()} or
     *                   attached to an event it delivered
     * @return A {@link Mono} that emits {@code false} if the feed no longer has the history back to {@code checkpoint},
     * otherwise {@code true}, or fails if the model could not find out
     */
    default Mono<Boolean> canResumeFrom(Checkpoint checkpoint) {
        return Mono.just(true);
    }
}
