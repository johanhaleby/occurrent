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

package org.occurrent.subscription.api.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.occurrent.retry.AttemptNotMade;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.GlobalCheckpointSource;

import java.util.function.BooleanSupplier;

/**
 * A {@link SubscriptionModel} that produces {@link CheckpointAwareCloudEvent} compatible {@link CloudEvent}'s.
 * This is useful for subscribers that want to persist the checkpoint for a given subscription if the event store doesn't
 * maintain the position for subscriptions.
 * <p>
 * A catch-up model that wraps this model can make a subscription's action wait on its delivery thread. The wait lasts
 * until the action of a replay that a cancel or a stop ended for the same subscription id has returned. An
 * implementation that calls the actions of several subscriptions on one thread therefore lets one subscription hold
 * back the others.
 * <p>
 * The action a catch-up model gives this model throws {@link AttemptNotMade} without handling the event once a cancel
 * of the subscription has forgotten it. While the catch-up model shuts down, an action that waits as above throws it a
 * second after the shutdown began. An implementation should not restart a run whose action threw once its shutdown has
 * begun. The throw can reach a run it still has open when its shutdown begins more than a second after the catch-up
 * model's. An implementation must not store its position past an event whose action threw it.
 */
public interface CheckpointAwareSubscriptionModel extends SubscriptionModel, GlobalCheckpointSource<@Nullable Checkpoint> {

    /**
     * The global checkpoint might be e.g. the wall clock time of the server, vector clock, number of events consumed etc.
     * This is useful to get the initial position of a subscription before any message has been consumed by the subscription
     * (and thus no {@link Checkpoint} has been persisted for the subscription). The reason for doing this would be
     * to make sure that a subscription doesn't lose the very first message if there's an error consuming the first event.
     *
     * @return The global checkpoint for the database or {@code null} if there's an unresolvable problem
     */
    @Override
    @Nullable Checkpoint globalCheckpoint();

    /**
     * Whether a subscription started from {@code checkpoint} would get every event written after it. It would not when
     * the feed no longer has the history back to {@code checkpoint}, for example when it is a MongoDB change-stream
     * position older than the oldest entry left in the oplog. A position catch-up subscription asks this when it resumes
     * from a position it stored before a restart, and again after every replay, before it goes live from the position it
     * read before that replay. It replays history again instead when the answer is {@code false}, fails when the answer
     * is {@code false} after 4 replays in a row, and fails when this throws.
     * <p>
     * The default answers {@code true}, which is right for a model whose feed never drops history. A model that wraps
     * another one passes the call on to the model it wraps.
     *
     * @param checkpoint A position this model, or the model it wraps, returned from {@link #globalCheckpoint()} or
     *                   attached to an event it delivered
     * @return {@code false} if the feed no longer has the history back to {@code checkpoint}, otherwise {@code true}
     */
    default boolean canResumeFrom(Checkpoint checkpoint) {
        return true;
    }

    /**
     * {@link #globalCheckpoint()} for a caller that may stop wanting the answer while it waits for it. A catch-up
     * subscription calls this on its replay thread, and {@code stillWanted} turns {@code false} once the subscription is
     * cancelled, the catch-up model is stopped or shut down, or another replay took the subscription over. A model that
     * retries a failure should check {@code stillWanted} before each new attempt. Once it is {@code false}, the model
     * must throw the last failure instead of trying again, and not answer {@code null}, which a catch-up reads as a
     * model that has no global checkpoint.
     * <p>
     * The default calls {@link #globalCheckpoint()} and never reads {@code stillWanted}. A model that wraps another one
     * passes the call on to the model it wraps, with the same {@code stillWanted}.
     *
     * @param stillWanted {@code false} once the caller no longer wants the answer
     * @return The global checkpoint for the database or {@code null} if there's an unresolvable problem
     */
    default @Nullable Checkpoint globalCheckpoint(BooleanSupplier stillWanted) {
        return globalCheckpoint();
    }

    /**
     * {@link #canResumeFrom(Checkpoint)} for a caller that may stop wanting the answer while it waits for it. A catch-up
     * subscription calls this on its replay thread, and {@code stillWanted} turns {@code false} once the subscription is
     * cancelled, the catch-up model is stopped or shut down, or another replay took the subscription over. A model that
     * retries a failure should check {@code stillWanted} before each new attempt. Once it is {@code false}, the model
     * must throw the last failure instead of trying again, and not answer {@code false}, which a catch-up reads as a
     * feed that no longer has the history back to {@code checkpoint}. The MongoDB subscription models do that.
     * <p>
     * The default calls {@link #canResumeFrom(Checkpoint)} and never reads {@code stillWanted}. A model that wraps
     * another one passes the call on to the model it wraps, with the same {@code stillWanted}.
     *
     * @param checkpoint  A position this model, or the model it wraps, returned from {@link #globalCheckpoint()} or
     *                    attached to an event it delivered
     * @param stillWanted {@code false} once the caller no longer wants the answer
     * @return {@code false} if the feed no longer has the history back to {@code checkpoint}, otherwise {@code true}
     */
    default boolean canResumeFrom(Checkpoint checkpoint, BooleanSupplier stillWanted) {
        return canResumeFrom(checkpoint);
    }
}
