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

package org.occurrent.subscription;

/**
 * Something that can report the current global checkpoint, independent of which subscription stack it is attached
 * to. The global checkpoint might be e.g. the wall clock time of the server, a vector clock, the number of events
 * consumed etc. This is useful to get the initial position of a subscription before any message has been consumed
 * by the subscription (and thus no {@link Checkpoint} has been persisted for the subscription). The reason for
 * doing this would be to make sure that a subscription doesn't lose the very first message if there's an error
 * consuming the first event.
 * <p>
 * {@code T} is the shape the checkpoint arrives in: a plain, possibly-{@code null} value for a blocking stack, or a
 * reactive publisher for a non-blocking one. A caller that only needs this one capability, such as
 * {@code ManualStartSubscriptionModel.stoppedByDefault}, can depend on {@code GlobalCheckpointSource<T>} instead of
 * pulling in the full subscription model that happens to expose it.
 *
 * @param <T> the shape the checkpoint is returned in
 */
public interface GlobalCheckpointSource<T> {

    /**
     * @return The global checkpoint for the database, in whatever shape {@code T} names.
     */
    T globalCheckpoint();
}
