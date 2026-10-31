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

package org.occurrent.cloudevents;

import io.cloudevents.CloudEvent;

/**
 * A {@link CloudEvent} that also holds the checkpoint its subscription delivered it with.
 * {@link EventMetadata#from(CloudEvent)} reads it into {@link EventMetadata#getCheckpoint()}.
 */
public interface CheckpointCarrier {

    /**
     * The checkpoint the subscription delivered this event with, as a string to restart the subscription from.
     * <p>
     * Store it with the application's state, in the same transaction. Restart with
     * {@code StartAt.checkpoint(new StringBasedCheckpoint(stored))}, or with {@code DcbStartAt.checkpoint(stored)}
     * for a DCB subscription.
     * <p>
     * The checkpoint of a replayed event means something only to a catch-up subscription. A MongoDB subscription model
     * with no catch-up subscription wrapping it refuses that checkpoint when a subscription starts from a
     * {@code StartAt} that names it, or from a dynamic {@code StartAt} whose function returns it before the model counts
     * a change stream of the subscription as opened. The reactor model counts a {@code Flux} that was subscribed to when
     * the subscription has no id or can't read through the driver's cursor, so a position the server rejects ends that
     * check.
     * <p>
     * The checkpoint of a live event holds a change stream resume token. Once the oplog no longer reaches back to it,
     * a catch-up subscription doesn't restart a subscription from the present. On the blocking stack a subscribe from
     * it throws {@code CatchupCannotResumeException}. On the reactor stack a subscription with an id is paused, and
     * resuming it throws that exception, and the {@code Flux} of an unnamed one fails with it. A MongoDB subscription
     * model with no catch-up subscription wrapping it stops the subscription with an error in the log, or restarts it
     * from the present when it is set to restart after lost history, which skips the events in between.
     *
     * @return The checkpoint as a string.
     */
    String checkpoint();
}
