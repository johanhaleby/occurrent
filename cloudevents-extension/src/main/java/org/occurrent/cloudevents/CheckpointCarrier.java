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
     * The string means something only to a catch-up subscription. A MongoDB subscription model given the checkpoint
     * of a replayed event, with no catch-up subscription wrapping it, opens at its default with no error.
     * <p>
     * The checkpoint of a live event holds a change stream resume token. Once the oplog no longer reaches back to it,
     * the catch-up does not take a subscription restarted from it over, and the subscription model it wraps decides.
     * A blocking MongoDB subscription model then stops the subscription and logs an error, or restarts it from the
     * present when it is set to restart after lost history, which skips the events in between.
     *
     * @return The checkpoint as a string.
     */
    String checkpoint();
}
