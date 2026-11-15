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

import org.occurrent.subscription.CatchupMarker;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;

/**
 * A {@link CheckpointStorage} that can also store the catch-up marker of a subscription id, the checkpoint that says
 * a catch-up has replayed the whole history for that id so the next catch-up can skip the replay.
 * <p>
 * The marker is stored at the subscription id, where {@link #read(String)} and {@link #exists(String)} find it. Every
 * subscription id also has a generation, which starts at {@code 0} and changes each time
 * {@link #forgetCatchupMarker(String)} runs. A marker is saved with the generation its catch-up read, and
 * {@link #readCatchupMarker(String)} reports it as present only while that generation is still the current one. A
 * save that reaches the store after a forget, for example one the client gave up on after a timeout while the
 * database still ran it, therefore writes a marker that reads as absent, and the next catch-up replays the history.
 * <p>
 * Plain {@link #read(String)} and {@link #exists(String)} don't look at the generation, so they still find such a
 * marker. Use {@link #readCatchupMarker(String)} to decide whether a catch-up can skip the replay.
 */
public interface CatchupMarkerStorage extends CheckpointStorage {

    /**
     * Read the catch-up marker and the generation of {@code subscriptionId}, as the store held them at one instant.
     *
     * @param subscriptionId The id of the subscription whose marker to read
     * @return The marker, absent when none is stored or the stored one was saved with a generation that a later
     * {@link #forgetCatchupMarker(String)} replaced, and the current generation either way
     */
    CatchupMarker readCatchupMarker(String subscriptionId);

    /**
     * Save {@code marker} as the catch-up marker of {@code subscriptionId}, with {@code generation} stored next to it,
     * if {@code condition} is fulfilled.
     * <p>
     * Pass the {@link CatchupMarker#generation()} that {@link #readCatchupMarker(String)} returned at the start of the
     * same catch-up. A generation from anywhere else can make a forgotten marker read as present again, and a store
     * can't check it for you.
     * <p>
     * A save whose generation a {@link #forgetCatchupMarker(String)} has already replaced still writes the marker and
     * returns {@code true}, and {@link #readCatchupMarker(String)} then reports the marker as absent. A store failure
     * throws, and never returns {@code false}.
     *
     * @param subscriptionId The id of the subscription whose marker to save
     * @param marker         The marker to save
     * @param generation     The generation {@link #readCatchupMarker(String)} returned at the start of the catch-up
     * @param condition      What must be true of the stored write version for the save to be allowed, as for
     *                       {@link #save(String, Checkpoint, CheckpointWriteCondition)}
     * @return {@code false} if {@code condition} refused the save, {@code true} otherwise
     */
    boolean saveCatchupMarker(String subscriptionId, Checkpoint marker, long generation, CheckpointWriteCondition condition);

    /**
     * Forget the catch-up marker of {@code subscriptionId}, so that the next catch-up for it replays the history.
     * <p>
     * Changes the generation of {@code subscriptionId} in one write and deletes the checkpoint stored at it, as
     * {@link #delete(String)} does. Every marker saved with an earlier generation reads as absent from then on,
     * including one whose save reaches the store after this call returns.
     *
     * @param subscriptionId The id of the subscription whose marker to forget
     */
    void forgetCatchupMarker(String subscriptionId);
}
