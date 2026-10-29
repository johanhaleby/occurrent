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

package org.occurrent.subscription.internal;

import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.subscription.Checkpoint;

/**
 * The wording a blocking and a reactor catch-up share when the subscription model no longer has the history to resume
 * a subscription, kept in one place so the two stacks cannot drift.
 */
public final class HistoryLossMessages {

    private HistoryLossMessages() {
    }

    /**
     * The error a catch-up logs once when it pauses a subscription with an id for good after the subscription model
     * lost its history. The arguments are the subscription id, the lost position and the lost position again.
     */
    public static final String TERMINAL_FAILURE_LOG = "The subscription model no longer has the history to resume catch-up subscription {} from {}, so the subscription is paused and delivers nothing more."
            + " Resuming it throws CatchupCannotResumeException until it is cancelled. To get every event, cancel it and replay from the beginning, or subscribe again from a checkpoint the application kept."
            + " Cancelling and subscribing again from now skips the events since {}.";

    /**
     * The error a reactor catch-up logs when it fails an unnamed {@code Flux} after the subscription model lost its
     * history. The arguments are the lost position and the lost position again.
     */
    public static final String UNNAMED_TERMINAL_FAILURE_LOG = "The subscription model no longer has the history to resume an unnamed catch-up subscription from {}, so its Flux fails with CatchupCannotResumeException."
            + " To get every event, subscribe again from the beginning or from a checkpoint the application kept. Subscribing again from now skips the events since {}.";

    /**
     * Refuses to go live from {@code checkpoint}, a position the subscription model no longer has the history back to,
     * before anything is read. {@code subscriptionId} only names the subscription, and may be null.
     */
    public static CatchupCannotResumeException cannotStartFrom(@Nullable String subscriptionId, Checkpoint checkpoint) {
        return new CatchupCannotResumeException("Cannot start catch-up subscription " + name(subscriptionId) + " from checkpoint \"" + checkpoint.asString()
                + "\", because the subscription model no longer has the history back to it, and starting from anywhere later would skip the events in between."
                + " Start the subscription from the beginning, or from an earlier checkpoint the application kept.");
    }

    /**
     * Refuses to resume a subscription that was paused for good after the subscription model lost its history from
     * {@code lostFrom}. {@code subscriptionId} only names the subscription, and may be null.
     */
    public static CatchupCannotResumeException historyLost(@Nullable String subscriptionId, Checkpoint lostFrom) {
        return new CatchupCannotResumeException("Cannot resume catch-up subscription " + name(subscriptionId) + ", because the subscription model no longer has the history back to \"" + lostFrom.asString()
                + "\", where it stopped, and resuming from anywhere later would skip the events in between. Cancel it and replay from the beginning, or subscribe again from a checkpoint the application kept.");
    }

    private static String name(@Nullable String subscriptionId) {
        return subscriptionId == null ? "(unnamed)" : subscriptionId;
    }
}
