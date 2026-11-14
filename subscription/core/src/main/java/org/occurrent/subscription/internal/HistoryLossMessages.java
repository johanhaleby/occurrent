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
     * The error a catch-up logs once when it pauses a subscription with an id for good after the subscription model lost
     * the history from the live start its replay would go live from. The events a subscribe from now skips can start before
     * that live start, also when the replay delivered none. The arguments are the
     * subscription id, the live start and the live start again.
     */
    public static final String LIVE_START_LOST_LOG = "The subscription model no longer has the history to resume catch-up subscription {} from {}, the live start its replay would go live from, so the subscription is paused and delivers nothing more."
            + " Resuming it throws CatchupCannotResumeException until it is cancelled. To get every event, cancel it and replay from the beginning, or subscribe again from a checkpoint the application kept."
            + " Cancelling and subscribing again from now skips the events its replay hadn't delivered, which can start before {}.";

    /**
     * The error to log for a loss {@link #TERMINAL_FAILURE_LOG} or {@link #LIVE_START_LOST_LOG} describes, the second
     * when {@code liveStart} says the subscription model lost the live start a replay would go live from.
     */
    public static String terminalFailureLog(boolean liveStart) {
        return liveStart ? LIVE_START_LOST_LOG : TERMINAL_FAILURE_LOG;
    }

    /**
     * What a blocking catch-up logs when a resume at a given position clears a loss it recorded, which can come after
     * the error that loss logged. The arguments are the subscription id, the position resumed at and the lost position.
     */
    public static final String RESUMED_AFTER_LOSS_LOG = "Catch-up subscription {} was resumed at {}, so an error logged earlier for this subscription no longer applies to {}, the position it lost."
            + " A resume that opens it where the history is gone logs the loss again.";

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
        return new HistoryLost(checkpoint, true, "Cannot start catch-up subscription " + name(subscriptionId) + " from checkpoint \"" + checkpoint.asString()
                + "\", because the subscription model no longer has the history back to it, and starting from anywhere later would skip the events in between."
                + " Start the subscription from the beginning, or from an earlier checkpoint the application kept.");
    }

    /**
     * Refuses to resume a subscription that was paused for good after the subscription model lost its history from
     * {@code lostFrom}. {@code subscriptionId} only names the subscription, and may be null.
     */
    public static CatchupCannotResumeException historyLost(@Nullable String subscriptionId, Checkpoint lostFrom) {
        return new HistoryLost(lostFrom, true, "Cannot resume catch-up subscription " + name(subscriptionId) + ", because the subscription model no longer has the history back to \"" + lostFrom.asString()
                + "\", where it stopped, and resuming from anywhere later would skip the events in between. Cancel it and replay from the beginning, or subscribe again from a checkpoint the application kept.");
    }

    /**
     * Refuses to resume a subscription that was paused for good after the subscription model lost the history from
     * {@code liveStart}, the live start its replay would go live from. The events a resume from later would skip can start
     * before it, so the message doesn't name it as where the subscription stopped. {@code subscriptionId} only names the
     * subscription, and may be null.
     */
    public static CatchupCannotResumeException liveStartLostOnResume(@Nullable String subscriptionId, Checkpoint liveStart) {
        return new HistoryLost(liveStart, true, "Cannot resume catch-up subscription " + name(subscriptionId) + ", because the subscription model no longer has the history back to \"" + liveStart.asString()
                + "\", the live start its replay would go live from, and resuming from anywhere later would skip the events its replay hadn't delivered, which can start before that live start."
                + " Cancel it and replay from the beginning, or subscribe again from a checkpoint the application kept.");
    }

    /**
     * The refusal {@link #historyLost} or {@link #liveStartLostOnResume} gives, the second when {@code liveStart} says
     * the subscription model lost the live start a replay would go live from.
     */
    public static CatchupCannotResumeException resumeRefused(@Nullable String subscriptionId, Checkpoint lostFrom, boolean liveStart) {
        return liveStart ? liveStartLostOnResume(subscriptionId, lostFrom) : historyLost(subscriptionId, lostFrom);
    }

    /**
     * Refuses to hand a catch-up over to live delivery with {@code message}, because the subscription model no longer
     * has the history from {@code lostLiveStart}, the live start the replay read. {@code replaysFromTheBeginning} says
     * whether the replay started at position 0 or the beginning of time, so running it again skips nothing.
     */
    public static CatchupCannotResumeException liveStartLost(String message, Checkpoint lostLiveStart, boolean replaysFromTheBeginning) {
        return new HistoryLost(lostLiveStart, !replaysFromTheBeginning, message);
    }

    /**
     * Whether {@code failure} is a refusal from {@link #cannotStartFrom}, {@link #historyLost} or {@link #liveStartLost},
     * so the position the subscription would go on from is one the subscription model no longer has the history back
     * to. Any other {@link CatchupCannotResumeException} answers {@code false}.
     */
    public static boolean isHistoryLost(Throwable failure) {
        return failure instanceof HistoryLost;
    }

    /**
     * The position {@code failure} says the subscription model lost the history from, when {@link #isHistoryLost} is
     * true for a refusal whose resume would skip events, and {@code null} otherwise. A loss by a replay from the
     * beginning answers {@code null}, since running that replay again skips nothing.
     */
    public static @Nullable Checkpoint lostFromWhereAResumeSkipsEvents(Throwable failure) {
        return failure instanceof HistoryLost lost && lost.resumeSkipsEvents ? lost.lostFrom : null;
    }

    private static final class HistoryLost extends CatchupCannotResumeException {
        private final Checkpoint lostFrom;
        private final boolean resumeSkipsEvents;

        private HistoryLost(Checkpoint lostFrom, boolean resumeSkipsEvents, String message) {
            super(message);
            this.lostFrom = lostFrom;
            this.resumeSkipsEvents = resumeSkipsEvents;
        }
    }

    private static String name(@Nullable String subscriptionId) {
        return subscriptionId == null ? "(unnamed)" : subscriptionId;
    }
}
