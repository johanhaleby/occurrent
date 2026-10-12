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

import org.jspecify.annotations.NullMarked;

import static java.util.Objects.requireNonNull;

/**
 * What a subscription model does with a subscription whose history was lost, as a history loss listener answers it.
 * A subscription's history is lost when the position it opens at is no longer in what the model reads from, such as
 * a MongoDB change stream position the oplog no longer reaches back to.
 * <p>
 * {@link #leaveToSubscriptionModel()} has the model handle the subscription as it is configured to.
 * A MongoDB subscription model then restarts the subscription from the present, or stops delivering to it.
 * {@link #takeOver(Runnable)} has the model pause the subscription instead, and then run a task that brings it back,
 * such as a catch-up subscription replaying the history the model no longer has before it resumes the subscription.
 */
@NullMarked
public sealed interface HistoryLossHandling {

    /**
     * Has the subscription model handle the subscription as if no listener had been asked.
     */
    static HistoryLossHandling leaveToSubscriptionModel() {
        return new LeaveToSubscriptionModel();
    }

    /**
     * Has the subscription model pause the subscription, keep it, and then run {@code afterPause}. The model asks no
     * other listener about this loss. When a pause, a resume or a cancel of the subscription, or a shutdown of the
     * model, came before the model paused it, the model drops the takeover and doesn't run {@code afterPause}.
     *
     * @param afterPause Runs once the subscription is paused, on the subscription model's thread that found the
     *                   history lost, so start any long work on a thread of your own. When it throws, the model logs
     *                   it and the subscription stays paused.
     */
    static HistoryLossHandling takeOver(Runnable afterPause) {
        return new TakeOver(afterPause);
    }

    /**
     * See {@link HistoryLossHandling#leaveToSubscriptionModel()}.
     */
    record LeaveToSubscriptionModel() implements HistoryLossHandling {
    }

    /**
     * See {@link HistoryLossHandling#takeOver(Runnable)}.
     *
     * @param afterPause Runs once the subscription is paused
     */
    record TakeOver(Runnable afterPause) implements HistoryLossHandling {
        public TakeOver {
            requireNonNull(afterPause, "afterPause cannot be null");
        }
    }
}
