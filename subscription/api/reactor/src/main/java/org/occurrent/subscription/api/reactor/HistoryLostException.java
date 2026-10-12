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

package org.occurrent.subscription.api.reactor;

import org.jspecify.annotations.NullMarked;
import org.occurrent.subscription.Checkpoint;

import static java.util.Objects.requireNonNull;

/**
 * Ends the {@code Flux} of
 * {@link HistoryLossReportingSubscriptions#subscribeFailingIfStartLost(org.occurrent.subscription.SubscriptionFilter, org.occurrent.subscription.StartAt)}
 * when the history the subscription model reads from no longer reaches back to the position the {@code Flux} started
 * at.
 */
@NullMarked
public class HistoryLostException extends RuntimeException {
    private final Checkpoint lostFrom;

    /**
     * @param lostFrom The position the {@code Flux} started at, which the history no longer reaches back to.
     * @param cause    What the subscription model got when it opened at {@code lostFrom}.
     */
    public HistoryLostException(Checkpoint lostFrom, Throwable cause) {
        super("The history the subscription model reads from no longer reaches back to " + requireNonNull(lostFrom, "lostFrom cannot be null").asString(), cause);
        this.lostFrom = lostFrom;
    }

    /**
     * @return The position the {@code Flux} started at, which the history no longer reaches back to.
     */
    public Checkpoint lostFrom() {
        return lostFrom;
    }
}
