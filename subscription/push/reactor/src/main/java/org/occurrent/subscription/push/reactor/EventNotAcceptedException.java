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

package org.occurrent.subscription.push.reactor;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.occurrent.subscription.RoutingOutcome;

import java.util.Objects;

/**
 * {@link PushSubscriptionModel#acceptRedeliverable(CloudEvent)} did not apply the event, and nothing is wrong with the
 * event itself. Either no subscription could receive it ({@link RoutingOutcome#UNAVAILABLE}), or a
 * {@link CatchupThenPushSubscriptionModel} in front has not gone live yet ({@link RoutingOutcome#DEFERRED}).
 * <p>
 * Leave the message unacknowledged so the broker delivers it again, and do not send it through a failure policy such
 * as parking it. A stopped model and a paused subscription have to be started or resumed before a later delivery can
 * succeed.
 * <p>
 * Extends {@link IllegalStateException}, so code that already treats an {@link IllegalStateException} from the push
 * model as "do not acknowledge" keeps doing so.
 */
@NullMarked
public final class EventNotAcceptedException extends IllegalStateException {

    private final RoutingOutcome outcome;

    EventNotAcceptedException(CloudEvent cloudEvent, RoutingOutcome outcome) {
        super("Event with id '" + cloudEvent.getId() + "' was not applied (" + outcome + "). Leave it unacknowledged so it is delivered again.");
        this.outcome = Objects.requireNonNull(outcome, "outcome cannot be null");
    }

    /**
     * @return {@link RoutingOutcome#UNAVAILABLE} when no subscription could receive the event, or
     * {@link RoutingOutcome#DEFERRED} when a catch-up in front has not gone live yet.
     */
    public RoutingOutcome outcome() {
        return outcome;
    }
}
