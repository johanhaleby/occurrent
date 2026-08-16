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

package org.occurrent.subscription.push.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;

/**
 * Told about every event {@link PushSubscriptionModel#accept(CloudEvent)} is asked to deliver, before delivery is
 * attempted, so a misconfigured queue binding, a missing declared event type or a type-mapping typo can be told
 * apart from a saga or projection that received an event and chose not to act on it. {@code accept(...)} itself
 * stays silent about all of these by design, see ADR 104.
 * <p>
 * Called once per event, whether or not a handler ends up running: {@code matched} is {@code true} when a currently
 * registered, unpaused subscription's filter accepted the event, independent of whether that handler goes on to
 * succeed or throw. An observer that throws is caught and logged, never propagated, so a broken observer cannot turn
 * an event that was actually delivered into a broker redelivery.
 * <p>
 * The default, {@link #noop()}, changes nothing for existing code.
 */
@NullMarked
@FunctionalInterface
public interface PushObserver {

    /**
     * @param cloudEvent The event {@code accept(...)} was asked to deliver.
     * @param matched    Whether a currently registered, unpaused subscription's filter accepted the event.
     */
    void observe(CloudEvent cloudEvent, boolean matched);

    /**
     * An observer that does nothing, the default every {@link PushSubscriptionModel} constructor uses when none is
     * given.
     */
    static PushObserver noop() {
        return (cloudEvent, matched) -> {
        };
    }
}
