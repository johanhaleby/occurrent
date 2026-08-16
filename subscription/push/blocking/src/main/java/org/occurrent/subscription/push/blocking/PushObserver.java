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
 * Called once per event, whether or not a handler ends up running. {@code matched} is {@code true} only when the
 * model is running and a currently registered, unpaused subscription's filter accepted the event, independent of
 * whether that handler goes on to succeed or throw. It shares the same filter evaluation the actual dispatch
 * decision is made from, so the two can never disagree.
 * <p>
 * A filter that throws while being evaluated (a supplied {@code DataFieldReader} can) never gets to answer whether
 * it matched. A {@link RuntimeException} or {@link AssertionError} is reported to the observer as {@code false}
 * instead, standing in for the answer that never came, before that exception propagates. Any other {@link Error}
 * skips the observer entirely and propagates straight out. If the observer itself then throws while being told
 * about the filter's failure, that throw is attached to the filter's exception through
 * {@link Throwable#addSuppressed(Throwable)} rather than propagating on its own, so a filter failure is never
 * replaced by a failure in reporting it.
 * <p>
 * Called with the real {@code matched} instead, the observer can throw too. A {@link RuntimeException} or
 * {@link AssertionError} it throws here is caught and logged rather than propagated, so a broken observer cannot
 * turn an event that was actually delivered into a broker redelivery. Any other {@link Error} it throws here still
 * propagates on its own, once the observer has already run.
 * <p>
 * The default, {@link #noop()}, changes nothing for existing code, and {@link PushSubscriptionModel} skips both this
 * call and the match check entirely when no other observer is configured.
 */
@NullMarked
@FunctionalInterface
public interface PushObserver {

    /**
     * @param cloudEvent The event {@code accept(...)} was asked to deliver.
     * @param matched    Whether the model is running and a currently registered, unpaused subscription's filter
     *                   accepted the event.
     */
    void observe(CloudEvent cloudEvent, boolean matched);

    /**
     * An observer that does nothing, the default every {@link PushSubscriptionModel} constructor uses when none is
     * given. Always the same instance, which is what lets {@link PushSubscriptionModel} tell "nobody is observing"
     * from "an observer that happens to do nothing" and skip the match check for the former.
     */
    static PushObserver noop() {
        return PushObserverNoop.INSTANCE;
    }
}
