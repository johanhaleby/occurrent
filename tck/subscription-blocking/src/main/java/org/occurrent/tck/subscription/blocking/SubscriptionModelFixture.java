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

package org.occurrent.tck.subscription.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import java.util.List;

/**
 * What a {@link SubscriptionModel} implementation hands the conformance suites.
 * <p>
 * A fixture is created fresh for every test method, and the model it hands back <strong>must have no subscriptions</strong>.
 * <p>
 * Two rules that are easy to get wrong, both learned from what MongoDB does:
 * <ul>
 *     <li><strong>{@link #close()} must shut the model down, and cleanup must not drop a collection or a database while
 *     a subscription is open.</strong> Dropping either kills a live change stream, so a fixture that cleans up that way
 *     leaves the next test watching a stream that will never deliver. Delete documents instead.</li>
 *     <li><strong>Never let the suite publish the same event id twice.</strong> The suites never do, and a fixture must
 *     not make one up either: a store-backed model refuses a duplicate id through a unique index while an in-process
 *     model delivers it twice.</li>
 * </ul>
 */
@NullMarked
public interface SubscriptionModelFixture {

    /**
     * The model under test, with no subscriptions on it.
     */
    SubscriptionModel subscriptionModel();

    /**
     * Hands the events to whatever feeds this model, in order.
     * <p>
     * What that is differs completely between implementations, which is exactly why the suite is not allowed to know:
     * a store-backed model needs a write its change stream observes, and an in-process model is handed the events
     * directly. The suite only ever asks for them to arrive.
     * <p>
     * <strong>This is allowed to throw.</strong> On a model that propagates a handler exception rather than retrying,
     * delivery happens inside this call, so a throwing handler comes back out of here. Which of the two to expect is
     * {@link #retriesAFailingHandler()}.
     *
     * @param events The events to feed in, in order. Never empty, and no id is ever repeated within a test.
     */
    void publish(List<CloudEvent> events);

    /**
     * Whether a paused subscription's events are held for it, or dropped.
     * <p>
     * Both answers are correct and both are asserted. A model with a queue in front of each handler can hold an event
     * until the subscription resumes, while a model that dispatches as events arrive has nothing to hold them in, so
     * an event published while a subscription is paused never reaches that handler at all. Occurrent's register-only
     * models are the second kind, which their own javadoc words as <i>dropped, not deferred</i>.
     * <p>
     * This is a declaration rather than a question put to the model because nothing on {@code SubscriptionModel}
     * reports it, the same line {@code CheckpointStorageFixture.preservesCheckpointType} sits on.
     */
    boolean deliversEventsPublishedWhilePaused();

    /**
     * Whether a handler that throws is called again, or the exception reaches whoever published the event.
     * <p>
     * Both answers cost something to give. A retrying model owes a second call to the handler, and a propagating model
     * owes the exception out of {@link #publish(List)}. Occurrent's models split on this: the three that deliver
     * asynchronously wrap the handler in a {@code RetryStrategy}, and the two that deliver on the publishing thread
     * let the exception through.
     * <p>
     * The suites never install a handler that throws forever, because a retry here has no attempt cap by default and
     * the test would wait out its whole timeout rather than failing with a reason.
     */
    boolean retriesAFailingHandler();

    /**
     * Whether the model accepts more than one subscription at a time.
     * <p>
     * Answering {@code false} is a real design position rather than a limitation: a sink driven by an external broker
     * delivers one message under one acknowledgement, so a second consumer on it would mean one failing consumer
     * holding up the rest. A model answering {@code false} owes a refusal of the second {@code subscribe}, and one
     * answering {@code true} owes two subscriptions that receive independently.
     * <p>
     * Declared rather than asked because nothing reports it, and finding out by subscribing twice would leave the
     * model in whichever state the attempt produced.
     */
    default boolean acceptsSeveralSubscriptions() {
        return true;
    }

    /**
     * Releases whatever the fixture opened, and shuts the model down. Called after every test method, including a
     * failing one.
     */
    default void close() {
    }
}
