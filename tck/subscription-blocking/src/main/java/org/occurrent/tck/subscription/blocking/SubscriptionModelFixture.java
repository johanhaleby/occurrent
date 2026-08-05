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
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

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
     * Which of the four ways of saying where a subscription starts this model accepts.
     * <p>
     * Every accepted variant owes a subscription that receives what is published after it, and every variant left out
     * owes an {@link IllegalArgumentException} from {@code subscribe}. Both halves are asserted, so leaving a variant
     * out is a claim the model has to live up to rather than a way of not being asked about it.
     * <p>
     * Accepting a variant is not the same as acting on it. A model that dispatches events as they arrive has no
     * position to start from and its own javadoc says it ignores the one it is given, which is still an accepted
     * variant here: what the suite holds it to is that the subscription works, not that the position moved anything.
     * A model that cannot honour a position and refuses it instead owes the refusal, which is what
     * {@code CatchupThenPushSubscriptionModel} does with every variant but
     * {@link StartAtVariant#SUBSCRIPTION_MODEL_DEFAULT}, since it replays a whole history and has nothing to apply a
     * caller's position to.
     * <p>
     * The default is all four, because that is what every Occurrent model but that one does.
     */
    default Set<StartAtVariant> acceptedStartAtVariants() {
        return EnumSet.allOf(StartAtVariant.class);
    }

    /**
     * A checkpoint this model can be started from, used to build the {@link StartAtVariant#CHECKPOINT} start position.
     * <p>
     * Hand back a position at or before the present, since the suite starts a subscription there and then expects
     * everything published afterwards to arrive. A model reading a change stream should answer with what it reports
     * from {@code globalCheckpoint()}, which is the position a catch-up handover would use. A model that ignores the
     * start position may answer with anything, and {@code GlobalCheckpoint.of(0)} is the obvious nothing.
     * <p>
     * Asked of the fixture rather than of the model because {@code SubscriptionModel} has no member that reports one:
     * only the checkpoint-aware models do, and this suite runs against the rest as well.
     */
    Checkpoint aCheckpointToStartFrom();

    /**
     * The answer for a model that reports a position but is allowed to answer null, which most of them are: hand back
     * what it reported, or the global position zero when it reported nothing.
     * <p>
     * A convenience rather than a default on {@link #aCheckpointToStartFrom()}, which stays abstract on purpose. A
     * default would let a model that really does read a position inherit "position zero" by forgetting to override,
     * and a model reading a change stream treats a position it does not recognise as "start where you would have
     * started anyway", so the suite would pass while testing nothing.
     */
    static Checkpoint orGlobalPositionZero(@Nullable Checkpoint reported) {
        return reported == null ? GlobalCheckpoint.of(0) : reported;
    }

    /**
     * Whether a subscription id this model has not seen before is replayed the whole history first, or starts where it
     * was told to start.
     * <p>
     * Both answers are asserted. Answering {@code false}, which every change-stream and in-process model does, owes a
     * new subscription that does <em>not</em> receive what was published before it existed. Answering {@code true} owes
     * exactly the opposite, and it is a real contract rather than an accident of implementation: a model whose whole
     * job is to bring a read model up to date replays first and goes live after, which is why it also refuses a
     * caller-supplied start position in {@link #acceptedStartAtVariants()}.
     * <p>
     * Declared rather than asked because nothing on {@code SubscriptionModel} reports it, and finding out by
     * subscribing would already have delivered whatever the answer is.
     */
    default boolean replaysHistoryToANewSubscription() {
        return false;
    }

    /**
     * Releases whatever the fixture opened, and shuts the model down. Called after every test method, including a
     * failing one.
     */
    default void close() {
    }
}
