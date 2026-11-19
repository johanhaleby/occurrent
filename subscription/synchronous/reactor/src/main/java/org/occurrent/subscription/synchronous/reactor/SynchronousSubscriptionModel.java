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

package org.occurrent.subscription.synchronous.reactor;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.occurrent.application.service.WriteRefusedException;
import org.occurrent.application.service.reactor.ReactiveSynchronousEventDispatcher;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.RegisteringSubscribable;
import org.occurrent.subscription.api.reactor.internal.SynchronousHandlersTestSwitch;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Set;

/**
 * The reactive counterpart of the blocking {@code SynchronousSubscriptionModel}: a register-only reactive
 * {@link org.occurrent.subscription.api.reactor.Subscribable} whose handlers are composed <strong>synchronously</strong>
 * into the writer's reactive chain, before {@code execute} completes, rather than being driven asynchronously off a
 * change stream.
 * <p>
 * Driven by the reactive application service: after a successful write it hands the just-written cloud events to
 * {@link #dispatch(List, boolean)}, which routes each event to the registered handlers whose {@link SubscriptionFilter}
 * matches, invoking them in registration order and sequentially (the next handler does not start until the previous
 * one's {@link Mono} completes). A handler error reaches the caller, so a reactive transaction it passes through can
 * roll the write back. Whether it also stops the handlers behind it depends on that second argument.
 * <p>
 * The reactive application service asks {@link #beforeWrite()} before every write, which refuses the write while the
 * model is stopped or when a paused subscription matches its events, since a handler that misses an event never sees
 * it. Events fed in directly through {@link #dispatch(List)} skip a paused subscription instead, as ADR 85 describes.
 * <p>
 * The register-and-route machinery lives in {@link RegisteringSubscribable}. This model adds the application-service
 * dispatch entry point. For an externally driven push feed (RabbitMQ, Kafka, ...) use {@code PushSubscriptionModel}.
 */
@NullMarked
public class SynchronousSubscriptionModel extends RegisteringSubscribable implements ReactiveSynchronousEventDispatcher, SynchronousHandlersTestSwitch {

    private volatile boolean dropsEventsWhileStopped;

    /**
     * Several handlers, unlike the push models, which take one each. There is no broker and no acknowledgement to share
     * here. The events arrive from the write that just produced them, and a handler error reaches the writer.
     * {@link #dispatch(List, boolean)} told {@code true} stops at the failure, and the write usually rolls back. Told
     * {@code false} it offers the other handlers the event and reports the failures together, so a handler's error
     * doesn't strand the handlers behind it. See ADR 90 for the isolation argument that makes the push sinks
     * single-consumer, and its ADR 57 follow-up for the no-transaction case here.
     */
    public SynchronousSubscriptionModel() {
        super(Consumers.MANY);
    }

    /**
     * Creates a model that can answer a subscription filter on a {@code data} payload field by reading it through
     * {@code dataFieldReader}. Occurrent ships a Jackson-backed one in
     * {@code occurrent-common-inmemory-filter-matching-jackson}. Without one, such a filter is refused.
     */
    public SynchronousSubscriptionModel(DataFieldReader dataFieldReader) {
        super(Consumers.MANY, dataFieldReader);
    }

    /**
     * Dispatch the supplied cloud events to every matching registered handler, sequentially, stopping at the first
     * handler that errors.
     * <p>
     * For a dispatch that knows whether a transaction is in force, use {@link #dispatch(List, boolean)}, which is what
     * the reactive application service calls. This form is for driving the model directly, for example from a test.
     *
     * @param writtenCloudEvents The newly written cloud events.
     * @return A {@link Mono} that completes when dispatch is done.
     */
    public Mono<Void> dispatch(List<CloudEvent> writtenCloudEvents) {
        return routeWrittenEvents(() -> route(writtenCloudEvents));
    }

    /**
     * Dispatch as {@link #dispatch(List)} does, told whether the write can't commit once a handler fails.
     * <p>
     * With {@code transactional} true the first handler error stops dispatch, because the write usually rolls back and
     * the handlers behind it would only be doing work that is discarded, possibly with effects outside the datastore.
     * Otherwise nothing tells this model that the write can't commit, so the other handlers are still offered the
     * event and the failures are reported afterwards, though a handler that failed gets no more of that
     * batch and an {@link Error} still stops it. A handler skipped because a sibling errored would never see the event
     * again, since this model has no replay, which is why this argument exists. See the ADR 57 amendment.
     *
     * @param writtenCloudEvents The newly written cloud events.
     * @param transactional      Whether the write can't commit once a handler fails, for example because it runs in a
     *                           transaction that a failure rolls back.
     * @return A {@link Mono} that completes when dispatch is done.
     */
    @Override
    public Mono<Void> dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
        return routeWrittenEvents(() -> transactional ? route(writtenCloudEvents) : routeIsolated(writtenCloudEvents));
    }

    /**
     * Refuses a write while this model is stopped and has handlers registered, and returns a check that refuses one
     * whose events a paused subscription matches. A handler skipped either way would never see those events, since
     * this model has no replay. A {@link #stop()} or {@link #pauseSubscription(String)} that comes after this call is
     * held back until the check is closed, so the subscriptions it let the write through for still get its events.
     *
     * @throws WriteRefusedException when this model is stopped and has handlers registered
     */
    @Override
    public WriteCheck beforeWrite() {
        if (dropsEventsWhileStopped) {
            return WriteCheck.NONE;
        }
        Runnable closeWindow = openWriteWindow();
        if (closeWindow == null) {
            if (hasSubscriptions()) {
                throw WriteRefusedException.synchronousSubscriptionsStopped();
            }
            return WriteCheck.NONE;
        }
        return new WriteCheck() {
            @Override
            public void check(List<CloudEvent> newEvents) {
                Set<String> paused = pausedSubscriptionsMatching(newEvents);
                if (!paused.isEmpty()) {
                    throw WriteRefusedException.synchronousSubscriptionsPaused(paused);
                }
            }

            @Override
            public void close() {
                closeWindow.run();
            }
        };
    }

    /**
     * Internal, for test support only. See {@link SynchronousHandlersTestSwitch}.
     */
    @Override
    public void dropEventsWhileStopped() {
        dropsEventsWhileStopped = true;
    }
}
