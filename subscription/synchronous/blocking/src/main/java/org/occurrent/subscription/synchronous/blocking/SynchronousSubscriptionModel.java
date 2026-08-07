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

package org.occurrent.subscription.synchronous.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.occurrent.application.service.blocking.SynchronousEventDispatcher;
import org.occurrent.inmemory.filtermatching.DataFieldReader;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.RegisteringSubscribable;

import java.util.List;
import java.util.function.Consumer;

/**
 * A register-only subscription model whose handlers are invoked <strong>synchronously</strong>, in-process, on
 * the thread that supplies the events, rather than asynchronously off a change stream.
 * <p>
 * It exists to be driven by the application service: after a successful write, the application service hands
 * the just-written cloud events to {@link #dispatch(List, boolean)}, which routes each event to the registered
 * handlers whose {@link SubscriptionFilter} matches, invoking them in registration order on the calling thread. A
 * handler exception reaches the caller (so, under a transaction, it rolls the write back). Whether it also stops the
 * handlers behind it depends on the transaction, which is what that second argument carries.
 * {@link #accept(List)} routes the same way for callers driving the model directly, and always offers the event to
 * every handler rather than stopping at the first failure, because nothing rolls back on that path.
 * <p>
 * Unlike the asynchronous {@code SubscriptionModel}s, this model has no start position, checkpoint, catch-up, or
 * replay. It only ever reacts to events fed to it here and now. It is a full {@link org.occurrent.subscription.api.blocking.SubscriptionModel}, so
 * stopping it, or pausing a subscription, drops rather than defers events that arrive in the meantime (ADR 85).
 * Dispatch happens inside the write, so a stopped model here still lets the write succeed. Only the projection
 * behind it does not run, so stopping subscriptions and then accepting traffic leaves writes landing with no
 * projection. The register-and-route machinery lives in {@link RegisteringSubscribable}. This model adds the
 * application-service dispatch entry point. For an externally driven push feed (RabbitMQ, Kafka, ...) use
 * {@code PushSubscriptionModel} instead.
 */
@NullMarked
public class SynchronousSubscriptionModel extends RegisteringSubscribable implements SynchronousEventDispatcher, Consumer<List<CloudEvent>> {

    /**
     * Several handlers, unlike the push models, which take one each. Fan-out is safe here because there is no broker
     * and no acknowledgement to share: the events arrive from the write that just produced them, and a handler
     * exception reaches the writer. Under a transaction the write rolls back, so no handler's work survives. Without
     * one, {@link #dispatch(List, boolean)} offers every handler the event and reports the failures together, so a
     * failing handler cannot strand the handlers behind it. See ADR 90 for the isolation argument that makes the push
     * sinks single-consumer, and its ADR 57 follow-up for the no-transaction case here.
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
     * Dispatch the supplied cloud events to every matching registered handler, synchronously, on the calling thread, in
     * registration order, stopping at the first handler that throws.
     * <p>
     * For a dispatch that knows whether a transaction is in force, use {@link #dispatch(List, boolean)}, which is what
     * the application service calls. This form is for driving the model directly, for example from a test or from an
     * in-memory event-store write listener.
     *
     * @param writtenCloudEvents The newly written cloud events.
     */
    public void dispatch(List<CloudEvent> writtenCloudEvents) {
        route(writtenCloudEvents);
    }

    /**
     * Dispatch as {@link #dispatch(List)} does, told whether the caller wrapped this in a transaction.
     * <p>
     * Inside a transaction the first handler exception stops dispatch, because the write is about to roll back and the
     * handlers behind it would only be doing work that is discarded, possibly with effects outside the datastore.
     * Outside one the write has already committed, so every handler is offered every event and the failures are
     * reported afterwards. That is the difference this argument exists for: a handler skipped because a sibling threw
     * would never see the event again, since this model has no replay. See the ADR 57 amendment.
     *
     * @param writtenCloudEvents The newly written cloud events.
     * @param transactional      Whether the write and these handlers are running inside a transaction.
     */
    @Override
    public void dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
        if (transactional) {
            route(writtenCloudEvents);
        } else {
            routeIsolated(writtenCloudEvents);
        }
    }

    /**
     * Feeds the events in as {@link #dispatch(List)} does, so the model can be used directly as a
     * {@code Consumer<List<CloudEvent>>} listener (for example as an in-memory event-store write listener).
     * <p>
     * Handlers are isolated from each other here, because nothing on this path opens a transaction: the write has
     * already happened by the time a listener is called, so a handler that threw could otherwise leave the handlers
     * behind it never receiving that event at all.
     */
    @Override
    public void accept(List<CloudEvent> cloudEvents) {
        dispatch(cloudEvents, false);
    }
}
