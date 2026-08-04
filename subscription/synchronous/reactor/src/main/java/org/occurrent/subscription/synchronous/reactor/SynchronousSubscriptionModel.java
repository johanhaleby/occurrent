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
import org.occurrent.inmemory.filtermatching.DataFieldReader;
import org.occurrent.application.service.reactor.ReactiveSynchronousEventDispatcher;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.RegisteringSubscribable;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * The reactive counterpart of the blocking {@code SynchronousSubscriptionModel}: a register-only reactive
 * {@link org.occurrent.subscription.api.reactor.Subscribable} whose handlers are composed <strong>synchronously</strong>
 * into the writer's reactive chain, before {@code execute} completes, rather than being driven asynchronously off a
 * change stream.
 * <p>
 * Driven by the reactive application service: after a successful write it hands the just-written cloud events to
 * {@link #dispatch(List, boolean)}, which routes each event to the registered handlers whose
 * {@link SubscriptionFilter} matches, invoking them in registration order and sequentially (the next handler does not
 * start until the previous one's {@link Mono} completes). A handler error reaches the caller, so under a reactive
 * transaction it rolls the write back. Whether it also stops the handlers behind it depends on the transaction, which
 * is what that second argument carries.
 * <p>
 * The register-and-route machinery lives in {@link RegisteringSubscribable}. This model adds the application-service
 * dispatch entry point. For an externally driven push feed (RabbitMQ, Kafka, ...) use {@code PushSubscriptionModel}.
 */
@NullMarked
public class SynchronousSubscriptionModel extends RegisteringSubscribable implements ReactiveSynchronousEventDispatcher {

    /**
     * Several handlers, unlike the push models, which take one each. Fan-out is safe here because there is no broker
     * and no acknowledgement to share: the events arrive from the write that just produced them, and a handler error
     * reaches the writer. Under a reactive transaction the write rolls back, so no handler's work survives. Without
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
        return route(writtenCloudEvents);
    }

    /**
     * Dispatch as {@link #dispatch(List)} does, told whether the caller wrapped this in a reactive transaction.
     * <p>
     * Inside a transaction the first handler error stops dispatch, because the write is about to roll back and the
     * handlers behind it would only be doing work that is discarded, possibly with effects outside the datastore.
     * Outside one the write has already committed, so every handler is offered every event and the failures are
     * reported afterwards. That is the difference this argument exists for: a handler skipped because a sibling errored
     * would never see the event again, since this model has no replay. See the ADR 57 amendment.
     *
     * @param writtenCloudEvents The newly written cloud events.
     * @param transactional      Whether the write and these handlers are running inside a transaction.
     * @return A {@link Mono} that completes when dispatch is done.
     */
    @Override
    public Mono<Void> dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
        return transactional ? route(writtenCloudEvents) : routeIsolated(writtenCloudEvents);
    }
}
