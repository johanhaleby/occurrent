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
 * {@link #dispatch(List)}, which routes each event to the registered handlers whose {@link SubscriptionFilter}
 * matches, invoking them in registration order and sequentially (the next handler does not start until the previous
 * one's {@link Mono} completes). A handler error propagates, so under a reactive transaction it rolls the write back.
 * <p>
 * The register-and-route machinery lives in {@link RegisteringSubscribable}. This model adds the application-service
 * dispatch entry point. For an externally driven push feed (RabbitMQ, Kafka, ...) use {@code PushSubscriptionModel}.
 */
@NullMarked
public class SynchronousSubscriptionModel extends RegisteringSubscribable implements ReactiveSynchronousEventDispatcher {

    /**
     * Several handlers, unlike the push models, which take one each. Fan-out is safe here because there is no broker
     * and no acknowledgement to share: the events arrive from the write that just produced them, and a handler error
     * propagates to the writer rather than stranding the handlers behind it. Under a reactive transaction the write
     * rolls back, so nothing is folded by anyone. See ADR 90 for the isolation argument that makes the push sinks
     * single-consumer, and the follow-up it leaves open for the no-transaction case here.
     */
    public SynchronousSubscriptionModel() {
        super(Consumers.MANY);
    }

    @Override
    public Mono<Void> dispatch(List<CloudEvent> writtenCloudEvents) {
        return route(writtenCloudEvents);
    }
}
