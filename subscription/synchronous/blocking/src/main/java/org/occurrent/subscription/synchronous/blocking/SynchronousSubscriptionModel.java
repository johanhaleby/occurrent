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
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.RegisteringSubscribable;

import java.util.List;
import java.util.function.Consumer;

/**
 * A register-only subscription model whose handlers are invoked <strong>synchronously</strong>, in-process, on
 * the thread that supplies the events, rather than asynchronously off a change stream.
 * <p>
 * It exists to be driven by the application service: after a successful write, the application service hands
 * the just-written cloud events to {@link #accept(List)}, which routes each event to the registered handlers
 * whose {@link SubscriptionFilter} matches, invoking them in registration order on the calling thread. A
 * handler exception propagates to the caller (so, under a transaction, it rolls the write back).
 * <p>
 * Unlike the asynchronous {@code SubscriptionModel}s, this model has no lifecycle, start position, checkpoint,
 * catch-up, or replay: it only ever reacts to events fed to it here and now. The register-and-route machinery
 * lives in {@link RegisteringSubscribable}. This model adds the application-service dispatch entry point. For an
 * externally driven push feed (RabbitMQ, Kafka, ...) use {@code PushSubscriptionModel} instead.
 */
@NullMarked
public class SynchronousSubscriptionModel extends RegisteringSubscribable implements SynchronousEventDispatcher, Consumer<List<CloudEvent>> {

    /**
     * Dispatch the supplied cloud events to every matching registered handler, synchronously, on the calling
     * thread, in registration order. Called by the application service with the events it just wrote.
     *
     * @param writtenCloudEvents The newly written cloud events.
     */
    @Override
    public void dispatch(List<CloudEvent> writtenCloudEvents) {
        route(writtenCloudEvents);
    }

    /**
     * Alias for {@link #dispatch(List)} so the model can also be used directly as a
     * {@code Consumer<List<CloudEvent>>} listener (for example as an in-memory event-store write listener).
     */
    @Override
    public void accept(List<CloudEvent> cloudEvents) {
        dispatch(cloudEvents);
    }
}
