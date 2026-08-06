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

import org.jspecify.annotations.NullMarked;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import java.util.List;

/**
 * What a model hands {@link RestartConformance}, on top of everything {@link SubscriptionModelFixture} already asks
 * for.
 * <p>
 * Only a fixture that can do both halves of a restart supplies this. It must build the model a second time over
 * whatever it left behind, and feed events in while no model is running. A model whose events arrive by being handed to it has
 * neither, since there is nowhere for an event to wait, which is why declining is done by not extending the suite
 * rather than by answering a declaration.
 */
@NullMarked
public interface RestartableSubscriptionModelFixture extends SubscriptionModelFixture {

    /**
     * Shuts the current model down and builds a fresh one over the same durable state, the way restarting the
     * application would.
     * <p>
     * This rebuilds rather than restarts in place, because {@code SubscriptionModelLifeCycle.shutdown()} is documented
     * as not reversible, and starting the same instance again would prove nothing about state outliving a process.
     * <p>
     * After this call {@link #subscriptionModel()} must answer with the new model, so {@link #close()} shuts down the
     * one that is actually running, and {@link #publish(List)} must feed the new model too.
     *
     * @return The fresh model, with no subscriptions on it.
     */
    SubscriptionModel restart();

    /**
     * Whether a subscription re-created on the fresh model carries on from where the old one had got to.
     * <p>
     * Both answers are asserted and neither is free. Answering {@code true} owes delivery of an event published while
     * no model was running, which is the whole promise of keeping a checkpoint. Answering {@code false} owes the
     * opposite, that the fresh model starts at the present and the events from the gap are gone, which is exactly why
     * a model without durable state gets wrapped in one that has it.
     * <p>
     * Redelivery is allowed on either answer. Resuming from the last position that was stored rather than from just
     * after it hands an event over twice, and this contract is at-least-once, so the suite asserts that nothing is
     * lost rather than that nothing repeats.
     */
    boolean resumesAfterARestart();
}
