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
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;

/**
 * The four ways a caller can say where a subscription starts, named so a fixture can declare which of them its model
 * accepts.
 * <p>
 * There are exactly four because {@link StartAt} is a sealed interface with four permitted implementations, so this
 * enum cannot fall behind it without the compiler saying so. It exists rather than a set of {@code StartAt} instances
 * because a declaration has to name the variants a model <em>refuses</em> as well as the ones it takes, and a refused
 * variant has no instance the fixture would want to build.
 */
@NullMarked
public enum StartAtVariant {

    /**
     * {@link StartAt#now()}: from this moment, whatever the model was doing before.
     */
    NOW,

    /**
     * {@link StartAt#subscriptionModelDefault()}: wherever the model itself thinks a subscription should start, which
     * for a model wrapping a checkpoint is the last position it stored.
     */
    SUBSCRIPTION_MODEL_DEFAULT,

    /**
     * {@link StartAt#checkpoint(Checkpoint)}: from a position the caller has in hand.
     */
    CHECKPOINT,

    /**
     * {@link StartAt#dynamic(java.util.function.Supplier)}: resolved when the model asks, so it can change over the
     * life of the model. It is also how a caller tells a wrapper not to handle a subscription at all, by resolving to
     * null.
     */
    DYNAMIC;

    /**
     * Builds the {@code StartAt} this variant names.
     *
     * @param checkpointToStartFrom The checkpoint {@link #CHECKPOINT} starts from. Unused by the other three, since
     *                              none of them carries a position.
     */
    public StartAt startAt(Checkpoint checkpointToStartFrom) {
        return switch (this) {
            case NOW -> StartAt.now();
            case SUBSCRIPTION_MODEL_DEFAULT -> StartAt.subscriptionModelDefault();
            case CHECKPOINT -> StartAt.checkpoint(checkpointToStartFrom);
            // Resolves to the model's own default, so an accepting model has somewhere to start and this variant tests
            // the acceptance of a dynamic position rather than the position it happens to resolve to.
            case DYNAMIC -> StartAt.dynamic(StartAt::subscriptionModelDefault);
        };
    }
}
