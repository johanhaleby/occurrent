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

package org.occurrent.springboot.blocking;

import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.api.blocking.CheckpointWriteVersionSource;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.springframework.beans.factory.ObjectProvider;

import java.util.OptionalLong;
import java.util.function.BooleanSupplier;

import static java.util.Objects.requireNonNull;

/**
 * Turns a {@link CompetingConsumerStrategy} bean, resolved lazily, into a {@link CheckpointWriteVersionSource} (see
 * ADR 116). One instance is built per wiring site that constructs a checkpoint-writing model, since there is no
 * single bean the starter can wrap that reaches every site. Public so a store starter's own auto-configuration
 * (outside this module) can use it at the wiring sites it owns, the way {@link OccurrentBlockingBeanNames} is.
 * <p>
 * Resolution happens on the first checkpoint write rather than at construction. {@link CompetingConsumerStrategy}
 * depends on {@code List<CompetingConsumerListener>}, an open extension point, and a user listener that injects a
 * subscription model would otherwise close a construction cycle with the model this source is built for. Which bean
 * that resolution picks, and when it refuses to pick one, is decided by {@code CompetingConsumerStrategies}, the same
 * rule the startup check applies.
 * <p>
 * Once a strategy is found it is kept, and a first attempt that finds none is retried on the next write rather
 * than disabling the fence for the life of the process. The field is {@code volatile} rather than guarded, since
 * resolving twice under a race is harmless (both attempts settle on the same bean). A resolution that throws
 * counts as no version for that write, which keeps a registrar-driven checkpoint write working while the strategy
 * bean is still being built. An ambiguous strategy is the exception, since answering no version there would be the
 * fence quietly switching itself off.
 */
public final class CompetingConsumerCheckpointWriteVersionSource implements CheckpointWriteVersionSource {

    private final ObjectProvider<CompetingConsumerStrategy> strategyProvider;
    private final BooleanSupplier fenceCheckpoints;
    private volatile @Nullable Boolean fencing;
    private volatile @Nullable CompetingConsumerStrategy strategy;

    /**
     * @param strategyProvider Resolves the {@link CompetingConsumerStrategy} bean to read a fencing token from.
     * @param fenceCheckpoints Answers {@code false} to write every checkpoint unconditionally. Asked once, on the same
     *                         first write that resolves the strategy, so a wiring site reading
     *                         {@code occurrent.subscription.competing-consumer.fence-checkpoints} can be built before
     *                         the properties bean exists.
     */
    public CompetingConsumerCheckpointWriteVersionSource(ObjectProvider<CompetingConsumerStrategy> strategyProvider, BooleanSupplier fenceCheckpoints) {
        this.strategyProvider = requireNonNull(strategyProvider, "strategyProvider cannot be null");
        this.fenceCheckpoints = requireNonNull(fenceCheckpoints, "fenceCheckpoints cannot be null");
    }

    @Override
    public OptionalLong writeVersion(String subscriptionId) {
        Boolean fenced = fencing;
        if (fenced == null) {
            fenced = fenceCheckpoints.getAsBoolean();
            fencing = fenced;
        }
        if (!fenced) {
            return OptionalLong.empty();
        }
        CompetingConsumerStrategy resolved = strategy;
        if (resolved == null) {
            try {
                resolved = CompetingConsumerStrategies.resolveUnique(strategyProvider);
            } catch (AmbiguousCompetingConsumerStrategyException e) {
                throw e;
            } catch (RuntimeException e) {
                return OptionalLong.empty();
            }
            if (resolved == null) {
                return OptionalLong.empty();
            }
            strategy = resolved;
        }
        return resolved.fencingToken(subscriptionId);
    }
}
