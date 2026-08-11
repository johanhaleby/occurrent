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
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;

/**
 * Refuses to finish startup when the competing-consumer fence cannot do what the configuration implies, rather than
 * letting a subscription find out on a checkpoint write hours later.
 * <p>
 * A {@link SmartInitializingSingleton} because it runs after every singleton exists, so asking for a strategy bean here
 * cannot pull one into existence early and close the construction cycle
 * {@link CompetingConsumerCheckpointWriteVersionSource} resolves lazily to avoid. That callback is the one for an
 * application with no annotations to register. {@link OccurrentBlockingAnnotationBeanPostProcessor} calls
 * {@link #check(ApplicationContext)} itself before it registers anything, since a push projection or saga writes a
 * checkpoint while catching up and would reach that write first.
 */
class CheckpointFencingConfigurationCheck implements SmartInitializingSingleton {

    /**
     * Whether checkpoint writes carry the lease version.
     * <p>
     * Answers the default when no {@link OccurrentProperties} bean is registered, so a wiring site can ask without
     * requiring one to exist.
     */
    static boolean fenceCheckpoints(ObjectProvider<OccurrentProperties> propertiesProvider) {
        OccurrentProperties properties = propertiesProvider.getIfAvailable();
        return properties == null || properties.getSubscription().getCompetingConsumer().isFenceCheckpoints();
    }

    /**
     * Runs the same two checks for a caller that holds a context and has to run them at a moment of its own choosing.
     * <p>
     * Reads beans rather than creating any, so running it a second time from the callback below costs nothing.
     */
    static void check(ApplicationContext applicationContext) {
        new CheckpointFencingConfigurationCheck(applicationContext.getBeanProvider(CompetingConsumerStrategy.class),
                applicationContext.getBeanProvider(CheckpointStorage.class),
                applicationContext.getBeanProvider(OccurrentProperties.class)).afterSingletonsInstantiated();
    }

    private final ObjectProvider<CompetingConsumerStrategy> strategyProvider;
    private final ObjectProvider<CheckpointStorage> storageProvider;
    private final ObjectProvider<OccurrentProperties> propertiesProvider;

    CheckpointFencingConfigurationCheck(ObjectProvider<CompetingConsumerStrategy> strategyProvider,
                                        ObjectProvider<CheckpointStorage> storageProvider,
                                        ObjectProvider<OccurrentProperties> propertiesProvider) {
        this.strategyProvider = strategyProvider;
        this.storageProvider = storageProvider;
        this.propertiesProvider = propertiesProvider;
    }

    @Override
    public void afterSingletonsInstantiated() {
        // Throws when several strategy beans compete, whatever the fencing property says, because the strategy also
        // decides which node delivers events and which one polls a saga's timers.
        CompetingConsumerStrategy strategy = CompetingConsumerStrategies.resolveUnique(strategyProvider);
        if (strategy == null || !fenceCheckpoints(propertiesProvider)) {
            return;
        }
        @Nullable CheckpointStorage storage = storageProvider.getIfUnique();
        if (storage != null && !storage.evaluatesWriteConditions()) {
            throw new CheckpointStorageCannotFenceException(storage.getClass());
        }
    }
}
