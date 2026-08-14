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

import java.util.List;
import java.util.Set;

/**
 * Refuses to finish startup when the competing-consumer fence cannot do what the configuration implies, rather than
 * letting a subscription find out on a checkpoint write hours later.
 * <p>
 * A {@link SmartInitializingSingleton} because it runs after every singleton exists, so asking for a strategy bean here
 * cannot pull one into existence early and close the construction cycle
 * {@link CompetingConsumerCheckpointWriteVersionSource} resolves lazily to avoid. That callback is the one for an
 * application with no annotations to register, and it only ever runs the storage-wide check below, since it has no
 * subscription id of its own to ask about. {@link OccurrentBlockingAnnotationBeanPostProcessor} calls
 * {@link #check(ApplicationContext, Set)} itself before it registers anything, since a push projection or saga writes
 * a checkpoint while catching up and would reach that write first, and passes the subscription ids the id-specific
 * check below asks about. {@link CheckpointStorageCannotFenceSubscriptionException}'s javadoc says exactly which
 * ids those are, which are left out even though the storage might refuse them, and which are asked about even
 * though the storage refusing them would never matter.
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
     * Runs the same checks for a caller that holds a context and has to run them at a moment of its own choosing,
     * against every subscription id it can enumerate.
     * <p>
     * Reads beans rather than creating any, so running it a second time from the callback below costs nothing.
     */
    static void check(ApplicationContext applicationContext, Set<String> subscriptionIds) {
        new CheckpointFencingConfigurationCheck(applicationContext.getBeanProvider(CompetingConsumerStrategy.class),
                applicationContext.getBeanProvider(CheckpointStorage.class),
                applicationContext.getBeanProvider(OccurrentProperties.class)).check(subscriptionIds);
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

    // The bean's own SmartInitializingSingleton callback, with no subscription id to ask about, see the class
    // javadoc for when this is the only invocation that runs.
    @Override
    public void afterSingletonsInstantiated() {
        check(Set.of());
    }

    private void check(Set<String> subscriptionIds) {
        // Throws when several strategy beans compete, whatever the fencing property says, because the strategy also
        // decides which node delivers events and which one polls a saga's timers.
        CompetingConsumerStrategy strategy = CompetingConsumerStrategies.resolveUnique(strategyProvider);
        if (strategy == null || !fenceCheckpoints(propertiesProvider)) {
            return;
        }
        @Nullable CheckpointStorage storage = storageProvider.getIfUnique();
        if (storage == null) {
            return;
        }
        if (!storage.evaluatesWriteConditions()) {
            throw new CheckpointStorageCannotFenceException(storage.getClass());
        }
        // The storage-wide answer above is true, so a caller wiring the pair together has been told this storage can
        // fence. Ask it again for each id specifically, since a storage can answer true overall while refusing one
        // shape of id, and collect every failure into one exception rather than stopping at the first.
        List<String> unsupportedSubscriptionIds = subscriptionIds.stream()
                .filter(id -> !storage.evaluatesWriteConditionsFor(id))
                .sorted()
                .toList();
        if (!unsupportedSubscriptionIds.isEmpty()) {
            throw new CheckpointStorageCannotFenceSubscriptionException(storage.getClass(), unsupportedSubscriptionIds);
        }
    }
}
