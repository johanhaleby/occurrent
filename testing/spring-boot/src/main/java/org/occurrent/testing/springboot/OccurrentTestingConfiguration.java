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

package org.occurrent.testing.springboot;

import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.SubscriptionModelLifeCycle;
import org.occurrent.testing.junit.blocking.OccurrentSubscriptionsExtension;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import java.util.List;

/**
 * Exposes an {@link OccurrentSubscriptionsExtension} over every blocking {@link SubscriptionModelLifeCycle} bean in
 * the application context, so a test can autowire the extension instead of constructing it from the subscription
 * models it first has to inject itself.
 * <p>
 * It wires every such bean, not just one, because a context can hold more than one life-cycle bearing model, for
 * example a durable model and a {@code SynchronousSubscriptionModel}, and deny-by-default means none of them run
 * until a test asks.
 *
 * @see EnableOccurrentTesting
 */
@Configuration(proxyBeanMethods = false)
public class OccurrentTestingConfiguration {

    /**
     * The extension that stops every subscription model before and after each test.
     * <p>
     * It is a prototype bean because the extension accumulates the subscription ids a test told it about, and a test
     * class registering it with {@code @RegisterExtension} should not inherit the ids another test class named.
     * <p>
     * It also clears state on its own where the context already holds what that takes. Exactly one
     * {@code CheckpointStorage} bean is auto-applied with {@code clearingCheckpoints(..)}, the ambiguous case of more
     * than one left for a test to wire by hand with {@code clearingCheckpointsFor(..)}. A
     * {@code clearState = true} store integration, {@code OccurrentMongoFlushTestingConfiguration} for MongoDB today,
     * is auto-applied with {@code clearingStateWith(..)} the same way.
     *
     * @param subscriptionModels every {@code SubscriptionModelLifeCycle} bean in the application context
     * @param checkpointStorages every {@code CheckpointStorage} bean in the application context
     * @param stateClearers      the {@code clearState = true} store integration's clearer, if one was wired
     * @return an extension to register with {@code @RegisterExtension}
     * @throws IllegalStateException if the context has no {@code SubscriptionModelLifeCycle} bean, so there is
     *                                nothing to stop
     */
    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public OccurrentSubscriptionsExtension occurrentSubscriptionsExtension(ObjectProvider<SubscriptionModelLifeCycle> subscriptionModels,
                                                                            ObjectProvider<CheckpointStorage> checkpointStorages,
                                                                            ObjectProvider<OccurrentTestStateClearer> stateClearers) {
        List<SubscriptionModelLifeCycle> models = subscriptionModels.orderedStream().toList();
        if (models.isEmpty()) {
            throw new IllegalStateException("No " + SubscriptionModelLifeCycle.class.getSimpleName() + " bean found in "
                    + "the application context, so there is nothing for @" + EnableOccurrentTesting.class.getSimpleName()
                    + " to stop.");
        }
        OccurrentSubscriptionsExtension extension = OccurrentSubscriptionsExtension.stoppedByDefault(models);
        CheckpointStorage checkpointStorage = checkpointStorages.getIfUnique();
        if (checkpointStorage != null) {
            extension.clearingCheckpoints(checkpointStorage);
        }
        OccurrentTestStateClearer stateClearer = stateClearers.getIfUnique();
        if (stateClearer != null) {
            extension.clearingStateWith(stateClearer);
        }
        return extension;
    }
}
