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

import org.occurrent.subscription.api.reactor.SubscriptionModelLifeCycle;
import org.occurrent.testing.junit.reactor.OccurrentSubscriptionsExtension;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import java.util.List;

/**
 * The reactive counterpart of {@link OccurrentTestingConfiguration}: exposes a reactive
 * {@link OccurrentSubscriptionsExtension} over every reactive {@link SubscriptionModelLifeCycle} bean in the
 * application context.
 * <p>
 * Registered under its own bean name, distinct from the blocking configuration's, so a mixed application using both
 * stacks gets both extensions rather than one overwriting the other.
 *
 * @see EnableOccurrentTesting
 */
@Configuration(proxyBeanMethods = false)
public class OccurrentReactorTestingConfiguration {

    /**
     * The extension that stops every reactive subscription model before and after each test.
     * <p>
     * It is a prototype bean for the same reason the blocking one is: the extension accumulates the subscription ids a
     * test told it about, and a test class registering it with {@code @RegisterExtension} should not inherit the ids
     * another test class named.
     *
     * @param subscriptionModels every reactive {@code SubscriptionModelLifeCycle} bean in the application context
     * @return an extension to register with {@code @RegisterExtension}
     * @throws IllegalStateException if the context has no such bean, so there is nothing to stop
     */
    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public OccurrentSubscriptionsExtension occurrentReactorSubscriptionsExtension(ObjectProvider<SubscriptionModelLifeCycle> subscriptionModels) {
        List<SubscriptionModelLifeCycle> models = subscriptionModels.orderedStream().toList();
        if (models.isEmpty()) {
            throw new IllegalStateException("No " + SubscriptionModelLifeCycle.class.getSimpleName() + " bean found in "
                    + "the application context, so there is nothing for @" + EnableOccurrentTesting.class.getSimpleName()
                    + " to stop.");
        }
        return OccurrentSubscriptionsExtension.stoppedByDefault(models);
    }
}
