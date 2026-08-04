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

import org.occurrent.subscription.api.blocking.SubscriptionModelLifeCycle;
import org.occurrent.testing.junit.blocking.OccurrentSubscriptionsExtension;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

/**
 * Exposes an {@link OccurrentSubscriptionsExtension} over the application context's own
 * {@link SubscriptionModelLifeCycle}, so a test can autowire the extension instead of constructing it from a
 * subscription model it first has to inject itself.
 *
 * @see EnableOccurrentTesting
 */
@Configuration(proxyBeanMethods = false)
public class OccurrentTestingConfiguration {

    /**
     * The extension that stops every subscription before and after each test.
     * <p>
     * It is a prototype bean because the extension accumulates the subscription ids a test told it about, and a test
     * class registering it with {@code @RegisterExtension} should not inherit the ids another test class named.
     *
     * @param subscriptionModel the subscription model in the application context
     * @return an extension to register with {@code @RegisterExtension}
     */
    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public OccurrentSubscriptionsExtension occurrentSubscriptionsExtension(SubscriptionModelLifeCycle subscriptionModel) {
        return OccurrentSubscriptionsExtension.stoppedByDefault(subscriptionModel);
    }
}
