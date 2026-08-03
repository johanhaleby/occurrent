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

import org.occurrent.dsl.saga.SagaInstances;
import org.occurrent.dsl.saga.SagaInstancesRegistry;
import org.occurrent.dsl.saga.internal.SagaInstancesRegistryImpl;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.occurrent.springboot.common.BackgroundCatchupFailures;
import org.occurrent.springboot.common.OnSubscriptionsNotDisabledCondition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

/**
 * The store-neutral half of a blocking Occurrent starter: the annotation post-processor and the saga instances
 * registry it fills in. A store starter imports this and contributes the store-specific seams
 * ({@link DefaultProjectionStoreProvider}, {@link DefaultSnapshotStoreProvider}, {@link DefaultSagaStateStoreProvider}
 * and {@code StartupWorkaround}) as beans of its own.
 */
@Configuration(proxyBeanMethods = false)
public class OccurrentBlockingAnnotationConfiguration {

    @Bean
    @Conditional(OnSubscriptionsNotDisabledCondition.class)
    static OccurrentBlockingAnnotationBeanPostProcessor occurrentBlockingAnnotationBeanPostProcessor() {
        return new OccurrentBlockingAnnotationBeanPostProcessor();
    }

    /**
     * Lets an application start a {@code @Projection(source = PUSH)} that {@code occurrent.subscription.mode=manual}
     * withheld at boot. Gated on the same property as the annotation post-processor that fills it in, since it has
     * nothing to hold when annotation processing is off, and it exists under every mode (not only {@code manual}) so
     * an application can inject it without conditioning its own wiring on the mode.
     */
    @Bean
    @ConditionalOnMissingBean(ManualStartProjections.class)
    @Conditional(OnSubscriptionsNotDisabledCondition.class)
    public ManualStartProjections occurrentManualStartProjections() {
        return new ManualStartProjections();
    }

    /**
     * Lets an application see a {@code @Projection(source = PUSH, startupMode = BACKGROUND)} whose catch-up failed.
     * Nobody waits for a background replay, so a failure has nowhere to be thrown and the context is long refreshed by
     * the time it happens. Gated the same way as the post-processor that writes it, and present under every startup
     * mode so an application can inject it without conditioning its own wiring on the mode.
     */
    @Bean
    @ConditionalOnMissingBean(BackgroundCatchupFailures.class)
    @Conditional(OnSubscriptionsNotDisabledCondition.class)
    public BackgroundCatchupFailures occurrentBackgroundCatchupFailures() {
        return new BackgroundCatchupFailures();
    }

    /**
     * Lets an application observe the instances of every {@code @Saga} in the context. It is defined here, rather than
     * registered as a singleton the way each saga's own {@link SagaInstances} is, so that it exists during refresh and
     * can be constructor-injected. The {@code @Saga} registrar fills it in afterwards, which is why it is empty until
     * the scan has run: a saga factory cannot be invoked before the beans it collaborates with are wired. See
     * {@link SagaInstancesRegistry} for what that means for a caller.
     * <p>
     * Gated on the same property as the annotation post-processor that populates it, because it has nothing to hold
     * when annotation processing is off. It is blocking-only, since {@code @Saga} is: the reactive starter has no saga
     * registrar.
     */
    @Bean
    @ConditionalOnMissingBean(SagaInstancesRegistry.class)
    @Conditional(OnSubscriptionsNotDisabledCondition.class)
    public SagaInstancesRegistryImpl occurrentSagaInstancesRegistry() {
        // The declared return type is the implementation, not the SagaInstancesRegistry interface an application
        // injects, so that the registrar's by-type lookup of the writable type matches from the bean definition rather
        // than only once the singleton has been instantiated. Declaring the interface here happens to work today
        // because population runs from afterSingletonsInstantiated, but it would silently start resolving nothing if
        // this bean became @Lazy or population moved earlier, leaving an empty registry forever.
        return new SagaInstancesRegistryImpl();
    }
}
