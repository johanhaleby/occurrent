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

package org.occurrent.springboot.reactor;

import org.occurrent.springboot.common.PushCatchupStatus;
import org.occurrent.springboot.common.PushCatchupStatusImpl;
import org.occurrent.springboot.common.OnSubscriptionsNotDisabledCondition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

/**
 * The store-neutral half of a reactive Occurrent starter: the annotation post-processor and the background catch-up
 * failures it records. A store starter imports this and contributes the store-specific seams
 * ({@link DefaultReactiveSnapshotStoreProvider} and {@code StartupWorkaround}) as beans of its own.
 * <p>
 * Unlike the blocking twin there is no saga instances registry here, since {@code @Saga} is blocking-only, no
 * read-model store seam, since the reactive stack has no zero-config projection store default, and no manual-start
 * registry, since {@code occurrent.subscription.mode = manual} is blocking-only so far.
 */
@Configuration(proxyBeanMethods = false)
public class OccurrentReactiveAnnotationConfiguration {

    @Bean
    @Conditional(OnSubscriptionsNotDisabledCondition.class)
    static OccurrentReactiveAnnotationBeanPostProcessor occurrentReactiveAnnotationBeanPostProcessor() {
        return new OccurrentReactiveAnnotationBeanPostProcessor();
    }

    /**
     * Lets an application start a {@code @Projection(source = PUSH)} that {@code occurrent.subscription.mode=manual}
     * withheld at boot. Gated on the same property as the annotation post-processor that fills it in, since it has
     * nothing to hold when annotation processing is off, and it exists under every mode (not only {@code manual}) so
     * an application can inject it without conditioning its own wiring on the mode.
     */
    @Bean
    @ConditionalOnMissingBean(ManualStartPushSources.class)
    @Conditional(OnSubscriptionsNotDisabledCondition.class)
    public ManualStartPushSources occurrentManualStartPushSources() {
        return new ManualStartPushSources();
    }

    /**
     * Lets an application see where every {@code source = PUSH} projection and saga is in its catch-up, and why one
     * failed. Nobody waits for a {@code startupMode = BACKGROUND} replay, so neither its progress nor its failure has
     * anywhere to be returned or thrown, and the context is long refreshed by the time either happens. Gated the same
     * way as the post-processor that writes it, and present under every startup mode so an application can inject it
     * without conditioning its own wiring on the mode.
     */
    @Bean
    @ConditionalOnMissingBean(PushCatchupStatus.class)
    @Conditional(OnSubscriptionsNotDisabledCondition.class)
    public PushCatchupStatusImpl occurrentPushCatchupStatus() {
        // The declared return type is the implementation, not the PushCatchupStatus interface an application injects,
        // so that the registrar's by-type lookup of the writable type matches from the bean definition rather than
        // only once the singleton has been instantiated. See OccurrentBlockingAnnotationConfiguration for the same
        // reasoning applied to SagaInstancesRegistryImpl.
        return new PushCatchupStatusImpl();
    }
}
