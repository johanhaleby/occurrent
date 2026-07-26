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

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins that the store-neutral reactive configuration actually contributes the annotation post-processor, and that the
 * subscription kill switch removes it. Both halves are needed: an absence-only test passes just as happily when the
 * {@code @Bean} method is gone entirely.
 * <p>
 * The reactive twin of {@code AnnotationBeanPostProcessorDestroyCallbackTest} in the blocking module, and the only
 * container-free coverage of this configuration class (the store starter's equivalent is Docker-gated).
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class OccurrentReactiveAnnotationConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(OccurrentReactiveAnnotationConfiguration.class));

    @Test
    void the_post_processor_is_contributed_by_default() {
        runner.run(context -> assertThat(context).hasSingleBean(OccurrentReactiveAnnotationBeanPostProcessor.class));
    }

    @Test
    void the_post_processor_is_contributed_when_subscriptions_are_explicitly_enabled() {
        runner.withPropertyValues("occurrent.subscription.enabled=true")
                .run(context -> assertThat(context).hasSingleBean(OccurrentReactiveAnnotationBeanPostProcessor.class));
    }

    @Test
    void turning_subscriptions_off_removes_the_post_processor_entirely() {
        runner.withPropertyValues("occurrent.subscription.enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean(OccurrentReactiveAnnotationBeanPostProcessor.class));
    }
}
