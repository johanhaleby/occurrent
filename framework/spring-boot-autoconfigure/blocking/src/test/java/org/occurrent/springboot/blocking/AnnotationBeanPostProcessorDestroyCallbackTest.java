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

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the annotation post-processor is contributed by the store-neutral configuration, that the subscription kill
 * switch removes it, and that Spring actually calls {@code destroy} on it when the context shuts down.
 * <p>
 * The destroy callback is what {@code SagaAnnotationRegistrar.close} hangs off, so a callback that stops firing leaves
 * poller threads behind. That downstream effect is deliberately NOT asserted here: observing a real {@code @Saga} timer
 * poller needs a running store, so it belongs to the Docker-gated tests in the store starter. What this file pins is
 * the callback firing at all.
 * <p>
 * Worth pinning because moving the post-processor between modules changed how its bean is declared. An earlier
 * version of this test asserted the bean definition's recorded type implements {@code DisposableBean}, which turned
 * out to prove nothing: the recorded type resolves to the instantiated singleton's concrete class, so it passed even
 * when the {@code @Bean} method's declared return type was narrowed. Observing the callback itself is the only form
 * of this test that can fail.
 * <p>
 * Container-free, since context lifecycle needs no store.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class AnnotationBeanPostProcessorDestroyCallbackTest {

    @Test
    void closing_the_context_runs_the_destroy_callback_that_stops_the_saga_timer_pollers() {
        RecordingPostProcessor postProcessor = new RecordingPostProcessor();

        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, () -> postProcessor)
                .run(context -> assertThat(postProcessor.destroyed).isFalse());

        // ApplicationContextRunner closes the context once the lambda returns, so the callback lands after it.
        assertThat(postProcessor.destroyed).isTrue();
    }

    // The presence half of the pair below. Without it the absence test passes just as happily when the @Bean method is
    // gone entirely, and the destroy test above registers its own instance so it proves nothing about the wiring.
    @Test
    void the_post_processor_is_contributed_by_default() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(OccurrentBlockingAnnotationConfiguration.class))
                .run(context -> assertThat(context).hasSingleBean(OccurrentBlockingAnnotationBeanPostProcessor.class));
    }

    @Test
    void the_post_processor_is_contributed_when_subscriptions_are_explicitly_enabled() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(OccurrentBlockingAnnotationConfiguration.class))
                .withPropertyValues("occurrent.subscription.enabled=true")
                .run(context -> assertThat(context).hasSingleBean(OccurrentBlockingAnnotationBeanPostProcessor.class));
    }

    @Test
    void turning_subscriptions_off_removes_the_post_processor_entirely() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(OccurrentBlockingAnnotationConfiguration.class))
                .withPropertyValues("occurrent.subscription.enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean(OccurrentBlockingAnnotationBeanPostProcessor.class));
    }

    static class RecordingPostProcessor extends OccurrentBlockingAnnotationBeanPostProcessor {
        final AtomicBoolean destroyed = new AtomicBoolean();

        @Override
        public void destroy() {
            destroyed.set(true);
            super.destroy();
        }
    }
}
