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
 * Proves Spring actually calls {@code destroy} on the annotation post-processor when the context shuts down. That
 * method closes every {@code @Saga} timer poller, so if the callback stops firing a poller thread outlives the
 * context and nothing else in the suite notices.
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
