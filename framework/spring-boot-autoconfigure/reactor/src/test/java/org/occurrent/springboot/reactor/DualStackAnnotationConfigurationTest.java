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
import org.occurrent.springboot.blocking.OccurrentBlockingAnnotationConfiguration;
import org.occurrent.springboot.common.PushCatchupStatus;
import org.occurrent.springboot.common.PushCatchupStatusImpl;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * ADR 44 promises that the blocking and reactive starters "can sit on one classpath because the bean types are
 * disjoint". {@link OccurrentBlockingAnnotationConfiguration} and {@link OccurrentReactiveAnnotationConfiguration} are
 * the store-neutral half of each store starter, imported by {@code @EnableOccurrent} and {@code @EnableOccurrentReactive}
 * respectively, so this is the layer where an application actually combining both starters exercises that claim.
 * <p>
 * Until #621 both declared a {@code @Bean} method named {@code occurrentManualStartPushSources}, returning a different,
 * stack-specific {@code ManualStartPushSources} type each. A bean name collides regardless of the declared type, so a
 * context importing both configurations failed to refresh: exactly what combining {@code @EnableOccurrent} and
 * {@code @EnableOccurrentReactive} does.
 * <p>
 * Container-free: both configurations are store-neutral, with no Mongo dependency of their own, so no store starter or
 * Testcontainer is needed to reproduce or guard this.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DualStackAnnotationConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(OccurrentBlockingAnnotationConfiguration.class, OccurrentReactiveAnnotationConfiguration.class));

    @Test
    void the_blocking_and_reactive_annotation_configurations_coexist_with_distinct_manual_start_push_sources_beans() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(org.occurrent.springboot.blocking.ManualStartPushSources.class);
            assertThat(context).hasSingleBean(ManualStartPushSources.class);

            // ADR 44 relies on this being the SAME bean, contributed once and shared by both stacks' registrars (see
            // PushCatchupStatusImpl.register), not a second name collision the way occurrentManualStartPushSources was.
            assertThat(context).hasSingleBean(PushCatchupStatus.class);
        });
    }

    @Test
    void the_shared_push_catchup_status_bean_rejects_a_second_registration_from_the_other_stack() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            PushCatchupStatusImpl status = context.getBean(PushCatchupStatusImpl.class);

            // Stands in for what each stack's own annotation processor does when a source = PUSH projection or saga
            // goes live: one shared PushCatchupStatusImpl bean, written to independently by the blocking and reactive
            // registrars, each keeping its own id set (see the class javadoc on register(..)). Two @Projection beans
            // with the same id do not reach this guard in practice: each post-processor's own registeredIds set scans
            // every bean in the context, blocking stack and reactive stack alike, and rejects the duplicate id first
            // (the archrev epic verified this path unreachable through the annotations for that reason). This asserts
            // the guard both stacks actually depend on directly, against the one bean the fixed dual-stack context
            // wires up, rather than through a route the framework already closes earlier.
            status.register("dual-stack-push-projection", () -> false, () -> true);

            assertThatThrownBy(() -> status.register("dual-stack-push-projection", () -> false, () -> true))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("dual-stack-push-projection")
                    .hasMessageContaining("already registered");
        });
    }
}
