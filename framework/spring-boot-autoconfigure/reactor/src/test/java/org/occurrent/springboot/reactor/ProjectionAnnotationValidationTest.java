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

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.Catchup;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.Source;
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.StartupMode;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.occurrent.subscription.push.reactor.PushSubscriptionModel;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * The reactor twin of the blocking {@code ProjectionAnnotationValidationTest}, scoped to the {@code catchup}
 * validation this class adds: rejecting it on a {@code source=EVENT_STORE} projection, and how it changes the
 * start-position and {@code startupMode} rejections for a {@code source=PUSH} one. Each fails fast at context startup
 * with no Docker involved.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ProjectionAnnotationValidationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
            .withUserConfiguration(ConverterConfiguration.class);

    @Test
    void a_default_catchup_push_projection_with_no_reader_bean_fails_fast_naming_catchup_none() {
        runner.withUserConfiguration(PushModelFeedConfiguration.class, PushDefaultCatchupConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("@Projection 'push-default-catchup-reactive'")
                    .hasMessageContaining("catches up from the event store before going live, which needs a PositionOrderedReader bean")
                    .hasMessageContaining("Set catchup = NONE");
        });
    }

    @Test
    void an_event_store_projection_that_sets_catchup_fails_fast_and_points_at_start_at() {
        runner.withUserConfiguration(EventStoreCatchupConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("sets catchup, which only applies to source=PUSH")
                    .hasMessageContaining("startAt = NOW to skip it");
        });
    }

    @Test
    void a_push_projection_with_catchup_none_that_sets_a_start_position_fails_fast_without_the_startup_mode_hint() {
        runner.withUserConfiguration(PushModelFeedConfiguration.class, PushCatchupNoneStartKnobsConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("cannot set startAt, startAtGlobalPosition or resumeBehavior")
                    .hasMessageContaining("With catchup=NONE it takes live events only")
                    .hasMessageNotContaining("startupMode = BACKGROUND");
        });
    }

    @Test
    void a_push_projection_with_catchup_none_that_sets_startup_mode_fails_fast() {
        runner.withUserConfiguration(PushModelFeedConfiguration.class, PushCatchupNoneStartupModeConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("replays nothing and there is no startup work for startupMode to decide about")
                    .hasMessageContaining("drop catchup=NONE if you meant the projection to catch up first");
        });
    }

    private static org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> countProjection() {
        return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                .id(event -> "k")
                .on(TestEvent.class, (state, event) -> state + 1)
                .build();
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("TestEvent").build();
    }

    record TestEvent(String id) {
    }

    @Configuration(proxyBeanMethods = false)
    static class ConverterConfiguration {
        @Bean
        CloudEventConverter<TestEvent> cloudEventConverter() {
            return new CloudEventConverter<>() {
                @Override
                public CloudEvent toCloudEvent(TestEvent domainEvent) {
                    return cloudEvent(domainEvent.id());
                }

                @Override
                public TestEvent toDomainEvent(CloudEvent cloudEvent) {
                    return new TestEvent(cloudEvent.getId());
                }

                @Override
                public String getCloudEventType(Class<? extends TestEvent> type) {
                    return type.getSimpleName();
                }
            };
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PushModelFeedConfiguration {
        @Bean
        PushSubscriptionModel pushModel() {
            return new PushSubscriptionModel();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PushDefaultCatchupConfiguration {
        @Bean
        PushDefaultCatchupProjection pushDefaultCatchupProjection() {
            return new PushDefaultCatchupProjection();
        }
    }

    static class PushDefaultCatchupProjection {
        @Projection(id = "push-default-catchup-reactive", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return countProjection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class EventStoreCatchupConfiguration {
        // The registrar skips its whole scan with neither a Subscribable nor a SynchronousSubscriptionModel bean
        // present, so this fixture needs one even though the rejection below fires before it would ever be used.
        @Bean
        Subscribable subscribable() {
            return mock(Subscribable.class);
        }

        @Bean
        EventStoreCatchupProjection eventStoreCatchupProjection() {
            return new EventStoreCatchupProjection();
        }
    }

    static class EventStoreCatchupProjection {
        @Projection(id = "event-store-catchup-reactive", catchup = Catchup.NONE)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return countProjection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PushCatchupNoneStartKnobsConfiguration {
        @Bean
        PushCatchupNoneStartKnobsProjection pushCatchupNoneStartKnobsProjection() {
            return new PushCatchupNoneStartKnobsProjection();
        }
    }

    static class PushCatchupNoneStartKnobsProjection {
        @Projection(id = "push-none-start-knobs-reactive", source = Source.PUSH, catchup = Catchup.NONE, startAt = StartPosition.BEGINNING)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return countProjection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PushCatchupNoneStartupModeConfiguration {
        @Bean
        PushCatchupNoneStartupModeProjection pushCatchupNoneStartupModeProjection() {
            return new PushCatchupNoneStartupModeProjection();
        }
    }

    static class PushCatchupNoneStartupModeProjection {
        @Projection(id = "push-none-startup-mode-reactive", source = Source.PUSH, catchup = Catchup.NONE, startupMode = StartupMode.BACKGROUND)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return countProjection();
        }
    }
}
