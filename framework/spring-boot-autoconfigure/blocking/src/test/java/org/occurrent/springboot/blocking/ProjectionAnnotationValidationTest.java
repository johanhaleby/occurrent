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

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.Mode;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.Source;
import org.occurrent.annotation.StartPosition;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.projection.DcbProjection;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Characterizes the {@code @Projection} validation branches that fail fast before any subscription model or store is
 * consulted, so they reproduce without a running store (no Docker): the {@code source=PUSH} guards (no synchronous
 * mode, no catch-up start knobs, no DcbProjection, and a feed bean that is neither a {@code PushSubscriptionModel} nor
 * a {@code DomainEventFeed}), and the convention-based store resolution failing when the factory return type carries no
 * concrete state type and no store bean exists. Each must fail fast at context startup with the exact message.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ProjectionAnnotationValidationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
            .withUserConfiguration(ConverterConfiguration.class);

    @Test
    void push_projection_with_synchronous_mode_fails_fast() {
        runner.withUserConfiguration(FeedConfiguration.class, PushSynchronousConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("cannot combine source=PUSH with mode=SYNCHRONOUS");
        });
    }

    @Test
    void push_projection_with_catch_up_start_knobs_fails_fast() {
        runner.withUserConfiguration(FeedConfiguration.class, PushStartKnobsConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("does not support the catch-up start knobs");
        });
    }

    @Test
    void push_projection_returning_a_dcb_projection_fails_fast() {
        runner.withUserConfiguration(FeedConfiguration.class, PushDcbConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("with source=PUSH must return a Projection. A DcbProjection push source is not supported");
        });
    }

    @Test
    void push_projection_whose_feed_bean_is_neither_a_push_model_nor_a_domain_feed_fails_fast() {
        runner.withUserConfiguration(WrongFeedConfiguration.class, PushWrongFeedConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("which is neither a PushSubscriptionModel nor a DomainEventFeed");
        });
    }

    @Test
    void projection_without_a_concrete_state_type_and_no_store_bean_fails_fast() {
        runner.withUserConfiguration(RawReturnTypeConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("needs a read-model store");
        });
    }

    private static org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> countProjection() {
        return org.occurrent.dsl.projection.Projection.<Integer, TestEvent, String>builder(0)
                .id(event -> "k")
                .on(TestEvent.class, (state, event) -> state + 1)
                .build();
    }

    @Configuration(proxyBeanMethods = false)
    static class ConverterConfiguration {
        @Bean
        CloudEventConverter<TestEvent> cloudEventConverter() {
            return new CloudEventConverter<>() {
                @Override
                public CloudEvent toCloudEvent(TestEvent domainEvent) {
                    return CloudEventBuilder.v1().withId("id").withSource(URI.create("urn:test")).withType("TestEvent").build();
                }

                @Override
                public TestEvent toDomainEvent(CloudEvent cloudEvent) {
                    return new TestEvent();
                }

                @Override
                public String getCloudEventType(Class<? extends TestEvent> type) {
                    return type.getSimpleName();
                }
            };
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class FeedConfiguration {
        @Bean
        DomainEventFeed<TestEvent> domainEventFeed(CloudEventConverter<TestEvent> converter) {
            return new DomainEventFeed<>(mock(PositionOrderedReader.class), converter, event -> "k");
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class WrongFeedConfiguration {
        @Bean
        String wrongFeed() {
            return "not-a-feed";
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PushSynchronousConfiguration {
        @Bean
        PushSynchronousProjection pushSynchronousProjection() {
            return new PushSynchronousProjection();
        }
    }

    static class PushSynchronousProjection {
        @Projection(id = "push-sync", source = Source.PUSH, mode = Mode.SYNCHRONOUS)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return countProjection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PushStartKnobsConfiguration {
        @Bean
        PushStartKnobsProjection pushStartKnobsProjection() {
            return new PushStartKnobsProjection();
        }
    }

    static class PushStartKnobsProjection {
        @Projection(id = "push-knobs", source = Source.PUSH, startAt = StartPosition.BEGINNING)
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return countProjection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PushDcbConfiguration {
        @Bean
        PushDcbProjection pushDcbProjection() {
            return new PushDcbProjection();
        }
    }

    static class PushDcbProjection {
        @Projection(id = "push-dcb", source = Source.PUSH)
        DcbProjection<Integer, TestEvent, String> projection() {
            return new DcbProjection<>(countProjection(), DcbCriteria.all());
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PushWrongFeedConfiguration {
        @Bean
        PushWrongFeedProjection pushWrongFeedProjection() {
            return new PushWrongFeedProjection();
        }
    }

    static class PushWrongFeedProjection {
        @Projection(id = "push-wrong-feed", source = Source.PUSH, subscriptionModelName = "wrongFeed")
        org.occurrent.dsl.projection.Projection<Integer, TestEvent, String> projection() {
            return countProjection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class RawReturnTypeConfiguration {
        @Bean
        RawReturnTypeProjection rawReturnTypeProjection() {
            return new RawReturnTypeProjection();
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    static class RawReturnTypeProjection {
        // Raw return type on purpose: reflectStateType cannot read a concrete state type from it, so with no store
        // bean the convention-based resolution must fail fast instead of using the zero-config default.
        @Projection(id = "raw-return")
        org.occurrent.dsl.projection.Projection projection() {
            return countProjection();
        }
    }

    record TestEvent() {
    }
}
