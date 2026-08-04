/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.springboot.blocking;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.Saga;
import org.occurrent.annotation.Source;
import org.occurrent.annotation.StartPosition;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;

import java.net.URI;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code @Saga(source = PUSH)} guards that fail before any store or feed is driven, so they reproduce without
 * Docker: the start-position attributes a push saga cannot honour, and the three ways resolving the feed bean can go
 * wrong (none declared, several declared, and one of the wrong type). The projection side has no test for the last two,
 * so these are the first.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaAnnotationPushValidationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
            .withUserConfiguration(ConverterConfiguration.class);

    @Test
    void a_push_saga_that_sets_a_start_position_fails_fast_saying_it_always_replays_from_the_beginning() {
        runner.withUserConfiguration(PushModelConfiguration.class, StartAtConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .hasMessageContaining("cannot set startAt, startAtGlobalPosition or resumeBehavior")
                    .hasMessageContaining("always from the beginning");
        });
    }

    @Test
    void a_push_saga_that_sets_a_resume_behavior_is_rejected_the_same_way() {
        runner.withUserConfiguration(PushModelConfiguration.class, ResumeBehaviorConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .hasMessageContaining("cannot set startAt, startAtGlobalPosition or resumeBehavior");
        });
    }

    @Test
    void a_push_saga_with_no_push_model_bean_says_to_declare_one() {
        runner.withUserConfiguration(PlainPushConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .hasMessageContaining("@Saga 'push-saga' with source=PUSH found no PushSubscriptionModel bean")
                    .hasMessageContaining("Declare one, or name it with subscriptionModelName");
        });
    }

    @Test
    void a_push_saga_with_several_push_model_beans_says_to_pick_one() {
        runner.withUserConfiguration(TwoPushModelsConfiguration.class, PlainPushConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .hasMessageContaining("found several push feed beans")
                    .hasMessageContaining("Pick one with subscriptionModel or subscriptionModelName");
        });
    }

    @Test
    void a_push_saga_that_would_catch_up_with_no_event_store_to_read_is_pointed_at_catchup_none() {
        runner.withUserConfiguration(PushModelConfiguration.class, PlainPushConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .hasMessageContaining("needs a PositionOrderedReader bean, and there is none")
                    .hasMessageContaining("Set catchup = NONE");
        });
    }

    @Test
    void a_push_saga_pointed_at_a_bean_that_is_not_a_push_model_names_the_type_it_needs() {
        runner.withUserConfiguration(WrongTypeConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .hasMessageContaining("must be a PushSubscriptionModel for source=PUSH");
        });
    }

    // --- Fixtures ---

    record TestEvent(String id) {
    }

    record TestCommand(String id) {
    }

    private static org.occurrent.dsl.saga.Saga<TestEvent, String, TestCommand> testSaga() {
        return org.occurrent.dsl.saga.Saga.<TestEvent, String, TestCommand>builder(null)
                .correlateAll(TestEvent::id)
                .startsOn(TestEvent.class)
                .evolve(TestEvent.class, (state, event) -> event.id())
                .react(TestEvent.class, (state, event) -> List.of(SagaEffect.issue(new TestCommand(event.id()))))
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
                    return new TestEvent("id");
                }

                @Override
                public String getCloudEventType(Class<? extends TestEvent> type) {
                    return type.getSimpleName();
                }
            };
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PushModelConfiguration {
        @Bean
        PushSubscriptionModel pushModel() {
            return new PushSubscriptionModel();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class TwoPushModelsConfiguration {
        @Bean
        PushSubscriptionModel firstPushModel() {
            return new PushSubscriptionModel();
        }

        @Bean
        PushSubscriptionModel secondPushModel() {
            return new PushSubscriptionModel();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PlainPushConfiguration {
        @Bean
        PlainPushSaga plainPushSaga() {
            return new PlainPushSaga();
        }
    }

    static class PlainPushSaga {
        @Saga(id = "push-saga", source = Source.PUSH)
        org.occurrent.dsl.saga.Saga<TestEvent, String, TestCommand> saga() {
            return testSaga();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class StartAtConfiguration {
        @Bean
        StartAtSaga startAtSaga() {
            return new StartAtSaga();
        }
    }

    static class StartAtSaga {
        @Saga(id = "push-start-at", source = Source.PUSH, startAt = StartPosition.BEGINNING)
        org.occurrent.dsl.saga.Saga<TestEvent, String, TestCommand> saga() {
            return testSaga();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ResumeBehaviorConfiguration {
        @Bean
        ResumeBehaviorSaga resumeBehaviorSaga() {
            return new ResumeBehaviorSaga();
        }
    }

    static class ResumeBehaviorSaga {
        @Saga(id = "push-resume", source = Source.PUSH, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        org.occurrent.dsl.saga.Saga<TestEvent, String, TestCommand> saga() {
            return testSaga();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class WrongTypeConfiguration {
        @Bean
        WrongTypeSaga wrongTypeSaga() {
            return new WrongTypeSaga();
        }
    }

    static class WrongTypeSaga {
        @Saga(id = "push-wrong-type", source = Source.PUSH, subscriptionModel = String.class)
        org.occurrent.dsl.saga.Saga<TestEvent, String, TestCommand> saga() {
            return testSaga();
        }
    }
}
