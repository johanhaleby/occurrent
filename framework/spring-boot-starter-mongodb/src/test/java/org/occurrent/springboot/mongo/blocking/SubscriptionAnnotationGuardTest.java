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

package org.occurrent.springboot.mongo.blocking;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.DcbSubscription;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.annotation.Subscription;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Characterizes the eager, per-bean validation the blocking annotation post-processor performs in
 * {@code postProcessBeforeInitialization}, before any subscription model or store is consulted, so it reproduces
 * without a running MongoDB (no Docker): a method carrying more than one subscription annotation is rejected, and a
 * {@code @DcbSubscription} without an event parameter is rejected. Both must fail fast at context startup with the
 * exact user-facing message.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SubscriptionAnnotationGuardTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new);

    @Test
    void method_annotated_with_more_than_one_subscription_annotation_fails_fast() {
        runner.withUserConfiguration(MultipleAnnotationsConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("annotated with more than one of @Subscription, @StreamSubscription, @DcbSubscription and @SynchronousSubscription, use only one.");
        });
    }

    @Test
    void dcb_subscription_without_an_event_parameter_fails_fast() {
        runner.withUserConfiguration(DcbNoEventParameterConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("A @DcbSubscription method must declare an event parameter, but");
        });
    }

    @Configuration(proxyBeanMethods = false)
    static class MultipleAnnotationsConfiguration {
        @Bean
        MultiplyAnnotatedSubscriber multiplyAnnotatedSubscriber() {
            return new MultiplyAnnotatedSubscriber();
        }
    }

    static class MultiplyAnnotatedSubscriber {
        @Subscription(id = "a")
        @StreamSubscription(id = "b")
        void on(TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class DcbNoEventParameterConfiguration {
        @Bean
        DcbNoEventParameterSubscriber dcbNoEventParameterSubscriber() {
            return new DcbNoEventParameterSubscriber();
        }
    }

    static class DcbNoEventParameterSubscriber {
        @DcbSubscription(id = "dcb-no-event", eventTypes = TestEvent.class)
        void on() {
        }
    }

    record TestEvent() {
    }
}
