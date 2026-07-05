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

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.DcbSubscription;
import org.occurrent.annotation.DcbSubscription.DcbStartPosition;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.dcb.DcbEventMetadata;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A {@code @DcbSubscription} whose {@link DcbSubscription#tags() tags} carries a value that is not in
 * {@code key:value} form must fail fast at startup, and the failure must name the subscription id and the offending
 * tag so it is diagnosable. The tag is parsed in {@code OccurrentBlockingAnnotationBeanPostProcessor} before the
 * subscription model is ever consulted, so this reproduces the failure without a running store (no Docker).
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbSubscriptionMalformedTagAnnotationTest {

    private static final String SUBSCRIPTION_ID = "malformed-tag-subscription";

    @Test
    void malformed_tags_fails_fast_at_startup_naming_the_subscription_id_and_the_tag() {
        new ApplicationContextRunner()
                .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
                .withUserConfiguration(ConverterConfiguration.class, MalformedTagSubscriberConfiguration.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    // The bean-creation failure wraps the post-processor's IllegalArgumentException, which names the
                    // subscription id and the offending tag; its own cause is the raw Tag.parse rejection.
                    assertThat(context.getStartupFailure())
                            .cause()
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining(SUBSCRIPTION_ID)
                            .hasMessageContaining("nope")
                            .hasMessageContaining("key:value");
                });
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
    static class MalformedTagSubscriberConfiguration {
        @Bean
        MalformedTagSubscriber malformedTagSubscriber() {
            return new MalformedTagSubscriber();
        }
    }

    static class MalformedTagSubscriber {
        @DcbSubscription(id = SUBSCRIPTION_ID, eventTypes = TestEvent.class, tags = "nope", startAt = DcbStartPosition.NOW)
        void on(TestEvent event, DcbEventMetadata metadata) {
        }
    }

    record TestEvent() {
    }
}
