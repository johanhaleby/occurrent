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
import kotlin.jvm.functions.Function2;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.StartupMode;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.subscription.reactor.Subscriptions;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * The scan reads a bean's handler methods from the class the container recorded for it, checks their ids for
 * duplicates and refuses a method carrying two annotations, and then resolves the bean again to register them. A
 * prototype whose factory method is free to return a different implementation makes those two classes differ, so
 * registering whatever the resolved instance happens to declare would take in a handler that went through neither
 * check, and its handler key would then make the next pass skip it, so nothing recovers the checks it missed.
 * <p>
 * Registration therefore acts on the methods the scan staged. Reactive counterpart of the blocking
 * {@code StagedSubscriptionMethodRegistrationTest}, reproduced without a running store (no Docker).
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StagedSubscriptionMethodRegistrationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new);

    @Test
    @SuppressWarnings("unchecked")
    void a_prototype_registers_the_handler_the_scan_read_rather_than_the_one_the_instance_it_resolved_declares() {
        runner.withUserConfiguration(SwitchingPrototypeConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<TestEvent> subscriptions = context.getBean(Subscriptions.class);
            verify(subscriptions).subscribe(eq("checked-handler"), any(AgnosticSubscriptionFilter.class), any(), any(Function2.class));
            verify(subscriptions, never()).subscribe(eq("unchecked-handler"), any(AgnosticSubscriptionFilter.class), any(), any(Function2.class));
        });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class SwitchingPrototypeConfiguration {

        private final AtomicInteger instances = new AtomicInteger();

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new NoopCloudEventConverter();
        }

        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        // The first instance is the one the container records the class of, and the scan reads its handler from
        // that recording. Every instance after it is a different class declaring a different id, which is what the
        // scan must not pick up when it resolves the bean to register against.
        @Bean
        @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
        Handler handler() {
            return instances.getAndIncrement() == 0 ? new CheckedHandler() : new UncheckedHandler();
        }

        // Asks for the prototype during startup, so one instance exists before afterSingletonsInstantiated runs.
        @Bean
        HandlerHolder handlerHolder(Handler handler) {
            return new HandlerHolder(handler);
        }
    }

    interface Handler {
        void on(TestEvent event);
    }

    static class CheckedHandler implements Handler {
        @Override
        @Subscription(id = "checked-handler", startupMode = StartupMode.BACKGROUND)
        public void on(TestEvent event) {
        }
    }

    static class UncheckedHandler implements Handler {
        @Override
        @Subscription(id = "unchecked-handler", startupMode = StartupMode.BACKGROUND)
        public void on(TestEvent event) {
        }
    }

    record HandlerHolder(Handler handler) {
    }

    record TestEvent() {
    }

    static class NoopCloudEventConverter implements CloudEventConverter<TestEvent> {
        @Override
        public CloudEvent toCloudEvent(TestEvent domainEvent) {
            return null;
        }

        @Override
        public TestEvent toDomainEvent(CloudEvent cloudEvent) {
            return null;
        }

        @Override
        public String getCloudEventType(Class<? extends TestEvent> type) {
            return type.getSimpleName();
        }
    }
}
