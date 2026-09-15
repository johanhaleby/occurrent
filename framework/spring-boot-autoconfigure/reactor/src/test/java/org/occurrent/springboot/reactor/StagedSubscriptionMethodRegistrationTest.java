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
import org.mockito.ArgumentCaptor;
import org.occurrent.annotation.StartupMode;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.subscription.reactor.Subscriptions;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.core.NestedExceptionUtils;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * The scan reads a bean's handler methods from the class the container recorded for it, checks their ids for
 * duplicates and refuses a method carrying two annotations, and then resolves the bean again to register them. A
 * prototype whose factory method is free to return a different implementation makes those two classes differ, so
 * registering whatever the resolved instance happens to declare would take in a handler that went through neither
 * check, and its handler key would then make the next pass skip it, so nothing recovers the checks it missed.
 * <p>
 * Registration therefore acts on the methods the scan staged, and only on an instance that runs them. A subclass
 * inheriting the staged method runs the same code, so it registers. An unrelated class sharing an interface with the
 * recorded one, or a subclass overriding the method, would run its own code under the staged method's id, so it is
 * refused. Reactive counterpart of the blocking {@code StagedSubscriptionMethodRegistrationTest}, reproduced
 * without a running store (no Docker).
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StagedSubscriptionMethodRegistrationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new);

    @Test
    @SuppressWarnings("unchecked")
    void a_prototype_registers_the_handler_the_scan_read_and_runs_it_on_a_subclass_that_inherits_it() {
        runner.withUserConfiguration(InheritingPrototypeConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<TestEvent> subscriptions = context.getBean(Subscriptions.class);
            ArgumentCaptor<Function2<EventMetadata, TestEvent, Mono<Void>>> handler = ArgumentCaptor.forClass(Function2.class);
            verify(subscriptions).subscribe(eq("checked-handler"), any(AgnosticSubscriptionFilter.class), any(), handler.capture());

            handler.getValue().invoke(null, new TestEvent()).block();

            assertThat(context.getBean(Invocations.class).bodies()).containsExactly("checked-body");
        });
    }

    @Test
    void a_prototype_that_builds_an_unrelated_implementation_is_refused() {
        runner.withUserConfiguration(UnrelatedPrototypeConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(SubscriptionHandlerNotInvocableException.class)
                    .hasMessageContaining(UncheckedHandler.class.getName());
        });
    }

    @Test
    void a_prototype_that_builds_a_subclass_overriding_the_handler_is_refused() {
        runner.withUserConfiguration(OverridingPrototypeConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(SubscriptionHandlerNotInvocableException.class)
                    .hasMessageContaining(OverridingHandler.class.getName());
        });
    }

    // The first instance is the one the container records the class of, and the scan reads its handler from that
    // recording. Every instance after it is built by laterInstances, which is what the scan must not pick up when it
    // resolves the bean to register against.
    abstract static class SwitchingPrototypeConfiguration {

        private final AtomicInteger instances = new AtomicInteger();
        private final Invocations invocations = new Invocations(new CopyOnWriteArrayList<>());
        private final Function<Invocations, Handler> laterInstances;

        SwitchingPrototypeConfiguration(Function<Invocations, Handler> laterInstances) {
            this.laterInstances = laterInstances;
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new NoopCloudEventConverter();
        }

        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        @Bean
        Invocations invocations() {
            return invocations;
        }

        @Bean
        @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
        Handler handler() {
            return instances.getAndIncrement() == 0 ? new CheckedHandler(invocations) : laterInstances.apply(invocations);
        }

        // Asks for the prototype during startup, so one instance exists before afterSingletonsInstantiated runs.
        @Bean
        HandlerHolder handlerHolder(Handler handler) {
            return new HandlerHolder(handler);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class InheritingPrototypeConfiguration extends SwitchingPrototypeConfiguration {
        InheritingPrototypeConfiguration() {
            super(InheritingHandler::new);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class UnrelatedPrototypeConfiguration extends SwitchingPrototypeConfiguration {
        UnrelatedPrototypeConfiguration() {
            super(UncheckedHandler::new);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class OverridingPrototypeConfiguration extends SwitchingPrototypeConfiguration {
        OverridingPrototypeConfiguration() {
            super(OverridingHandler::new);
        }
    }

    record Invocations(List<String> bodies) {
    }

    interface Handler {
        void on(TestEvent event);
    }

    static class CheckedHandler implements Handler {
        final Invocations invocations;

        CheckedHandler(Invocations invocations) {
            this.invocations = invocations;
        }

        @Override
        @Subscription(id = "checked-handler", startupMode = StartupMode.BACKGROUND)
        public void on(TestEvent event) {
            invocations.bodies().add("checked-body");
        }
    }

    // Inherits the staged handler unchanged, so invoking it runs the same code.
    static class InheritingHandler extends CheckedHandler {
        InheritingHandler(Invocations invocations) {
            super(invocations);
        }
    }

    static class OverridingHandler extends CheckedHandler {
        OverridingHandler(Invocations invocations) {
            super(invocations);
        }

        @Override
        public void on(TestEvent event) {
            invocations.bodies().add("overriding-body");
        }
    }

    static class UncheckedHandler implements Handler {
        private final Invocations invocations;

        UncheckedHandler(Invocations invocations) {
            this.invocations = invocations;
        }

        @Override
        @Subscription(id = "unchecked-handler", startupMode = StartupMode.BACKGROUND)
        public void on(TestEvent event) {
            invocations.bodies().add("unchecked-body");
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
