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
import kotlin.Unit;
import kotlin.jvm.functions.Function2;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.subscription.blocking.Subscriptions;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;

/**
 * A handler method counts whether the bean's class declares it, inherits it from a superclass or gets it as an
 * interface default method (issue #983). The scan registers the most specific method the bean runs, so an override is
 * one handler under its own annotation if it repeats one and under the overridden method's if it does not, and a
 * generic handler is one handler although the compiler adds a bridge method. Reproduced without a running store (no
 * Docker), reading the ids and handlers off a mocked {@link Subscriptions}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class InheritedHandlerRegistrationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
            .withUserConfiguration(InfrastructureConfiguration.class);

    @Test
    void a_handler_declared_on_an_abstract_superclass_registers_for_a_subclass_bean_that_does_not_override_it() {
        runner.withUserConfiguration(InheritingFromSuperclassConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);

            assertThat(subscribedIds(subscriptions)).containsExactly("base-handler");
            handlerFor(subscriptions, "base-handler").invoke(null, new TestEvent());
            assertThat(context.getBean(Invocations.class).bodies()).containsExactly("base-body");
        });
    }

    @Test
    void an_interface_default_method_registers_for_a_bean_that_does_not_override_it() {
        runner.withUserConfiguration(DefaultMethodConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);

            assertThat(subscribedIds(subscriptions)).containsExactly("default-method-handler");
            handlerFor(subscriptions, "default-method-handler").invoke(null, new TestEvent());
            assertThat(context.getBean(Invocations.class).bodies()).containsExactly("default-body");
        });
    }

    @Test
    void an_override_that_drops_the_annotation_registers_once_with_the_overridden_methods_id_and_runs_the_override() {
        runner.withUserConfiguration(OverrideWithoutAnnotationConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);

            assertThat(subscribedIds(subscriptions)).containsExactly("base-handler");
            handlerFor(subscriptions, "base-handler").invoke(null, new TestEvent());
            assertThat(context.getBean(Invocations.class).bodies()).containsExactly("override-body");
        });
    }

    @Test
    void an_override_that_repeats_the_annotation_with_another_id_registers_once_under_the_overrides_id() {
        runner.withUserConfiguration(OverrideWithOtherIdConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);

            assertThat(subscribedIds(subscriptions)).containsExactly("override-handler");
            handlerFor(subscriptions, "override-handler").invoke(null, new TestEvent());
            assertThat(context.getBean(Invocations.class).bodies()).containsExactly("override-body");
        });
    }

    @Test
    void a_generic_superclass_handler_overridden_with_a_concrete_type_registers_once_despite_the_bridge_method() {
        runner.withUserConfiguration(GenericHandlerConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);

            assertThat(subscribedIds(subscriptions)).containsExactly("generic-handler");
            handlerFor(subscriptions, "generic-handler").invoke(null, new TestEvent());
            assertThat(context.getBean(Invocations.class).bodies()).containsExactly("generic-body");
        });
    }

    @Test
    void a_generic_superclass_handler_re_declared_on_the_concrete_override_registers_once_despite_the_bridge_method() {
        runner.withUserConfiguration(GenericRedeclaredHandlerConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);

            assertThat(subscribedIds(subscriptions)).containsExactly("generic-redeclared-handler");
        });
    }

    @Test
    void private_handlers_of_the_same_signature_on_a_superclass_and_its_subclass_are_two_handlers() {
        runner.withUserConfiguration(PrivateSameSignatureConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);

            assertThat(subscribedIds(subscriptions)).containsExactlyInAnyOrder("base-private-handler", "subclass-private-handler");
            handlerFor(subscriptions, "base-private-handler").invoke(null, new TestEvent());
            handlerFor(subscriptions, "subclass-private-handler").invoke(null, new TestEvent());
            assertThat(context.getBean(Invocations.class).bodies()).containsExactlyInAnyOrder("base-private-body", "subclass-private-body");
        });
    }

    @Test
    void two_beans_extending_the_same_annotated_superclass_fail_startup_because_they_share_one_id() {
        runner.withUserConfiguration(TwoBeansOfOneSuperclassConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(DuplicateSubscriptionIdException.class)
                    .hasMessageContaining("base-handler");
        });
    }

    private static List<String> subscribedIds(Subscriptions<?> subscriptions) {
        return mockingDetails(subscriptions).getInvocations().stream()
                .filter(invocation -> invocation.getMethod().getName().equals("subscribe"))
                .map(invocation -> (String) invocation.getRawArguments()[0])
                .toList();
    }

    @SuppressWarnings("unchecked")
    private static Function2<EventMetadata, TestEvent, Unit> handlerFor(Subscriptions<?> subscriptions, String id) {
        return mockingDetails(subscriptions).getInvocations().stream()
                .filter(invocation -> invocation.getMethod().getName().equals("subscribe") && id.equals(invocation.getRawArguments()[0]))
                .map(invocation -> (Function2<EventMetadata, TestEvent, Unit>) invocation.getRawArguments()[invocation.getRawArguments().length - 1])
                .findFirst()
                .orElseThrow(() -> new AssertionError("No handler was subscribed under id " + id));
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class InfrastructureConfiguration {
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
            return new Invocations(new CopyOnWriteArrayList<>());
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class InheritingFromSuperclassConfiguration {
        @Bean
        InheritingBean inheritingBean(Invocations invocations) {
            return new InheritingBean(invocations);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class DefaultMethodConfiguration {
        @Bean
        DefaultMethodBean defaultMethodBean(Invocations invocations) {
            return new DefaultMethodBean(invocations);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class OverrideWithoutAnnotationConfiguration {
        @Bean
        OverridingWithoutAnnotationBean overridingBean(Invocations invocations) {
            return new OverridingWithoutAnnotationBean(invocations);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class OverrideWithOtherIdConfiguration {
        @Bean
        OverridingWithOtherIdBean overridingBean(Invocations invocations) {
            return new OverridingWithOtherIdBean(invocations);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class GenericHandlerConfiguration {
        @Bean
        GenericImplementationBean genericBean(Invocations invocations) {
            return new GenericImplementationBean(invocations);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class GenericRedeclaredHandlerConfiguration {
        @Bean
        GenericRedeclaringBean genericBean(Invocations invocations) {
            return new GenericRedeclaringBean(invocations);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PrivateSameSignatureConfiguration {
        @Bean
        PrivateSubclassBean privateBean(Invocations invocations) {
            return new PrivateSubclassBean(invocations);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class TwoBeansOfOneSuperclassConfiguration {
        @Bean
        InheritingBean first(Invocations invocations) {
            return new InheritingBean(invocations);
        }

        @Bean
        InheritingBean second(Invocations invocations) {
            return new InheritingBean(invocations);
        }
    }

    record Invocations(List<String> bodies) {
    }

    abstract static class BaseHandler {
        final Invocations invocations;

        BaseHandler(Invocations invocations) {
            this.invocations = invocations;
        }

        @Subscription(id = "base-handler")
        public void on(TestEvent event) {
            invocations.bodies().add("base-body");
        }
    }

    static class InheritingBean extends BaseHandler {
        InheritingBean(Invocations invocations) {
            super(invocations);
        }
    }

    interface DefaultMethodHandler {
        Invocations invocations();

        @Subscription(id = "default-method-handler")
        default void on(TestEvent event) {
            invocations().bodies().add("default-body");
        }
    }

    static class DefaultMethodBean implements DefaultMethodHandler {
        private final Invocations invocations;

        DefaultMethodBean(Invocations invocations) {
            this.invocations = invocations;
        }

        @Override
        public Invocations invocations() {
            return invocations;
        }
    }

    static class OverridingWithoutAnnotationBean extends BaseHandler {
        OverridingWithoutAnnotationBean(Invocations invocations) {
            super(invocations);
        }

        @Override
        public void on(TestEvent event) {
            invocations.bodies().add("override-body");
        }
    }

    static class OverridingWithOtherIdBean extends BaseHandler {
        OverridingWithOtherIdBean(Invocations invocations) {
            super(invocations);
        }

        @Override
        @Subscription(id = "override-handler")
        public void on(TestEvent event) {
            invocations.bodies().add("override-body");
        }
    }

    abstract static class GenericBaseHandler<E> {
        final Invocations invocations;

        GenericBaseHandler(Invocations invocations) {
            this.invocations = invocations;
        }

        @Subscription(id = "generic-handler")
        public void on(E event) {
            invocations.bodies().add("generic-body");
        }
    }

    // The override has a different erasure from the superclass method, so the compiler adds a bridge on(Object).
    static class GenericImplementationBean extends GenericBaseHandler<TestEvent> {
        GenericImplementationBean(Invocations invocations) {
            super(invocations);
        }

        @Override
        public void on(TestEvent event) {
            super.on(event);
        }
    }

    abstract static class GenericRedeclaredBaseHandler<E> {
        public void on(E event) {
        }
    }

    // The compiler copies the annotation onto the bridge on(Object) too, so a scan of declared methods sees two.
    static class GenericRedeclaringBean extends GenericRedeclaredBaseHandler<TestEvent> {
        GenericRedeclaringBean(Invocations invocations) {
        }

        @Override
        @Subscription(id = "generic-redeclared-handler")
        public void on(TestEvent event) {
        }
    }

    static class PrivateBaseBean {
        final Invocations invocations;

        PrivateBaseBean(Invocations invocations) {
            this.invocations = invocations;
        }

        @Subscription(id = "base-private-handler")
        private void on(TestEvent event) {
            invocations.bodies().add("base-private-body");
        }
    }

    static class PrivateSubclassBean extends PrivateBaseBean {
        PrivateSubclassBean(Invocations invocations) {
            super(invocations);
        }

        @Subscription(id = "subclass-private-handler")
        private void on(TestEvent event) {
            invocations.bodies().add("subclass-private-body");
        }
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
