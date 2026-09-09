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
import org.aopalliance.intercept.MethodInterceptor;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.DcbSubscription;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Characterizes the eager, per-bean validation the blocking annotation post-processor performs from
 * {@code afterSingletonsInstantiated}, before any subscription model or store is consulted, so it reproduces without
 * a running store (no Docker): a method carrying more than one subscription annotation is rejected, a
 * {@code @DcbSubscription} without an event parameter is rejected, and a handler method the bean's Spring proxy
 * cannot invoke is rejected rather than silently run unadvised on the raw bean. All four must fail fast at context
 * startup with the exact user-facing message.
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

    // A JDK dynamic proxy implementing only Marker can never carry the handler method declared on the concrete
    // class, the same mismatch StreamSubscriptionAnnotationJdkInterfaceProxyMongoTest used to paper over by falling
    // back to the raw bean (issue #836). That fallback is gone, so this now fails fast instead.
    @Test
    void handler_method_not_reachable_through_a_JDK_interface_proxy_fails_fast() {
        runner.withUserConfiguration(JdkInterfaceProxyConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(SubscriptionHandlerNotInvocableException.class)
                    .hasMessageContaining("does not implement it");
        });
    }

    // A CGLIB proxy never overrides a private method, and AopUtils.selectInvocableMethod refuses to resolve one
    // against such a proxy the same way it refuses a JDK interface proxy missing the method entirely.
    @Test
    void private_handler_method_on_a_CGLIB_proxy_fails_fast() {
        runner.withUserConfiguration(PrivateHandlerCglibProxyConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(SubscriptionHandlerNotInvocableException.class);
        });
    }

    // A CGLIB proxy never overrides a final method either, but unlike the private-method case,
    // AopUtils.selectInvocableMethod resolves it without complaint, so the final check is a separate guard.
    @Test
    void final_handler_method_on_a_CGLIB_proxy_fails_fast() {
        runner.withUserConfiguration(FinalHandlerCglibProxyConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(SubscriptionHandlerNotInvocableException.class)
                    .hasMessageContaining("is final");
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

    @Configuration(proxyBeanMethods = false)
    static class JdkInterfaceProxyConfiguration {
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new NoopCloudEventConverter();
        }

        @Bean
        MarkerSubscriber markerSubscriber() {
            return new MarkerSubscriber();
        }

        // Wraps the Marker-implementing subscriber in a genuine JDK interface proxy after the handler Method has
        // already been captured from the concrete class, the same shape a real advisor leaves an interface-proxied
        // bean in under spring.aop.proxy-target-class=false.
        @Bean
        static BeanPostProcessor jdkInterfaceProxyPostProcessor() {
            return new BeanPostProcessor() {
                @Override
                public Object postProcessAfterInitialization(Object bean, String beanName) {
                    if (!(bean instanceof Marker)) {
                        return bean;
                    }
                    ProxyFactory proxyFactory = new ProxyFactory();
                    proxyFactory.setTarget(bean);
                    proxyFactory.setInterfaces(Marker.class);
                    proxyFactory.setProxyTargetClass(false);
                    proxyFactory.addAdvice((MethodInterceptor) invocation -> invocation.proceed());
                    return proxyFactory.getProxy();
                }
            };
        }
    }

    interface Marker {
    }

    static class MarkerSubscriber implements Marker {
        @Subscription(id = "jdk-interface-proxy-guard")
        void on(TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PrivateHandlerCglibProxyConfiguration {
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new NoopCloudEventConverter();
        }

        @Bean
        PrivateHandlerSubscriber privateHandlerSubscriber() {
            return new PrivateHandlerSubscriber();
        }

        @Bean
        static BeanPostProcessor cglibProxyPostProcessor() {
            return new BeanPostProcessor() {
                @Override
                public Object postProcessAfterInitialization(Object bean, String beanName) {
                    if (!(bean instanceof PrivateHandlerSubscriber)) {
                        return bean;
                    }
                    ProxyFactory proxyFactory = new ProxyFactory();
                    proxyFactory.setTarget(bean);
                    proxyFactory.setProxyTargetClass(true);
                    proxyFactory.addAdvice((MethodInterceptor) invocation -> invocation.proceed());
                    return proxyFactory.getProxy();
                }
            };
        }
    }

    static class PrivateHandlerSubscriber {
        @Subscription(id = "private-handler-cglib-guard")
        private void on(TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class FinalHandlerCglibProxyConfiguration {
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new NoopCloudEventConverter();
        }

        @Bean
        FinalHandlerSubscriber finalHandlerSubscriber() {
            return new FinalHandlerSubscriber();
        }

        @Bean
        static BeanPostProcessor cglibProxyPostProcessor() {
            return new BeanPostProcessor() {
                @Override
                public Object postProcessAfterInitialization(Object bean, String beanName) {
                    if (!(bean instanceof FinalHandlerSubscriber)) {
                        return bean;
                    }
                    ProxyFactory proxyFactory = new ProxyFactory();
                    proxyFactory.setTarget(bean);
                    proxyFactory.setProxyTargetClass(true);
                    proxyFactory.addAdvice((MethodInterceptor) invocation -> invocation.proceed());
                    return proxyFactory.getProxy();
                }
            };
        }
    }

    static class FinalHandlerSubscriber {
        @Subscription(id = "final-handler-cglib-guard")
        final void on(TestEvent event) {
        }
    }

    record TestEvent() {
    }

    // resolveTypeFilter needs a CloudEventConverter to build the type filter before resolveHandlerInvocation ever
    // runs. Nothing here reads a converted event, so every method is a stub.
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
