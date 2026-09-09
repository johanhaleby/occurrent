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
import kotlin.jvm.functions.Function2;
import org.aopalliance.intercept.MethodInterceptor;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.DcbSubscription;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.subscription.blocking.Subscriptions;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.SmartFactoryBean;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

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
    // back to the raw bean (issue #836). That fallback is gone, so this now fails fast instead. The bean is created
    // eagerly (no @Lazy), matching how a real interface-proxied singleton exists by the time this scans: the scan
    // itself has to see the annotation through the already-created proxy, not just resolveHandlerInvocation later.
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

    // A JDK interface proxy forwards reflectively using the interface's Method, so Java's own virtual dispatch
    // resolves it against whatever the proxy wraps, a nested CGLIB proxy included, and that inner layer's advice is
    // what silently goes missing. isCglibProxy(bean) alone only sees the outer JDK layer, so this needs the guard to
    // walk the whole chain instead.
    @Test
    void final_handler_method_behind_a_nested_cglib_proxy_fails_fast() {
        runner.withUserConfiguration(NestedFinalHandlerProxyConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(SubscriptionHandlerNotInvocableException.class)
                    .hasMessageContaining("is final");
        });
    }

    // Method.invoke ignores its target for a static method, so it always dispatches on the declaring class alone,
    // proxied or not. Unlike the final-on-CGLIB check, this guard applies whether or not the bean is proxied at all.
    @Test
    void static_handler_method_fails_fast() {
        runner.withUserConfiguration(StaticHandlerConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(NestedExceptionUtils.getMostSpecificCause(context.getStartupFailure()))
                    .isInstanceOf(SubscriptionHandlerNotInvocableException.class)
                    .hasMessageContaining("is static");
        });
    }

    // A final method on a bean nothing proxies has no proxy to lose advice through, so the CGLIB-only reason the
    // check above exists does not apply here, and registration succeeds exactly as it would for a non-final method.
    @Test
    void final_handler_method_on_an_unproxied_bean_registers_normally() {
        runner.withUserConfiguration(FinalHandlerNoProxyConfiguration.class).run(context -> assertThat(context).hasNotFailed());
    }

    // containsSingleton(beanName) is true once a FactoryBean itself exists, whether or not its product does, and
    // getBean(beanName) would create that product. isEagerInit() == false (SmartFactoryBean's default) keeps Spring
    // itself from creating it during startup, so the scan must not create it either just to read its class.
    @Test
    void a_factory_beans_product_is_not_forced_by_the_scan() {
        runner.withUserConfiguration(FactoryBeanConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean("&lazyProductFactory", NeverEagerFactoryBean.class).productCreated()).isFalse();
        });
    }

    // Spring CGLIB-enhances a proxyBeanMethods = true @Configuration class to intercept its own @Bean factory method
    // calls, and that enhancement is not an Advised proxy, so SubscriptionAnnotations.ultimateTarget leaves it
    // untouched. CGLIB does not copy a method's annotations onto the override it generates, so scanning the enhanced
    // subclass instead of the user class it enhances would miss a subscription handler method declared directly on
    // the configuration class.
    @Test
    void handler_method_on_a_configuration_enhanced_class_registers_normally() {
        runner.withUserConfiguration(ConfigurationEnhancedHandlerConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            verify(context.getBean(Subscriptions.class))
                    .subscribe(eq("configuration-enhanced-handler"), any(AgnosticSubscriptionFilter.class), any(), anyBoolean(), any(Function2.class));
        });
    }

    @Configuration(proxyBeanMethods = true)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class ConfigurationEnhancedHandlerConfiguration {
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new NoopCloudEventConverter();
        }

        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        @Subscription(id = "configuration-enhanced-handler")
        void on(TestEvent event) {
        }
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

    @Configuration(proxyBeanMethods = false)
    static class NestedFinalHandlerProxyConfiguration {
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new NoopCloudEventConverter();
        }

        @Bean
        NestedFinalHandlerSubscriber nestedFinalHandlerSubscriber() {
            return new NestedFinalHandlerSubscriber();
        }

        // Wraps the raw bean in an inner CGLIB proxy first, then an outer JDK interface proxy around that, the
        // shape that lets a JDK proxy forward reflectively into a nested CGLIB proxy's inherited final method.
        @Bean
        static BeanPostProcessor nestedProxyPostProcessor() {
            return new BeanPostProcessor() {
                @Override
                public Object postProcessAfterInitialization(Object bean, String beanName) {
                    if (!(bean instanceof NestedFinalHandlerMarker)) {
                        return bean;
                    }
                    ProxyFactory innerCglibProxy = new ProxyFactory();
                    innerCglibProxy.setTarget(bean);
                    innerCglibProxy.setProxyTargetClass(true);
                    innerCglibProxy.addAdvice((MethodInterceptor) invocation -> invocation.proceed());
                    Object cglibProxy = innerCglibProxy.getProxy();

                    ProxyFactory outerJdkProxy = new ProxyFactory();
                    outerJdkProxy.setTarget(cglibProxy);
                    outerJdkProxy.setInterfaces(NestedFinalHandlerMarker.class);
                    outerJdkProxy.setProxyTargetClass(false);
                    outerJdkProxy.addAdvice((MethodInterceptor) invocation -> invocation.proceed());
                    return outerJdkProxy.getProxy();
                }
            };
        }
    }

    interface NestedFinalHandlerMarker {
        void on(TestEvent event);
    }

    static class NestedFinalHandlerSubscriber implements NestedFinalHandlerMarker {
        @Subscription(id = "nested-final-handler-guard")
        public final void on(TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class StaticHandlerConfiguration {
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new NoopCloudEventConverter();
        }

        @Bean
        StaticHandlerSubscriber staticHandlerSubscriber() {
            return new StaticHandlerSubscriber();
        }
    }

    static class StaticHandlerSubscriber {
        @Subscription(id = "static-handler-guard")
        static void on(TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class FinalHandlerNoProxyConfiguration {
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new NoopCloudEventConverter();
        }

        // Registration reaches this bean once resolveHandlerInvocation lets the final method through, so a mock is
        // enough to let the whole path complete without a real subscription model.
        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        @Bean
        FinalHandlerNoProxySubscriber finalHandlerNoProxySubscriber() {
            return new FinalHandlerNoProxySubscriber();
        }
    }

    static class FinalHandlerNoProxySubscriber {
        @Subscription(id = "final-handler-no-proxy-guard")
        final void on(TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class FactoryBeanConfiguration {
        @Bean
        NeverEagerFactoryBean lazyProductFactory() {
            return new NeverEagerFactoryBean();
        }
    }

    static class NeverEagerFactoryBean implements SmartFactoryBean<Object> {
        private boolean productCreated;

        @Override
        public Object getObject() {
            productCreated = true;
            return new Object();
        }

        @Override
        public Class<?> getObjectType() {
            return Object.class;
        }

        boolean productCreated() {
            return productCreated;
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
