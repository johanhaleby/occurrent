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
import org.aopalliance.intercept.MethodInterceptor;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.StartupMode;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.subscription.reactor.Subscriptions;
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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Reactive counterpart of the blocking {@code SubscriptionAnnotationGuardTest}: a handler method the bean's Spring
 * proxy cannot invoke is rejected from {@code afterSingletonsInstantiated} rather than silently run unadvised on the
 * raw bean, reproduced without a running store (no Docker). Subscription scanning and registration run
 * unconditionally there, ahead of the coordinator's own {@code Subscribable}-presence check, so no such bean is
 * needed here for a subscription to reach {@code resolveHandlerInvocation}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SubscriptionAnnotationGuardTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new);

    // A JDK dynamic proxy implementing only Marker can never carry the handler method declared on the concrete
    // class, the same mismatch ReactiveStreamSubscriptionAnnotationJdkInterfaceProxyMongoTest used to paper over by
    // falling back to the raw bean (issue #836). That fallback is gone, so this now fails fast instead.
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
                    .subscribe(eq("reactive-configuration-enhanced-handler"), any(AgnosticSubscriptionFilter.class), any(), any(Function2.class));
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

        @Subscription(id = "reactive-configuration-enhanced-handler", startupMode = StartupMode.BACKGROUND)
        void on(TestEvent event) {
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
        @Subscription(id = "reactive-jdk-interface-proxy-guard")
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
        @Subscription(id = "reactive-private-handler-cglib-guard")
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
        @Subscription(id = "reactive-final-handler-cglib-guard")
        final void on(TestEvent event) {
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
        // enough to let the whole path complete without a real subscription model. startupMode = BACKGROUND keeps
        // the registrar from calling waitUntilStarted() on whatever this mock's unstubbed subscribe(...) returns.
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
        @Subscription(id = "reactive-final-handler-no-proxy-guard", startupMode = StartupMode.BACKGROUND)
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
