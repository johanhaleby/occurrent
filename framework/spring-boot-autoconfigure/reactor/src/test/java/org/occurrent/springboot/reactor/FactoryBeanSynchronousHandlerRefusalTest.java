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
package org.occurrent.springboot.reactor;

import java.lang.reflect.Proxy;
import org.springframework.beans.factory.BeanCreationException;
import org.occurrent.annotation.Mode;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.Snapshot;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import kotlin.jvm.functions.Function2;
import org.occurrent.annotation.Subscription;
import org.occurrent.annotation.SynchronousSubscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.subscription.reactor.Subscriptions;
import org.occurrent.springboot.common.OccurrentProperties;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * An object the starter handles as a FactoryBean registers no handler, so one whose class declares a synchronous handler
 * fails its bean's creation rather than miss the writes the application service admits.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class FactoryBeanSynchronousHandlerRefusalTest {

    private static volatile boolean recreatedFactoryAsksForItself;

    @BeforeEach
    void reset() {
        recreatedFactoryAsksForItself = false;
    }

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
            .withUserConfiguration(RefusalConfiguration.class);

    @Test
    void a_product_that_implements_FactoryBean_and_declares_a_synchronous_handler_fails_when_its_factory_has_no_bean_definition() {
        runner.withInitializer(context -> ((GenericApplicationContext) context).getBeanFactory().registerSingleton("registeredFactory", new ProductFactory()))
                .run(context -> {
                    assertThat(context).hasNotFailed();

                    assertThatThrownBy(() -> context.getBean("registeredFactory"))
                            .rootCause()
                            .hasMessageContaining(SynchronousFactoryProduct.class.getName() + " implements FactoryBean and declares the synchronous handler onEvent, which isn't registered.");
                });
    }

    @Test
    void a_product_that_implements_FactoryBean_and_declares_a_synchronous_handler_fails_when_its_factory_asks_for_itself_during_its_own_initialization() {
        runner.withInitializer(context -> ((GenericApplicationContext) context).setAllowCircularReferences(true))
                .withBean("selfAskingFactory", SelfAskingFactory.class, SelfAskingFactory::new)
                .run(context -> {
                    assertThat(context).hasFailed();

                    assertThat(context.getStartupFailure())
                            .rootCause()
                            .hasMessageContaining(SynchronousFactoryProduct.class.getName() + " implements FactoryBean and declares the synchronous handler onEvent, which isn't registered.");
                });
    }

    @Test
    void a_FactoryBean_class_that_declares_a_synchronous_handler_fails_its_creation() {
        runner.withBean("handlerFactory", HandlerDeclaringFactory.class, HandlerDeclaringFactory::new)
                .run(context -> {
                    assertThat(context).hasFailed();

                    assertThat(context.getStartupFailure())
                            .hasMessageContaining(HandlerDeclaringFactory.class.getName() + " implements FactoryBean and declares the synchronous handler onEvent, which isn't registered.");
                });
    }

    @Test
    void a_factory_that_declares_no_handler_creates_a_product_that_declares_one() {
        runner.withBean("plainFactory", PlainProductFactory.class, PlainProductFactory::new)
                .run(context -> {
                    assertThat(context).hasNotFailed();

                    assertThat(context.getBean("plainFactory")).isInstanceOf(SynchronousProduct.class);
                });
    }

    @Test
    void a_product_whose_handler_is_already_registered_creates_without_refusal_when_its_factory_asks_for_itself_during_a_recreation() {
        runner.withInitializer(context -> ((GenericApplicationContext) context).setAllowCircularReferences(true))
                .withBean("registeredProductFactory", RegisteredProductFactory.class, RegisteredProductFactory::new)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    context.getBean("registeredProductFactory");
                    verify(context.getBean(OccurrentReactorBeanNames.SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME, Subscriptions.class))
                            .subscribe(eq("registered-product-handler"), any(AgnosticSubscriptionFilter.class), any(), any(Function2.class));

                    ((DefaultListableBeanFactory) context.getBeanFactory()).destroySingleton("registeredProductFactory");
                    recreatedFactoryAsksForItself = true;

                    assertThatCode(() -> context.getBean("registeredProductFactory")).doesNotThrowAnyException();
                });
    }

    @Test
    void a_FactoryBean_whose_handlers_cant_be_read_fails_its_creation() {
        FactoryBean<Object> factory = conflictingHandlerFactory();

        runner.withBean("unreadableFactory", FactoryBean.class, () -> factory)
                .run(context -> {
                    assertThat(context).hasFailed();

                    assertThat(context.getStartupFailure())
                            .isInstanceOf(BeanCreationException.class)
                            .hasMessageContaining("Occurrent can't read the handlers of")
                            .hasMessageContaining("which implements FactoryBean")
                            .hasCauseInstanceOf(IllegalArgumentException.class);
                });
    }

    @Test
    void a_product_that_declares_a_synchronous_handler_fails_when_its_factory_has_no_bean_definition() {
        runner.withInitializer(context -> ((GenericApplicationContext) context).getBeanFactory().registerSingleton("registeredPlainFactory", new PlainProductFactory()))
                .run(context -> {
                    assertThat(context).hasNotFailed();

                    assertThatThrownBy(() -> context.getBean("registeredPlainFactory"))
                            .rootCause()
                            .hasMessageContaining(SynchronousProduct.class.getName() + " declares the synchronous handler onEvent, which isn't registered. Bean 'registeredPlainFactory' has no bean definition");
                });
    }

    @Test
    void an_object_that_declares_a_synchronous_handler_fails_when_initialized_under_a_name_without_a_bean_definition() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();

            assertThatThrownBy(() -> context.getAutowireCapableBeanFactory().initializeBean(new SynchronousProduct(), "someUndefinedName"))
                    .isInstanceOf(BeanCreationException.class)
                    .hasMessageContaining(SynchronousProduct.class.getName() + " declares the synchronous handler onEvent, which isn't registered. Bean 'someUndefinedName' has no bean definition");
        });
    }

    @Test
    void an_object_that_declares_no_handler_is_initialized_under_a_name_without_a_bean_definition() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();

            assertThatCode(() -> context.getAutowireCapableBeanFactory().initializeBean(new Object(), "someUndefinedName"))
                    .doesNotThrowAnyException();
        });
    }

    @Test
    void an_object_that_declares_only_an_asynchronous_handler_is_initialized_under_a_name_without_a_bean_definition() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();

            assertThatCode(() -> context.getAutowireCapableBeanFactory().initializeBean(new AsynchronousOnlyObject(), "someUndefinedName"))
                    .doesNotThrowAnyException();
        });
    }

    @SuppressWarnings("unchecked")
    private static FactoryBean<Object> conflictingHandlerFactory() {
        return (FactoryBean<Object>) Proxy.newProxyInstance(FactoryBeanSynchronousHandlerRefusalTest.class.getClassLoader(),
                new Class<?>[]{FactoryBean.class, FirstConflictingHandler.class, SecondConflictingHandler.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getObject" -> new Object();
                    case "getObjectType" -> Object.class;
                    case "isSingleton" -> true;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    interface FirstConflictingHandler {
        @SynchronousSubscription(id = "first-conflicting-handler")
        void onEvent(LateBeanAnnotationRegistrationTest.TestEvent event);
    }

    interface SecondConflictingHandler {
        @SynchronousSubscription(id = "second-conflicting-handler")
        void onEvent(LateBeanAnnotationRegistrationTest.TestEvent event);
    }

    @Test
    void a_FactoryBean_class_that_declares_a_synchronous_projection_starts_when_no_Subscribable_bean_exists() {
        runner.withBean("projectionFactory", ProjectionDeclaringFactory.class, ProjectionDeclaringFactory::new)
                .run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void a_FactoryBean_class_that_declares_a_synchronous_snapshot_starts_when_no_Subscribable_bean_exists() {
        runner.withBean("snapshotFactory", SnapshotDeclaringFactory.class, SnapshotDeclaringFactory::new)
                .run(context -> assertThat(context).hasNotFailed());
    }

    static class ProjectionDeclaringFactory implements FactoryBean<Object> {
        @Projection(id = "factory-synchronous-projection", mode = Mode.SYNCHRONOUS)
        void onEvent(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }

        @Override
        public Object getObject() {
            return new Object();
        }

        @Override
        public Class<?> getObjectType() {
            return Object.class;
        }
    }

    static class SnapshotDeclaringFactory implements FactoryBean<Object> {
        @Snapshot(id = "factory-synchronous-snapshot", mode = Mode.SYNCHRONOUS)
        void onEvent(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }

        @Override
        public Object getObject() {
            return new Object();
        }

        @Override
        public Class<?> getObjectType() {
            return Object.class;
        }
    }

    static class RegisteredFactoryProduct implements FactoryBean<Object> {
        @SynchronousSubscription(id = "registered-product-handler")
        void onEvent(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }

        @Override
        public Object getObject() {
            return new Object();
        }

        @Override
        public Class<?> getObjectType() {
            return Object.class;
        }
    }

    // Asks for itself only once it has been destroyed and created again
    static class RegisteredProductFactory implements FactoryBean<RegisteredFactoryProduct>, BeanFactoryAware, InitializingBean {
        private BeanFactory beanFactory;

        @Override
        public void setBeanFactory(BeanFactory beanFactory) {
            this.beanFactory = beanFactory;
        }

        @Override
        public void afterPropertiesSet() {
            if (recreatedFactoryAsksForItself) {
                beanFactory.getBean("registeredProductFactory");
            }
        }

        @Override
        public RegisteredFactoryProduct getObject() {
            return new RegisteredFactoryProduct();
        }

        @Override
        public Class<?> getObjectType() {
            return RegisteredFactoryProduct.class;
        }
    }

    static class SynchronousFactoryProduct implements FactoryBean<Object> {
        @SynchronousSubscription(id = "factory-product-synchronous-handler")
        void onEvent(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }

        @Override
        public Object getObject() {
            return new Object();
        }

        @Override
        public Class<?> getObjectType() {
            return Object.class;
        }
    }

    static class AsynchronousOnlyObject {
        @Subscription(id = "asynchronous-only-handler")
        void onEvent(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }
    }

    static class SynchronousProduct {
        @SynchronousSubscription(id = "plain-product-synchronous-handler")
        void onEvent(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }
    }

    static class ProductFactory implements FactoryBean<Object> {
        @Override
        public Object getObject() {
            return new SynchronousFactoryProduct();
        }

        @Override
        public Class<?> getObjectType() {
            return Object.class;
        }
    }

    static class PlainProductFactory implements FactoryBean<SynchronousProduct> {
        @Override
        public SynchronousProduct getObject() {
            return new SynchronousProduct();
        }

        @Override
        public Class<?> getObjectType() {
            return SynchronousProduct.class;
        }
    }

    // Its product is post-processed under its own name while that name is in creation
    static class SelfAskingFactory implements FactoryBean<Object>, BeanFactoryAware, InitializingBean {
        private BeanFactory beanFactory;

        @Override
        public void setBeanFactory(BeanFactory beanFactory) {
            this.beanFactory = beanFactory;
        }

        @Override
        public void afterPropertiesSet() {
            beanFactory.getBean("selfAskingFactory");
        }

        @Override
        public Object getObject() {
            return new SynchronousFactoryProduct();
        }

        @Override
        public Class<?> getObjectType() {
            return Object.class;
        }
    }

    static class HandlerDeclaringFactory implements FactoryBean<Object> {
        @SynchronousSubscription(id = "factory-synchronous-handler")
        void onEvent(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }

        @Override
        public Object getObject() {
            return new Object();
        }

        @Override
        public Class<?> getObjectType() {
            return Object.class;
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class RefusalConfiguration {
        @Bean
        CloudEventConverter<LateBeanAnnotationRegistrationTest.TestEvent> testEventCloudEventConverter() {
            return new LateBeanAnnotationRegistrationTest.NoopCloudEventConverter();
        }

        @Bean
        @SuppressWarnings("unchecked")
        Subscriptions<LateBeanAnnotationRegistrationTest.TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        @Bean(OccurrentReactorBeanNames.SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME)
        @SuppressWarnings("unchecked")
        Subscriptions<LateBeanAnnotationRegistrationTest.TestEvent> synchronousSubscriptions() {
            return mock(Subscriptions.class);
        }
    }
}
