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
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.dcb.blocking.DcbSubscriptions;
import org.occurrent.dsl.subscription.blocking.StreamSubscriptions;
import org.occurrent.dsl.subscription.blocking.Subscriptions;
import org.occurrent.springboot.common.OccurrentProperties;
import org.springframework.beans.factory.SmartFactoryBean;
import org.springframework.beans.factory.config.BeanDefinitionCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;

/**
 * A class a method names can be missing at runtime, such as an optional dependency. When a superclass or an interface
 * of a bean has such a method, no handler can be read from the bean, so the post-processor skips it and the context
 * starts. A bean whose class has the method itself still fails as in 0.34.0, which the common module tests.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class UnreadableHierarchyRegistrationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
            .withUserConfiguration(InfrastructureConfiguration.class);

    @Test
    void an_eager_bean_whose_superclass_declares_a_method_naming_a_missing_class_starts_the_context_and_registers_no_handler() throws Exception {
        Class<?> product = new LoaderWithoutAbsentType().loadClass(ChildOfUnreadableSuperclass.class.getName());
        AtomicInteger constructed = new AtomicInteger();

        runner.withBean("products", ProductFactoryBean.class, () -> new ProductFactoryBean(product, constructed, true)).run(context -> {
            assertThat(context).hasNotFailed();

            assertSoftly(softly -> {
                softly.assertThat(constructed).describedAs("the bean is built and scanned").hasValue(1);
                softly.assertThat(subscribedIds(context.getBean(Subscriptions.class))).isEmpty();
            });
        });
    }

    @Test
    void an_eager_bean_whose_interface_declares_a_method_naming_a_missing_class_starts_the_context_and_registers_no_handler() throws Exception {
        Class<?> product = new LoaderWithoutAbsentType().loadClass(ChildOfUnreadableInterface.class.getName());
        AtomicInteger constructed = new AtomicInteger();

        runner.withBean("products", ProductFactoryBean.class, () -> new ProductFactoryBean(product, constructed, true)).run(context -> {
            assertThat(context).hasNotFailed();

            assertSoftly(softly -> {
                softly.assertThat(constructed).describedAs("the bean is built and scanned").hasValue(1);
                softly.assertThat(subscribedIds(context.getBean(Subscriptions.class))).isEmpty();
            });
        });
    }

    @Test
    void a_lazy_bean_with_a_readable_hierarchy_is_built_at_startup_for_its_inherited_handler() {
        AtomicInteger constructed = new AtomicInteger();

        withBean("readable", ChildOfUnreadableSuperclass.class, constructed, definition -> definition.setLazyInit(true)).run(context -> {
            assertThat(context).hasNotFailed();

            assertSoftly(softly -> {
                softly.assertThat(constructed).describedAs("the post-processor builds a lazy bean that has a handler").hasValue(1);
                softly.assertThat(subscribedIds(context.getBean(Subscriptions.class))).containsExactly("superclass-handler");
            });
        });
    }

    @Test
    void a_factory_bean_product_with_a_readable_hierarchy_is_built_at_startup_for_its_inherited_handler() {
        AtomicInteger constructed = new AtomicInteger();

        runner.withBean("products", ProductFactoryBean.class, () -> new ProductFactoryBean(ChildOfUnreadableSuperclass.class, constructed, false)).run(context -> {
            assertThat(context).hasNotFailed();

            assertSoftly(softly -> {
                softly.assertThat(constructed).describedAs("the post-processor builds a product that has a handler").hasPositiveValue();
                softly.assertThat(subscribedIds(context.getBean(Subscriptions.class))).containsExactly("superclass-handler");
            });
        });
    }

    @Test
    void a_factory_bean_product_whose_superclass_declares_a_method_naming_a_missing_class_is_not_built_by_the_post_processor() throws Exception {
        Class<?> product = new LoaderWithoutAbsentType().loadClass(ChildOfUnreadableSuperclass.class.getName());
        AtomicInteger constructed = new AtomicInteger();

        runner.withBean("products", ProductFactoryBean.class, () -> new ProductFactoryBean(product, constructed, false)).run(context -> {
            assertThat(context).hasNotFailed();

            assertSoftly(softly -> {
                softly.assertThat(constructed).describedAs("the product is only built when somebody asks for it").hasValue(0);
                softly.assertThat(subscribedIds(context.getBean(Subscriptions.class))).isEmpty();
            });
        });
    }

    @Test
    void a_factory_bean_product_whose_interface_declares_a_method_naming_a_missing_class_is_not_built_by_the_post_processor() throws Exception {
        Class<?> product = new LoaderWithoutAbsentType().loadClass(ChildOfUnreadableInterface.class.getName());
        AtomicInteger constructed = new AtomicInteger();

        runner.withBean("products", ProductFactoryBean.class, () -> new ProductFactoryBean(product, constructed, false)).run(context -> {
            assertThat(context).hasNotFailed();

            assertSoftly(softly -> {
                softly.assertThat(constructed).describedAs("the product is only built when somebody asks for it").hasValue(0);
                softly.assertThat(subscribedIds(context.getBean(Subscriptions.class))).isEmpty();
            });
        });
    }

    private <T> ApplicationContextRunner withBean(String name, Class<T> type, AtomicInteger constructed, BeanDefinitionCustomizer... customizers) {
        return runner.withBean(name, type, () -> construct(type, constructed), customizers);
    }

    private static <T> T construct(Class<T> type, AtomicInteger constructed) {
        try {
            T instance = type.getDeclaredConstructor().newInstance();
            constructed.incrementAndGet();
            return instance;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static List<String> subscribedIds(Subscriptions<?> subscriptions) {
        return mockingDetails(subscriptions).getInvocations().stream()
                .filter(invocation -> invocation.getMethod().getName().equals("subscribe"))
                .map(invocation -> (String) invocation.getRawArguments()[0])
                .toList();
    }

    // The class of a bean from a FactoryBean is not read by Spring's own scan for configuration classes, which would
    // fail on the missing class before the post-processor is reached
    static class ProductFactoryBean implements SmartFactoryBean<Object> {
        private final Class<?> productType;
        private final AtomicInteger constructed;
        private final boolean eager;

        ProductFactoryBean(Class<?> productType, AtomicInteger constructed, boolean eager) {
            this.productType = productType;
            this.constructed = constructed;
            this.eager = eager;
        }

        @Override
        public boolean isEagerInit() {
            return eager;
        }

        @Override
        public Object getObject() {
            return construct(productType, constructed);
        }

        @Override
        public Class<?> getObjectType() {
            return productType;
        }
    }

    public static class AbsentType {
    }

    public record TestEvent() {
    }

    public static class UnreadableSuperclass {
        @SuppressWarnings("unused")
        public void uses(AbsentType absent) {
        }

        @Subscription(id = "superclass-handler")
        public void on(TestEvent event) {
        }
    }

    public static class ChildOfUnreadableSuperclass extends UnreadableSuperclass {
    }

    public interface UnreadableInterface {
        @SuppressWarnings("unused")
        default void uses(AbsentType absent) {
        }

        @Subscription(id = "interface-handler")
        default void on(TestEvent event) {
        }
    }

    public static class ChildOfUnreadableInterface implements UnreadableInterface {
    }

    private static final List<Class<?>> FIXTURES = List.of(UnreadableSuperclass.class, ChildOfUnreadableSuperclass.class,
            UnreadableInterface.class, ChildOfUnreadableInterface.class);

    // Defines the fixtures itself so the methods that name AbsentType are read through it, and refuses to load AbsentType
    private static final class LoaderWithoutAbsentType extends ClassLoader {
        LoaderWithoutAbsentType() {
            super(UnreadableHierarchyRegistrationTest.class.getClassLoader());
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            synchronized (getClassLoadingLock(name)) {
                if (name.equals(AbsentType.class.getName())) {
                    throw new ClassNotFoundException(name);
                }
                if (FIXTURES.stream().noneMatch(fixture -> fixture.getName().equals(name))) {
                    return super.loadClass(name, resolve);
                }
                Class<?> loaded = findLoadedClass(name);
                if (loaded == null) {
                    try (InputStream bytes = getParent().getResourceAsStream(name.replace('.', '/') + ".class")) {
                        byte[] definition = bytes.readAllBytes();
                        loaded = defineClass(name, definition, 0, definition.length);
                    } catch (IOException e) {
                        throw new ClassNotFoundException(name, e);
                    }
                }
                return loaded;
            }
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class InfrastructureConfiguration {
        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter() {
            return new NoopCloudEventConverter();
        }

        @Bean
        @Primary
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> subscriptions() {
            return mock(Subscriptions.class);
        }

        @Bean
        @SuppressWarnings("unchecked")
        StreamSubscriptions<TestEvent> streamSubscriptions() {
            return mock(StreamSubscriptions.class);
        }

        @Bean(OccurrentBlockingBeanNames.SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME)
        @SuppressWarnings("unchecked")
        Subscriptions<TestEvent> synchronousSubscriptions() {
            return mock(Subscriptions.class);
        }

        @Bean
        @SuppressWarnings("unchecked")
        DcbSubscriptions<TestEvent> dcbSubscriptions() {
            return mock(DcbSubscriptions.class, Answers.RETURNS_MOCKS);
        }
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
