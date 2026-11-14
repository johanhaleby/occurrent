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
import org.mockito.Answers;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.dcb.blocking.DcbSubscriptions;
import org.occurrent.dsl.subscription.blocking.StreamSubscriptions;
import org.occurrent.dsl.subscription.blocking.Subscriptions;
import org.occurrent.springboot.blocking.overriding.declaring.MidOverridingInTheDeclaringPackage;
import org.occurrent.springboot.blocking.overriding.declaring.MissingAtRuntime;
import org.occurrent.springboot.blocking.overriding.declaring.UnreadableBaseWithPackagePrivateHandler;
import org.occurrent.springboot.blocking.overriding.leaf.LeafOverMidInTheDeclaringPackage;
import org.occurrent.springboot.common.OccurrentProperties;
import org.springframework.beans.factory.SmartFactoryBean;
import org.springframework.beans.factory.config.BeanDefinitionCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;

/**
 * A class a method names can be missing at runtime, such as an optional dependency. When a superclass or an interface
 * of a bean has such a method and declares no handler, the post-processor reads only the handlers the bean's class
 * declares and the context starts. When it declares a handler, or its class file can't be read to tell, the context
 * fails to start, since the handler would never register. A bean whose class has the method itself still fails as in
 * 0.34.0, which the common module tests.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class UnreadableHierarchyRegistrationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(OccurrentBlockingAnnotationBeanPostProcessor.class, OccurrentBlockingAnnotationBeanPostProcessor::new)
            .withUserConfiguration(InfrastructureConfiguration.class);

    @Test
    void an_eager_bean_whose_superclass_declares_a_handler_and_a_method_naming_a_missing_class_fails_the_context() throws Exception {
        Class<?> product = new LoaderWithoutAbsentType().loadClass(ChildOfUnreadableSuperclass.class.getName());

        runner.withBean("products", ProductFactoryBean.class, () -> new ProductFactoryBean(product, new AtomicInteger(), true)).run(context ->
                assertStartupRefused(context, UnreadableSuperclass.class));
    }

    @Test
    void an_eager_bean_whose_interface_declares_a_default_handler_and_a_method_naming_a_missing_class_fails_the_context() throws Exception {
        Class<?> product = new LoaderWithoutAbsentType().loadClass(ChildOfUnreadableInterface.class.getName());

        runner.withBean("products", ProductFactoryBean.class, () -> new ProductFactoryBean(product, new AtomicInteger(), true)).run(context ->
                assertStartupRefused(context, UnreadableInterface.class));
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
    void a_factory_bean_product_whose_superclass_declares_a_handler_and_a_method_naming_a_missing_class_fails_the_context() throws Exception {
        Class<?> product = new LoaderWithoutAbsentType().loadClass(ChildOfUnreadableSuperclass.class.getName());

        runner.withBean("products", ProductFactoryBean.class, () -> new ProductFactoryBean(product, new AtomicInteger(), false)).run(context ->
                assertStartupRefused(context, UnreadableSuperclass.class));
    }

    @Test
    void a_factory_bean_product_whose_interface_declares_a_default_handler_and_a_method_naming_a_missing_class_fails_the_context() throws Exception {
        Class<?> product = new LoaderWithoutAbsentType().loadClass(ChildOfUnreadableInterface.class.getName());

        runner.withBean("products", ProductFactoryBean.class, () -> new ProductFactoryBean(product, new AtomicInteger(), false)).run(context ->
                assertStartupRefused(context, UnreadableInterface.class));
    }

    @Test
    void an_eager_bean_declaring_a_handler_fails_the_context_when_its_superclass_declares_a_handler_and_a_method_naming_a_missing_class() throws Exception {
        Class<?> product = new LoaderWithoutAbsentType().loadClass(DeclaresHandlerAndExtendsUnreadableSuperclass.class.getName());

        runner.withBean("products", ProductFactoryBean.class, () -> new ProductFactoryBean(product, new AtomicInteger(), true)).run(context ->
                assertStartupRefused(context, UnreadableSuperclass.class));
    }

    @Test
    void a_factory_bean_product_declaring_a_handler_fails_the_context_when_its_superclass_declares_a_handler_and_a_method_naming_a_missing_class() throws Exception {
        Class<?> product = new LoaderWithoutAbsentType().loadClass(DeclaresHandlerAndExtendsUnreadableSuperclass.class.getName());

        runner.withBean("products", ProductFactoryBean.class, () -> new ProductFactoryBean(product, new AtomicInteger(), false)).run(context ->
                assertStartupRefused(context, UnreadableSuperclass.class));
    }

    @Test
    void an_eager_bean_declaring_a_handler_fails_the_context_when_its_interface_declares_a_default_handler_and_a_method_naming_a_missing_class() throws Exception {
        Class<?> product = new LoaderWithoutAbsentType().loadClass(DeclaresHandlerAndImplementsUnreadableInterface.class.getName());

        runner.withBean("products", ProductFactoryBean.class, () -> new ProductFactoryBean(product, new AtomicInteger(), true)).run(context ->
                assertStartupRefused(context, UnreadableInterface.class));
    }

    @Test
    void a_factory_bean_product_declaring_a_handler_fails_the_context_when_its_interface_declares_a_default_handler_and_a_method_naming_a_missing_class() throws Exception {
        Class<?> product = new LoaderWithoutAbsentType().loadClass(DeclaresHandlerAndImplementsUnreadableInterface.class.getName());

        runner.withBean("products", ProductFactoryBean.class, () -> new ProductFactoryBean(product, new AtomicInteger(), false)).run(context ->
                assertStartupRefused(context, UnreadableInterface.class));
    }

    @Test
    void an_eager_bean_whose_interface_declares_a_method_naming_a_missing_class_registers_the_handler_a_declared_bridge_calls_once() throws Exception {
        Class<?> product = new LoaderWithoutAbsentType().loadClass(DeclaresBridgedHandlerAndImplementsUnreadableInterface.class.getName());
        AtomicInteger constructed = new AtomicInteger();

        runner.withBean("products", ProductFactoryBean.class, () -> new ProductFactoryBean(product, constructed, true)).run(context -> {
            assertThat(context).hasNotFailed();

            assertSoftly(softly -> {
                softly.assertThat(constructed).describedAs("the bean is built and scanned").hasPositiveValue();
                softly.assertThat(subscribedIds(context.getBean(Subscriptions.class))).containsExactly("bridged-handler");
            });
        });
    }

    @Test
    void an_eager_bean_whose_superclass_declares_a_method_naming_a_missing_class_registers_its_handlers_although_the_bridge_to_one_of_them_cannot_be_resolved() throws Exception {
        Class<?> product = new LoaderWithoutAbsentType().loadClass(DeclaresBridgeAndHandlerAndExtendsUnreadableSuperclass.class.getName());
        AtomicInteger constructed = new AtomicInteger();

        runner.withBean("products", ProductFactoryBean.class, () -> new ProductFactoryBean(product, constructed, true)).run(context -> {
            assertThat(context).hasNotFailed();

            assertSoftly(softly -> {
                softly.assertThat(constructed).describedAs("the bean is built and scanned").hasPositiveValue();
                softly.assertThat(subscribedIds(context.getBean(Subscriptions.class))).containsExactlyInAnyOrder("bridged-handler", "own-handler");
            });
        });
    }

    @Test
    void an_eager_bean_fails_the_context_when_the_unreadable_superclass_with_a_handler_is_a_grandparent() throws Exception {
        Class<?> product = new LoaderWithoutAbsentType().loadClass(GrandchildOfUnreadableSuperclass.class.getName());

        runner.withBean("products", ProductFactoryBean.class, () -> new ProductFactoryBean(product, new AtomicInteger(), true)).run(context ->
                assertStartupRefused(context, UnreadableSuperclass.class));
    }

    @Test
    void an_eager_bean_fails_the_context_naming_the_missing_class_when_the_class_file_of_a_superclass_declaring_a_method_naming_it_can_not_be_read() throws Exception {
        Class<?> product = new LoaderWithoutAbsentType(UnreadableSuperclass.class).loadClass(ChildOfUnreadableSuperclass.class.getName());

        runner.withBean("products", ProductFactoryBean.class, () -> new ProductFactoryBean(product, new AtomicInteger(), true)).run(context ->
                assertStartupRefusedForUnreadableClassFile(context, UnreadableSuperclass.class));
    }

    @Test
    void an_eager_bean_whose_superclass_declares_no_handler_and_a_method_naming_a_missing_class_starts_the_context_and_registers_the_handler_the_bean_declares() throws Exception {
        Class<?> product = new LoaderWithoutAbsentType().loadClass(DeclaresHandlerAndExtendsUnreadableSuperclassWithoutHandler.class.getName());
        AtomicInteger constructed = new AtomicInteger();

        runner.withBean("products", ProductFactoryBean.class, () -> new ProductFactoryBean(product, constructed, true)).run(context -> {
            assertThat(context).hasNotFailed();

            assertSoftly(softly -> {
                softly.assertThat(constructed).describedAs("the bean is built and scanned").hasPositiveValue();
                softly.assertThat(subscribedIds(context.getBean(Subscriptions.class))).containsExactly("own-handler");
            });
        });
    }

    @Test
    void an_eager_bean_registers_and_runs_the_visibility_bridge_carrying_the_handler_annotation_when_the_package_private_superclass_declaring_the_handler_can_not_be_read() throws Exception {
        Class<?> product = new LoaderWithoutAbsentType().loadClass(LeafOverUnreadablePackagePrivateBase.class.getName());
        AtomicInteger constructed = new AtomicInteger();

        runner.withBean("products", ProductFactoryBean.class, () -> new ProductFactoryBean(product, constructed, true)).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);

            assertThat(subscribedIds(subscriptions)).containsExactly("visibility-bridge-handler");
            handlerFor(subscriptions, "visibility-bridge-handler").invoke(null, new TestEvent());
            assertThat(ranBodies(product)).containsExactly("visibility-bridge-body");
        });
    }

    @Test
    void an_eager_bean_registers_and_runs_the_visibility_bridge_for_the_superclass_handler_when_it_resolves_to_a_default_handler_of_an_interface_that_can_not_be_read() throws Exception {
        Class<?> product = new LoaderWithoutAbsentType().loadClass(LeafOverMidAndUnreadableInterface.class.getName());
        AtomicInteger constructed = new AtomicInteger();

        runner.withBean("products", ProductFactoryBean.class, () -> new ProductFactoryBean(product, constructed, true)).run(context -> {
            assertThat(context).hasNotFailed();
            Subscriptions<?> subscriptions = context.getBean(Subscriptions.class);

            assertThat(subscribedIds(subscriptions)).containsExactly("mid");
            handlerFor(subscriptions, "mid").invoke(null, new TestEvent());
            assertThat(ranBodies(product)).containsExactly("mid-body");
        });
    }

    @Test
    void an_eager_bean_fails_the_context_naming_the_overload_that_has_no_visibility_bridge_when_the_package_private_superclass_declaring_it_can_not_be_read() throws Exception {
        Class<?> product = new LoaderWithoutAbsentType().loadClass(LeafOverUnreadableBaseWithPackagePrivateOverload.class.getName());

        runner.withBean("products", ProductFactoryBean.class, () -> new ProductFactoryBean(product, new AtomicInteger(), true)).run(context ->
                assertStartupRefused(context, UnreadableBaseWithPackagePrivateOverload.class, "on(java.lang.Integer)"));
    }

    @Test
    void an_eager_bean_registers_only_the_handler_it_declares_and_not_its_generic_bridge_when_it_re_annotates_a_handler_of_an_unreadable_generic_superclass() throws Exception {
        Class<?> product = new LoaderWithoutAbsentType().loadClass(ReAnnotatesGenericHandlerOfUnreadableSuperclass.class.getName());
        AtomicInteger constructed = new AtomicInteger();

        runner.withBean("products", ProductFactoryBean.class, () -> new ProductFactoryBean(product, constructed, true)).run(context -> {
            assertThat(context).hasNotFailed();

            assertThat(subscribedIds(context.getBean(Subscriptions.class))).containsExactly("generic-handler");
        });
    }

    @Test
    void an_eager_bean_registers_the_handler_of_an_unreadable_package_private_superclass_beside_the_overload_it_declares() throws Exception {
        Class<?> product = new LoaderWithoutAbsentType().loadClass(LeafOverloadingAnUnreadableBaseHandler.class.getName());
        AtomicInteger constructed = new AtomicInteger();

        runner.withBean("products", ProductFactoryBean.class, () -> new ProductFactoryBean(product, constructed, true)).run(context -> {
            assertThat(context).hasNotFailed();

            assertThat(subscribedIds(context.getBean(Subscriptions.class))).containsExactlyInAnyOrder("overload-handler", "base-handler");
        });
    }

    @Test
    void an_eager_bean_registers_its_handler_when_a_class_between_it_and_the_unreadable_declaring_class_overrides_the_package_private_handler_in_the_package_of_it() throws Exception {
        Class<?> product = new LoaderWithoutAbsentType().loadClass(LeafOverMidInTheDeclaringPackage.class.getName());
        AtomicInteger constructed = new AtomicInteger();

        runner.withBean("products", ProductFactoryBean.class, () -> new ProductFactoryBean(product, constructed, true)).run(context -> {
            assertThat(context).hasNotFailed();

            assertThat(subscribedIds(context.getBean(Subscriptions.class))).containsExactly("leaf-handler");
        });
    }

    @SuppressWarnings("unchecked")
    private static List<String> ranBodies(Class<?> product) throws ReflectiveOperationException {
        return (List<String>) product.getField("RAN").get(null);
    }

    @SuppressWarnings("unchecked")
    private static Function2<EventMetadata, TestEvent, Unit> handlerFor(Subscriptions<?> subscriptions, String id) {
        return mockingDetails(subscriptions).getInvocations().stream()
                .filter(invocation -> invocation.getMethod().getName().equals("subscribe") && id.equals(invocation.getRawArguments()[0]))
                .map(invocation -> (Function2<EventMetadata, TestEvent, Unit>) invocation.getRawArguments()[invocation.getRawArguments().length - 1])
                .findFirst()
                .orElseThrow(() -> new AssertionError("No handler was subscribed under id " + id));
    }

    private static void assertStartupRefused(AssertableApplicationContext context, Class<?> unreadable) {
        assertStartupRefused(context, unreadable, "on(" + TestEvent.class.getName() + ")");
    }

    private static void assertStartupRefused(AssertableApplicationContext context, Class<?> unreadable, String handler) {
        String missing = AbsentType.class.getName();

        assertThat(context).hasFailed();
        assertThat(refusalIn(context.getStartupFailure()))
                .hasMessageContaining("The methods of " + unreadable.getName() + " can't be read")
                .hasMessageContaining("since " + missing + " is missing at runtime")
                .hasMessageContaining("its Occurrent handler " + unreadable.getName() + "#" + handler + " would never register")
                .hasMessageContaining("Put " + missing + " on the classpath.");
    }

    @Test
    void an_eager_bean_fails_the_context_naming_the_handler_of_a_readable_superclass_when_a_class_above_it_declares_a_method_naming_a_missing_class() throws Exception {
        Class<?> product = new LoaderWithoutAbsentType().loadClass(ExtendsReadableHandlerClassAboveUnreadableSuperclass.class.getName());
        String missing = AbsentType.class.getName();

        runner.withBean("products", ProductFactoryBean.class, () -> new ProductFactoryBean(product, new AtomicInteger(), true)).run(context -> {
            assertThat(context).hasFailed();
            assertThat(refusalIn(context.getStartupFailure()))
                    .hasMessageContaining("The Occurrent handler " + ReadableHandlerClassAboveUnreadableSuperclass.class.getName() + "#on(" + TestEvent.class.getName() + ") would never register")
                    .hasMessageContaining("since " + missing + " is missing at runtime")
                    .hasMessageContaining("so the scan reads only the methods " + product.getName() + " declares")
                    .hasMessageContaining("Put " + missing + " on the classpath.");
        });
    }

    private static void assertStartupRefusedForUnreadableClassFile(AssertableApplicationContext context, Class<?> unreadable) {
        String missing = AbsentType.class.getName();

        assertThat(context).hasFailed();
        assertThat(refusalIn(context.getStartupFailure()))
                .hasMessageContaining("The methods of " + unreadable.getName() + " can't be read")
                .hasMessageContaining("since " + missing + " is missing at runtime, and neither can its class file")
                .hasMessageContaining("Put " + missing + " on the classpath.");
    }

    private static Throwable refusalIn(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof IllegalStateException && cause.getMessage() != null && (cause.getMessage().startsWith("The methods of ") || cause.getMessage().startsWith("The Occurrent handler "))) {
                return cause;
            }
        }
        throw new AssertionError("no scan refusal in the startup failure", failure);
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

    public static class DeclaresHandlerAndExtendsUnreadableSuperclass extends UnreadableSuperclass {
        @Subscription(id = "own-handler")
        public void own(TestEvent event) {
        }
    }

    public static class DeclaresHandlerAndImplementsUnreadableInterface implements UnreadableInterface {
        @Subscription(id = "own-handler")
        public void own(TestEvent event) {
        }
    }

    // The method naming AbsentType is private, so the class that implements this interface can list its methods and the
    // bridge to on(Object) it declares resolves, while the scan of the interface itself fails
    public interface UnreadableGenericInterface<T> {
        void on(T event);

        @SuppressWarnings("unused")
        private void uses(AbsentType absent) {
        }
    }

    public static class DeclaresBridgedHandlerAndImplementsUnreadableInterface implements UnreadableGenericInterface<TestEvent> {
        @Subscription(id = "bridged-handler")
        @Override
        public void on(TestEvent event) {
        }
    }

    public abstract static class UnreadableGenericSuperclass<T> {
        @SuppressWarnings("unused")
        public void uses(AbsentType absent) {
        }

        public abstract void on(T event);
    }

    public static class DeclaresBridgeAndHandlerAndExtendsUnreadableSuperclass extends UnreadableGenericSuperclass<TestEvent> {
        @Subscription(id = "bridged-handler")
        @Override
        public void on(TestEvent event) {
        }

        @Subscription(id = "own-handler")
        public void own(TestEvent event) {
        }
    }

    public static class GrandchildOfUnreadableSuperclass extends ChildOfUnreadableSuperclass {
    }

    public static class UnreadableSuperclassWithoutHandler {
        @SuppressWarnings("unused")
        public void uses(AbsentType absent) {
        }
    }

    public static class DeclaresHandlerAndExtendsUnreadableSuperclassWithoutHandler extends UnreadableSuperclassWithoutHandler {
        @Subscription(id = "own-handler")
        public void own(TestEvent event) {
        }
    }

    public abstract static class UnreadableGenericSuperclassWithHandler<T> {
        @SuppressWarnings("unused")
        public void uses(AbsentType absent) {
        }

        @Subscription(id = "generic-handler")
        public abstract void on(T event);
    }

    public static class ReAnnotatesGenericHandlerOfUnreadableSuperclass extends UnreadableGenericSuperclassWithHandler<TestEvent> {
        @Subscription(id = "generic-handler")
        @Override
        public void on(TestEvent event) {
        }
    }

    // The superclass that can't be read declares no handler, so only the handler of this class fails the scan
    public static class ReadableHandlerClassAboveUnreadableSuperclass extends UnreadableSuperclassWithoutHandler {
        @Subscription(id = "readable-superclass-handler")
        public void on(TestEvent event) {
        }
    }

    public static class ExtendsReadableHandlerClassAboveUnreadableSuperclass extends ReadableHandlerClassAboveUnreadableSuperclass {
    }

    private static final List<Class<?>> FIXTURES = List.of(LeafOverMidAndUnreadableInterface.class, PackagePrivateMidWithHandler.class, UnreadableInterfaceWithDefaultHandler.class,
            UnreadableSuperclass.class, ChildOfUnreadableSuperclass.class,
            ReadableHandlerClassAboveUnreadableSuperclass.class, ExtendsReadableHandlerClassAboveUnreadableSuperclass.class,
            UnreadableInterface.class, ChildOfUnreadableInterface.class,
            DeclaresHandlerAndExtendsUnreadableSuperclass.class, DeclaresHandlerAndImplementsUnreadableInterface.class,
            UnreadableGenericInterface.class, DeclaresBridgedHandlerAndImplementsUnreadableInterface.class,
            UnreadableGenericSuperclass.class, DeclaresBridgeAndHandlerAndExtendsUnreadableSuperclass.class,
            GrandchildOfUnreadableSuperclass.class, UnreadableSuperclassWithoutHandler.class, DeclaresHandlerAndExtendsUnreadableSuperclassWithoutHandler.class,
            LeafOverUnreadablePackagePrivateBase.class, UnreadablePackagePrivateBase.class,
            LeafOverUnreadableBaseWithPackagePrivateOverload.class, UnreadableBaseWithPackagePrivateOverload.class,
            UnreadableGenericSuperclassWithHandler.class, ReAnnotatesGenericHandlerOfUnreadableSuperclass.class,
            LeafOverloadingAnUnreadableBaseHandler.class, UnreadablePackagePrivateBaseWithBaseEventHandler.class,
            UnreadableBaseWithPackagePrivateHandler.class, MidOverridingInTheDeclaringPackage.class, LeafOverMidInTheDeclaringPackage.class);

    // Defines the fixtures itself so the methods that name AbsentType or MissingAtRuntime are read through it, and refuses to load both.
    // It can also hide the class file of a fixture it still defines
    private static final class LoaderWithoutAbsentType extends ClassLoader {
        private final String hiddenResource;

        LoaderWithoutAbsentType() {
            super(UnreadableHierarchyRegistrationTest.class.getClassLoader());
            this.hiddenResource = null;
        }

        LoaderWithoutAbsentType(Class<?> fixtureWithHiddenClassFile) {
            super(UnreadableHierarchyRegistrationTest.class.getClassLoader());
            this.hiddenResource = fixtureWithHiddenClassFile.getName().replace('.', '/') + ".class";
        }

        @Override
        public URL getResource(String name) {
            return name.equals(hiddenResource) ? null : super.getResource(name);
        }

        @Override
        public InputStream getResourceAsStream(String name) {
            return name.equals(hiddenResource) ? null : super.getResourceAsStream(name);
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            synchronized (getClassLoadingLock(name)) {
                if (name.equals(AbsentType.class.getName()) || name.equals(MissingAtRuntime.class.getName())) {
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
