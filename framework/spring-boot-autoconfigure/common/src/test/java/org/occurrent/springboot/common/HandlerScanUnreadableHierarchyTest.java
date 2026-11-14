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

package org.occurrent.springboot.common;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.Subscription;
import org.occurrent.springboot.common.internal.HandlerScan;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.tuple;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

/**
 * A class a method names can be missing at runtime, such as an optional dependency. When a superclass or an interface
 * has such a method, the scan of a class reads only the methods the class declares, so an inherited handler is not
 * found and one the class declares is. It fails when the class itself declares such a method, as in 0.34.0.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class HandlerScanUnreadableHierarchyTest {

    private static final List<Class<? extends Annotation>> HANDLER_ANNOTATIONS = List.of(Subscription.class);

    public static class AbsentType {
    }

    public static class UnreadableSuperclass {
        @SuppressWarnings("unused")
        public void uses(AbsentType absent) {
        }

        @Subscription(id = "superclass-handler")
        public void on(String event) {
        }
    }

    public static class ChildOfUnreadableSuperclass extends UnreadableSuperclass {
    }

    public interface UnreadableInterface {
        @SuppressWarnings("unused")
        default void uses(AbsentType absent) {
        }

        @Subscription(id = "interface-handler")
        default void on(String event) {
        }
    }

    public static class ChildOfUnreadableInterface implements UnreadableInterface {
    }

    public static class DeclaresUnreadableMethod {
        @SuppressWarnings("unused")
        public void uses(AbsentType absent) {
        }

        @Subscription(id = "own-handler")
        public void on(String event) {
        }
    }

    public static class DeclaresHandlerAndExtendsUnreadableSuperclass extends UnreadableSuperclass {
        @Subscription(id = "own-handler")
        public void own(Integer event) {
        }
    }

    public static class DeclaresHandlerAndImplementsUnreadableInterface implements UnreadableInterface {
        @Subscription(id = "own-handler")
        public void own(Integer event) {
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

    public static class DeclaresBridgedHandlerAndImplementsUnreadableInterface implements UnreadableGenericInterface<String> {
        @Subscription(id = "bridged-handler")
        @Override
        public void on(String event) {
        }
    }

    public abstract static class UnreadableGenericSuperclass<T> {
        @SuppressWarnings("unused")
        public void uses(AbsentType absent) {
        }

        public abstract void on(T event);
    }

    public static class DeclaresBridgeAndHandlerAndExtendsUnreadableSuperclass extends UnreadableGenericSuperclass<String> {
        @Subscription(id = "bridged-handler")
        @Override
        public void on(String event) {
        }

        @Subscription(id = "own-handler")
        public void own(Integer event) {
        }
    }

    private static final List<Class<?>> FIXTURES = List.of(UnreadableSuperclass.class, ChildOfUnreadableSuperclass.class,
            UnreadableInterface.class, ChildOfUnreadableInterface.class, DeclaresUnreadableMethod.class,
            DeclaresHandlerAndExtendsUnreadableSuperclass.class, DeclaresHandlerAndImplementsUnreadableInterface.class,
            UnreadableGenericInterface.class, DeclaresBridgedHandlerAndImplementsUnreadableInterface.class,
            UnreadableGenericSuperclass.class, DeclaresBridgeAndHandlerAndExtendsUnreadableSuperclass.class);

    @Test
    void handlerMethods_finds_the_handler_of_a_class_inheriting_it_when_every_class_the_hierarchy_names_is_present() {
        assertSoftly(softly -> {
            softly.assertThat(HandlerScan.handlerMethods(ChildOfUnreadableSuperclass.class, HANDLER_ANNOTATIONS)).extracting(Method::getName).containsExactly("on");
            softly.assertThat(HandlerScan.handlerMethods(ChildOfUnreadableInterface.class, HANDLER_ANNOTATIONS)).extracting(Method::getName).containsExactly("on");
        });
    }

    @Test
    void handlerMethods_is_empty_when_a_superclass_declares_a_method_naming_a_class_missing_at_runtime() throws Exception {
        Class<?> child = new LoaderWithoutAbsentType().loadClass(ChildOfUnreadableSuperclass.class.getName());

        assertThat(scanWithoutThrowing(child)).isEmpty();
    }

    @Test
    void handlerMethods_is_empty_when_an_interface_declares_a_method_naming_a_class_missing_at_runtime() throws Exception {
        Class<?> child = new LoaderWithoutAbsentType().loadClass(ChildOfUnreadableInterface.class.getName());

        assertThat(scanWithoutThrowing(child)).isEmpty();
    }

    @Test
    void handlerMethods_returns_only_the_handler_the_class_declares_when_a_superclass_declares_a_method_naming_a_class_missing_at_runtime() throws Exception {
        Class<?> child = new LoaderWithoutAbsentType().loadClass(DeclaresHandlerAndExtendsUnreadableSuperclass.class.getName());

        assertThat(scanWithoutThrowing(child)).extracting(Method::getName, method -> method.getDeclaringClass().getName())
                .containsExactly(tuple("own", child.getName()));
    }

    @Test
    void handlerMethods_returns_only_the_handler_the_class_declares_when_an_interface_declares_a_method_naming_a_class_missing_at_runtime() throws Exception {
        Class<?> child = new LoaderWithoutAbsentType().loadClass(DeclaresHandlerAndImplementsUnreadableInterface.class.getName());

        assertThat(scanWithoutThrowing(child)).extracting(Method::getName, method -> method.getDeclaringClass().getName())
                .containsExactly(tuple("own", child.getName()));
    }

    @Test
    void handlerMethods_returns_the_method_a_declared_bridge_calls_and_not_the_bridge_when_an_interface_declares_a_method_naming_a_class_missing_at_runtime() throws Exception {
        Class<?> child = new LoaderWithoutAbsentType().loadClass(DeclaresBridgedHandlerAndImplementsUnreadableInterface.class.getName());

        assertThat(child.getDeclaredMethods()).describedAs("the class declares the bridge beside the handler")
                .extracting(Method::getName, Method::isBridge).containsExactlyInAnyOrder(tuple("on", false), tuple("on", true));
        assertThat(scanWithoutThrowing(child)).extracting(Method::getName, Method::isBridge, method -> method.getDeclaringClass().getName(), method -> List.of(method.getParameterTypes()))
                .containsExactly(tuple("on", false, child.getName(), List.of(String.class)));
    }

    @Test
    void handlerMethods_leaves_out_a_declared_bridge_that_can_not_be_resolved_and_returns_the_other_handlers_the_class_declares_when_a_superclass_declares_a_method_naming_a_class_missing_at_runtime() throws Exception {
        Class<?> child = new LoaderWithoutAbsentType().loadClass(DeclaresBridgeAndHandlerAndExtendsUnreadableSuperclass.class.getName());

        assertThat(child.getDeclaredMethods()).describedAs("the class declares a bridge beside its handlers")
                .filteredOn(Method::isBridge).extracting(Method::getName).containsExactly("on");
        assertThat(scanWithoutThrowing(child)).extracting(Method::getName, Method::isBridge, method -> method.getDeclaringClass().getName())
                .containsExactlyInAnyOrder(tuple("on", false, child.getName()), tuple("own", false, child.getName()));
    }

    @Test
    void handlerMethods_still_throws_NoClassDefFoundError_when_the_class_itself_declares_a_method_naming_a_class_missing_at_runtime() throws Exception {
        Class<?> declaring = new LoaderWithoutAbsentType().loadClass(DeclaresUnreadableMethod.class.getName());

        assertThatThrownBy(() -> HandlerScan.handlerMethods(declaring, HANDLER_ANNOTATIONS)).isInstanceOf(NoClassDefFoundError.class);
    }

    private static Set<Method> scanWithoutThrowing(Class<?> type) {
        AtomicReference<Set<Method>> scanned = new AtomicReference<>();
        Throwable thrown = catchThrowable(() -> scanned.set(HandlerScan.handlerMethods(type, HANDLER_ANNOTATIONS)));

        assertThat(thrown).describedAs("the scan of a class with an unreadable superclass or interface").isNull();
        return scanned.get();
    }

    // Defines the fixtures itself so the methods that name AbsentType are read through it, and refuses to load AbsentType
    private static final class LoaderWithoutAbsentType extends ClassLoader {
        LoaderWithoutAbsentType() {
            super(HandlerScanUnreadableHierarchyTest.class.getClassLoader());
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
}
