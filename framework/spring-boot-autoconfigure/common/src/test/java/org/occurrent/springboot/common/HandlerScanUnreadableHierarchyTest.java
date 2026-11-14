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

import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.Subscription;
import org.occurrent.springboot.common.internal.HandlerScan;
import org.occurrent.springboot.common.overriding.declaring.MidOverridingInTheDeclaringPackage;
import org.occurrent.springboot.common.overriding.declaring.MissingAtRuntime;
import org.occurrent.springboot.common.overriding.declaring.UnreadableBaseWithPackagePrivateHandler;
import org.occurrent.springboot.common.overriding.declaring.UnreadableMidInTheDeclaringPackage;
import org.occurrent.springboot.common.overriding.elsewhere.MidInAnotherPackage;
import org.occurrent.springboot.common.overriding.leaf.LeafOverMidInAnotherPackage;
import org.occurrent.springboot.common.overriding.leaf.LeafOverUnreadableMidInTheDeclaringPackage;
import org.occurrent.springboot.common.overriding.leaf.LeafOverMidInTheDeclaringPackage;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.net.URL;
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
 * has such a method and declares no handler, the scan of a class reads only the methods the class declares. When it
 * declares a handler, or its class file can't be read to tell, the scan fails, since the handler would never register.
 * It also fails when the class itself declares such a method, as in 0.34.0.
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

    // The method naming AbsentType is private, so finding the method a bridge calls reads only the public methods of the
    // interface and succeeds, while the scan of the interface itself fails
    public interface UnreadableOnlyByItsPrivateMethod {
        @SuppressWarnings("unused")
        private void uses(AbsentType absent) {
        }
    }

    public static class GrandchildOfUnreadableSuperclass extends ChildOfUnreadableSuperclass {
    }

    public interface InterfaceExtendingUnreadableInterface extends UnreadableInterface {
    }

    public static class ImplementsInterfaceExtendingUnreadableInterface implements InterfaceExtendingUnreadableInterface {
    }

    public static class UnreadableSuperclassWithoutHandler {
        @SuppressWarnings("unused")
        public void uses(AbsentType absent) {
        }
    }

    public static class ChildOfUnreadableSuperclassWithoutHandler extends UnreadableSuperclassWithoutHandler {
    }

    public static class DeclaresHandlerAndExtendsUnreadableSuperclassWithoutHandler extends UnreadableSuperclassWithoutHandler {
        @Subscription(id = "own-handler")
        public void own(Integer event) {
        }
    }

    // A static method of an interface is no handler, so the handler annotation on it does not fail the scan
    public interface UnreadableInterfaceWithStaticAnnotatedMethod {
        @SuppressWarnings("unused")
        default void uses(AbsentType absent) {
        }

        @Subscription(id = "static-interface-method")
        static void on(String event) {
        }
    }

    public static class ChildOfUnreadableInterfaceWithStaticAnnotatedMethod implements UnreadableInterfaceWithStaticAnnotatedMethod {
    }

    public abstract static class UnreadableGenericSuperclassWithHandler<T> {
        @SuppressWarnings("unused")
        public void uses(AbsentType absent) {
        }

        @Subscription(id = "generic-handler")
        public abstract void on(T event);
    }

    public static class ReAnnotatesGenericHandlerOfUnreadableSuperclass extends UnreadableGenericSuperclassWithHandler<String> {
        @Subscription(id = "generic-handler")
        @Override
        public void on(String event) {
        }
    }

    public static class OverridesUnreadableHandlerWithoutAnnotation extends UnreadableSuperclass {
        @Override
        public void on(String event) {
        }
    }

    public static class UnreadableSuperclassWithPrivateHandler {
        @SuppressWarnings("unused")
        public void uses(AbsentType absent) {
        }

        @SuppressWarnings("unused")
        @Subscription(id = "private-handler")
        private void on(String event) {
        }
    }

    public static class DeclaresHandlerOfTheSignatureOfAnUnreadablePrivateHandler extends UnreadableSuperclassWithPrivateHandler {
        @Subscription(id = "own-handler")
        public void on(String event) {
        }
    }

    // The superclass that can't be read declares no handler, so only the handler of this class fails the scan
    public static class ReadableHandlerClassAboveUnreadableSuperclass extends UnreadableSuperclassWithoutHandler {
        @Subscription(id = "readable-superclass-handler")
        public void on(String event) {
        }
    }

    public static class ExtendsReadableHandlerClassAboveUnreadableSuperclass extends ReadableHandlerClassAboveUnreadableSuperclass {
    }

    // A static method of an interface is no handler, so the readable interface does not fail the scan
    public interface ReadableInterfaceWithStaticAnnotatedMethod {
        @Subscription(id = "static-interface-method")
        static void on(String event) {
        }
    }

    public static class ExtendsUnreadableSuperclassAndImplementsReadableInterfaceWithStaticAnnotatedMethod extends UnreadableSuperclassWithoutHandler implements ReadableInterfaceWithStaticAnnotatedMethod {
        @Subscription(id = "own-handler")
        public void own(Integer event) {
        }
    }

    private static final List<Class<?>> FIXTURES = List.of(UnreadableSuperclass.class, ChildOfUnreadableSuperclass.class,
            ReadableInterfaceWithStaticAnnotatedMethod.class, ExtendsUnreadableSuperclassAndImplementsReadableInterfaceWithStaticAnnotatedMethod.class,
            ReadableHandlerClassAboveUnreadableSuperclass.class, ExtendsReadableHandlerClassAboveUnreadableSuperclass.class,
            UnreadableInterface.class, ChildOfUnreadableInterface.class, DeclaresUnreadableMethod.class,
            DeclaresHandlerAndExtendsUnreadableSuperclass.class, DeclaresHandlerAndImplementsUnreadableInterface.class,
            UnreadableGenericInterface.class, DeclaresBridgedHandlerAndImplementsUnreadableInterface.class,
            UnreadableGenericSuperclass.class, DeclaresBridgeAndHandlerAndExtendsUnreadableSuperclass.class,
            UnreadableOnlyByItsPrivateMethod.class, PublicLeafDeclaringOnlyTheVisibilityBridge.class, PackagePrivateUnreadableHierarchyBase.class,
            GrandchildOfUnreadableSuperclass.class, InterfaceExtendingUnreadableInterface.class, ImplementsInterfaceExtendingUnreadableInterface.class,
            UnreadableSuperclassWithoutHandler.class, ChildOfUnreadableSuperclassWithoutHandler.class, DeclaresHandlerAndExtendsUnreadableSuperclassWithoutHandler.class,
            UnreadableInterfaceWithStaticAnnotatedMethod.class, ChildOfUnreadableInterfaceWithStaticAnnotatedMethod.class,
            LeafOverUnreadablePackagePrivateBase.class, UnreadablePackagePrivateBase.class,
            LeafOverUnreadableBaseWithPackagePrivateOverload.class, UnreadableBaseWithPackagePrivateOverload.class,
            LeafOverReadablePackagePrivateBelowUnreadableTop.class, ReadablePackagePrivateBelowUnreadableTop.class, UnreadableTop.class,
            UnreadableGenericSuperclassWithHandler.class, ReAnnotatesGenericHandlerOfUnreadableSuperclass.class,
            OverridesUnreadableHandlerWithoutAnnotation.class, UnreadableSuperclassWithPrivateHandler.class, DeclaresHandlerOfTheSignatureOfAnUnreadablePrivateHandler.class,
            LeafOverloadingAnUnreadableBaseHandler.class, UnreadablePackagePrivateBaseWithObjectHandler.class,
            UnreadableBaseWithPackagePrivateHandler.class, MidOverridingInTheDeclaringPackage.class, LeafOverMidInTheDeclaringPackage.class,
            MidInAnotherPackage.class, LeafOverMidInAnotherPackage.class,
            UnreadableMidInTheDeclaringPackage.class, LeafOverUnreadableMidInTheDeclaringPackage.class);

    @Test
    void handlerMethods_finds_the_handler_of_a_class_inheriting_it_when_every_class_the_hierarchy_names_is_present() {
        assertSoftly(softly -> {
            softly.assertThat(HandlerScan.handlerMethods(ChildOfUnreadableSuperclass.class, HANDLER_ANNOTATIONS)).extracting(Method::getName).containsExactly("on");
            softly.assertThat(HandlerScan.handlerMethods(ChildOfUnreadableInterface.class, HANDLER_ANNOTATIONS)).extracting(Method::getName).containsExactly("on");
        });
    }

    @Test
    void handlerMethods_throws_naming_the_handler_when_a_superclass_declares_a_handler_and_a_method_naming_a_class_missing_at_runtime() throws Exception {
        Class<?> child = new LoaderWithoutAbsentType().loadClass(ChildOfUnreadableSuperclass.class.getName());

        assertRefusedForUnreadableHandler(child, UnreadableSuperclass.class);
    }

    @Test
    void handlerMethods_throws_naming_the_handler_when_an_interface_declares_a_default_handler_and_a_method_naming_a_class_missing_at_runtime() throws Exception {
        Class<?> child = new LoaderWithoutAbsentType().loadClass(ChildOfUnreadableInterface.class.getName());

        assertRefusedForUnreadableHandler(child, UnreadableInterface.class);
    }

    @Test
    void handlerMethods_throws_for_the_inherited_handler_although_the_class_declares_a_handler_of_its_own_when_a_superclass_declares_a_method_naming_a_class_missing_at_runtime() throws Exception {
        Class<?> child = new LoaderWithoutAbsentType().loadClass(DeclaresHandlerAndExtendsUnreadableSuperclass.class.getName());

        assertRefusedForUnreadableHandler(child, UnreadableSuperclass.class);
    }

    @Test
    void handlerMethods_throws_for_the_inherited_handler_although_the_class_declares_a_handler_of_its_own_when_an_interface_declares_a_method_naming_a_class_missing_at_runtime() throws Exception {
        Class<?> child = new LoaderWithoutAbsentType().loadClass(DeclaresHandlerAndImplementsUnreadableInterface.class.getName());

        assertRefusedForUnreadableHandler(child, UnreadableInterface.class);
    }

    @Test
    void handlerMethods_throws_when_the_unreadable_superclass_with_a_handler_is_a_grandparent_of_the_class() throws Exception {
        Class<?> grandchild = new LoaderWithoutAbsentType().loadClass(GrandchildOfUnreadableSuperclass.class.getName());

        assertRefusedForUnreadableHandler(grandchild, UnreadableSuperclass.class);
    }

    @Test
    void handlerMethods_throws_when_the_unreadable_interface_with_a_default_handler_is_a_super_interface_of_an_interface_the_class_implements() throws Exception {
        Class<?> implementor = new LoaderWithoutAbsentType().loadClass(ImplementsInterfaceExtendingUnreadableInterface.class.getName());

        assertRefusedForUnreadableHandler(implementor, UnreadableInterface.class);
    }

    @Test
    void handlerMethods_throws_naming_the_handler_of_a_readable_superclass_when_a_class_above_it_declares_a_method_naming_a_class_missing_at_runtime() throws Exception {
        Class<?> child = new LoaderWithoutAbsentType().loadClass(ExtendsReadableHandlerClassAboveUnreadableSuperclass.class.getName());
        String missing = AbsentType.class.getName();

        assertThatThrownBy(() -> HandlerScan.handlerMethods(child, HANDLER_ANNOTATIONS))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("The Occurrent handler " + ReadableHandlerClassAboveUnreadableSuperclass.class.getName() + "#on(java.lang.String) would never register")
                .hasMessageContaining("since " + missing + " is missing at runtime")
                .hasMessageContaining("so the scan reads only the methods " + child.getName() + " declares")
                .hasMessageContaining("Put " + missing + " on the classpath.");
    }

    @Test
    void handlerMethods_returns_the_handler_the_class_declares_when_a_readable_interface_declares_a_static_method_with_a_handler_annotation_and_a_superclass_declares_a_method_naming_a_class_missing_at_runtime() throws Exception {
        Class<?> child = new LoaderWithoutAbsentType().loadClass(ExtendsUnreadableSuperclassAndImplementsReadableInterfaceWithStaticAnnotatedMethod.class.getName());

        assertThat(scanWithoutThrowing(child)).extracting(Method::getName, method -> method.getDeclaringClass().getName())
                .containsExactly(tuple("own", child.getName()));
    }

    @Test
    void handlerMethods_throws_naming_the_missing_class_when_the_class_file_of_a_superclass_declaring_a_method_naming_it_can_not_be_read() throws Exception {
        Class<?> child = new LoaderWithoutAbsentType(UnreadableSuperclass.class).loadClass(ChildOfUnreadableSuperclass.class.getName());

        assertRefusedForUnreadableClassFile(child, UnreadableSuperclass.class);
    }

    @Test
    void handlerMethods_throws_naming_the_missing_class_when_the_class_file_of_an_interface_declaring_a_method_naming_it_can_not_be_read() throws Exception {
        Class<?> child = new LoaderWithoutAbsentType(UnreadableInterface.class).loadClass(ChildOfUnreadableInterface.class.getName());

        assertRefusedForUnreadableClassFile(child, UnreadableInterface.class);
    }

    @Test
    void handlerMethods_is_empty_when_a_superclass_declares_no_handler_and_a_method_naming_a_class_missing_at_runtime() throws Exception {
        Class<?> child = new LoaderWithoutAbsentType().loadClass(ChildOfUnreadableSuperclassWithoutHandler.class.getName());

        assertThat(scanWithoutThrowing(child)).isEmpty();
    }

    @Test
    void handlerMethods_returns_the_handler_the_class_declares_when_a_superclass_declares_no_handler_and_a_method_naming_a_class_missing_at_runtime() throws Exception {
        Class<?> child = new LoaderWithoutAbsentType().loadClass(DeclaresHandlerAndExtendsUnreadableSuperclassWithoutHandler.class.getName());

        assertThat(scanWithoutThrowing(child)).extracting(Method::getName, method -> method.getDeclaringClass().getName())
                .containsExactly(tuple("own", child.getName()));
    }

    @Test
    void handlerMethods_is_empty_when_an_interface_declares_a_static_method_with_a_handler_annotation_and_a_method_naming_a_class_missing_at_runtime() throws Exception {
        Class<?> child = new LoaderWithoutAbsentType().loadClass(ChildOfUnreadableInterfaceWithStaticAnnotatedMethod.class.getName());

        assertThat(scanWithoutThrowing(child)).isEmpty();
    }

    @Test
    void handlerMethods_returns_the_visibility_bridge_carrying_the_handler_annotation_when_the_package_private_superclass_declaring_the_handler_can_not_be_read() throws Exception {
        Class<?> leaf = new LoaderWithoutAbsentType().loadClass(LeafOverUnreadablePackagePrivateBase.class.getName());

        assertThat(leaf.getDeclaredMethods()).describedAs("the class declares only the visibility bridge")
                .extracting(Method::getName, Method::isBridge).containsExactly(tuple("on", true));
        assertThat(scanWithoutThrowing(leaf)).extracting(Method::getName, Method::isBridge, method -> method.getDeclaringClass().getName(), method -> List.of(method.getParameterTypes()))
                .containsExactly(tuple("on", true, leaf.getName(), List.of(String.class)));
    }

    @Test
    void handlerMethods_throws_naming_the_overload_that_has_no_visibility_bridge_when_the_package_private_superclass_declaring_it_can_not_be_read() throws Exception {
        Class<?> leaf = new LoaderWithoutAbsentType().loadClass(LeafOverUnreadableBaseWithPackagePrivateOverload.class.getName());

        assertThat(leaf.getDeclaredMethods()).describedAs("the class declares a bridge for on(String) only")
                .extracting(Method::getName, Method::isBridge, method -> List.of(method.getParameterTypes())).containsExactly(tuple("on", true, List.of(String.class)));
        assertRefusedForUnreadableHandler(leaf, UnreadableBaseWithPackagePrivateOverload.class, "on(java.lang.Integer)")
                .hasMessageNotContaining("on(java.lang.String)");
    }

    @Test
    void handlerMethods_throws_naming_the_handler_of_the_unreadable_top_class_although_a_visibility_bridge_runs_a_handler_of_the_same_name_below_it() throws Exception {
        Class<?> leaf = new LoaderWithoutAbsentType().loadClass(LeafOverReadablePackagePrivateBelowUnreadableTop.class.getName());

        assertThat(leaf.getDeclaredMethods()).describedAs("the class declares the visibility bridge")
                .extracting(Method::getName, Method::isBridge, method -> List.of(method.getParameterTypes())).containsExactly(tuple("on", true, List.of(String.class)));
        assertRefusedForUnreadableHandler(leaf, UnreadableTop.class, "on(java.lang.Integer)")
                .hasMessageNotContaining("on(java.lang.String)");
    }

    @Test
    void handlerMethods_returns_only_the_handler_the_class_declares_and_not_its_generic_bridge_when_it_re_annotates_a_handler_of_an_unreadable_generic_superclass() throws Exception {
        Class<?> child = new LoaderWithoutAbsentType().loadClass(ReAnnotatesGenericHandlerOfUnreadableSuperclass.class.getName());

        assertThat(child.getDeclaredMethods()).describedAs("the class declares the generic bridge beside the handler")
                .extracting(Method::getName, Method::isBridge).containsExactlyInAnyOrder(tuple("on", false), tuple("on", true));
        assertThat(scanWithoutThrowing(child)).extracting(Method::getName, Method::isBridge, method -> method.getDeclaringClass().getName(), method -> List.of(method.getParameterTypes()))
                .containsExactly(tuple("on", false, child.getName(), List.of(String.class)));
    }

    @Test
    void handlerMethods_throws_naming_the_handler_of_an_unreadable_superclass_when_the_class_overrides_it_without_a_handler_annotation() throws Exception {
        Class<?> child = new LoaderWithoutAbsentType().loadClass(OverridesUnreadableHandlerWithoutAnnotation.class.getName());

        assertRefusedForUnreadableHandler(child, UnreadableSuperclass.class);
    }

    @Test
    void handlerMethods_throws_naming_the_private_handler_of_an_unreadable_superclass_although_the_class_declares_a_handler_with_the_same_signature() throws Exception {
        Class<?> child = new LoaderWithoutAbsentType().loadClass(DeclaresHandlerOfTheSignatureOfAnUnreadablePrivateHandler.class.getName());

        assertRefusedForUnreadableHandler(child, UnreadableSuperclassWithPrivateHandler.class);
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
    void handlerMethods_returns_the_method_a_visibility_bridge_calls_when_the_bridge_is_the_only_copy_the_class_declares_and_an_interface_declares_a_method_naming_a_class_missing_at_runtime() throws Exception {
        Class<?> leaf = new LoaderWithoutAbsentType().loadClass(PublicLeafDeclaringOnlyTheVisibilityBridge.class.getName());

        assertThat(leaf.getDeclaredMethods()).describedAs("the class declares only the visibility bridge")
                .extracting(Method::getName, Method::isBridge).containsExactly(tuple("on", true));
        assertThat(scanWithoutThrowing(leaf)).extracting(Method::getName, Method::isBridge, method -> method.getDeclaringClass().getSimpleName(), method -> List.of(method.getParameterTypes()))
                .containsExactly(tuple("on", false, PackagePrivateUnreadableHierarchyBase.class.getSimpleName(), List.of(String.class)));
    }

    @Test
    void handlerMethods_returns_the_overload_and_the_visibility_bridge_for_the_handler_of_the_package_private_superclass_when_the_class_declares_an_annotated_overload_and_the_superclass_can_not_be_read() throws Exception {
        Class<?> leaf = new LoaderWithoutAbsentType().loadClass(LeafOverloadingAnUnreadableBaseHandler.class.getName());

        assertThat(leaf.getDeclaredMethods()).describedAs("the class declares the overload beside the visibility bridge")
                .extracting(Method::getName, Method::isBridge, method -> List.of(method.getParameterTypes()))
                .containsExactlyInAnyOrder(tuple("on", false, List.of(String.class)), tuple("on", true, List.of(Object.class)));
        assertThat(scanWithoutThrowing(leaf)).extracting(Method::getName, Method::isBridge, method -> method.getDeclaringClass().getName(), method -> List.of(method.getParameterTypes()))
                .containsExactlyInAnyOrder(tuple("on", false, leaf.getName(), List.of(String.class)), tuple("on", true, leaf.getName(), List.of(Object.class)));
    }

    @Test
    void handlerMethods_returns_the_overriding_handler_of_the_class_when_a_class_between_it_and_the_unreadable_declaring_class_overrides_the_package_private_handler_in_the_package_of_it() throws Exception {
        Class<?> leaf = new LoaderWithoutAbsentType().loadClass(LeafOverMidInTheDeclaringPackage.class.getName());

        assertThat(scanWithoutThrowing(leaf)).extracting(Method::getName, method -> method.getDeclaringClass().getName())
                .containsExactly(tuple("on", leaf.getName()));
    }

    @Test
    void handlerMethods_throws_naming_the_package_private_handler_when_the_class_between_it_and_the_class_overriding_it_declares_the_method_in_a_third_package() throws Exception {
        Class<?> leaf = new LoaderWithoutAbsentType().loadClass(LeafOverMidInAnotherPackage.class.getName());

        assertRefusedForUnreadableHandler(leaf, UnreadableBaseWithPackagePrivateHandler.class, "on(java.lang.String)", MissingAtRuntime.class);
    }

    @Test
    void handlerMethods_throws_naming_the_package_private_handler_when_the_class_between_it_and_the_class_overriding_it_can_not_be_read_to_tell_whether_it_overrides_the_handler() throws Exception {
        Class<?> leaf = new LoaderWithoutAbsentType().loadClass(LeafOverUnreadableMidInTheDeclaringPackage.class.getName());

        assertRefusedForUnreadableHandler(leaf, UnreadableBaseWithPackagePrivateHandler.class, "on(java.lang.String)", MissingAtRuntime.class);
    }

    @Test
    void handlerMethods_still_throws_NoClassDefFoundError_when_the_class_itself_declares_a_method_naming_a_class_missing_at_runtime() throws Exception {
        Class<?> declaring = new LoaderWithoutAbsentType().loadClass(DeclaresUnreadableMethod.class.getName());

        assertThatThrownBy(() -> HandlerScan.handlerMethods(declaring, HANDLER_ANNOTATIONS)).isInstanceOf(NoClassDefFoundError.class);
    }

    private static void assertRefusedForUnreadableHandler(Class<?> type, Class<?> unreadable) {
        assertRefusedForUnreadableHandler(type, unreadable, "on(java.lang.String)");
    }

    private static AbstractThrowableAssert<?, ? extends Throwable> assertRefusedForUnreadableHandler(Class<?> type, Class<?> unreadable, String handler) {
        return assertRefusedForUnreadableHandler(type, unreadable, handler, AbsentType.class);
    }

    private static AbstractThrowableAssert<?, ? extends Throwable> assertRefusedForUnreadableHandler(Class<?> type, Class<?> unreadable, String handler, Class<?> absent) {
        String missing = absent.getName();

        return assertThatThrownBy(() -> HandlerScan.handlerMethods(type, HANDLER_ANNOTATIONS))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("The methods of " + unreadable.getName() + " can't be read")
                .hasMessageContaining("since " + missing + " is missing at runtime")
                .hasMessageContaining("its Occurrent handler " + unreadable.getName() + "#" + handler + " would never register")
                .hasMessageContaining("Put " + missing + " on the classpath.")
                .hasCauseInstanceOf(NoClassDefFoundError.class);
    }

    private static void assertRefusedForUnreadableClassFile(Class<?> type, Class<?> unreadable) {
        String missing = AbsentType.class.getName();

        assertThatThrownBy(() -> HandlerScan.handlerMethods(type, HANDLER_ANNOTATIONS))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("The methods of " + unreadable.getName() + " can't be read")
                .hasMessageContaining("since " + missing + " is missing at runtime, and neither can its class file")
                .hasMessageContaining("Put " + missing + " on the classpath.")
                .hasCauseInstanceOf(NoClassDefFoundError.class);
    }

    private static Set<Method> scanWithoutThrowing(Class<?> type) {
        AtomicReference<Set<Method>> scanned = new AtomicReference<>();
        Throwable thrown = catchThrowable(() -> scanned.set(HandlerScan.handlerMethods(type, HANDLER_ANNOTATIONS)));

        assertThat(thrown).describedAs("the scan of a class with an unreadable superclass or interface").isNull();
        return scanned.get();
    }

    // Defines the fixtures itself so the methods that name AbsentType or MissingAtRuntime are read through it, and refuses to load both.
    // It can also hide the class file of a fixture it still defines
    private static final class LoaderWithoutAbsentType extends ClassLoader {
        private final String hiddenResource;

        LoaderWithoutAbsentType() {
            super(HandlerScanUnreadableHierarchyTest.class.getClassLoader());
            this.hiddenResource = null;
        }

        LoaderWithoutAbsentType(Class<?> fixtureWithHiddenClassFile) {
            super(HandlerScanUnreadableHierarchyTest.class.getClassLoader());
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
}
