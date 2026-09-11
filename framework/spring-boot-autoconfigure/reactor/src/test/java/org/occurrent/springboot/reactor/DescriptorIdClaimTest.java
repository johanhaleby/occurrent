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

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.DuplicateSubscriptionIdException;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A {@code @Projection} or {@code @Snapshot} id is added to the coordinator's registry by one call, and the call that gets
 * past that add is the only one entitled to take the id back out. Two registrations declaring the same id used to
 * decide that by reading the registry before the attempt, which both of them read as free, so the one that lost the
 * add took the winner's id back out and left the durable checkpoint key free for a third registration.
 * <p>
 * The window between that read and the add is the whole defect, and no context can stage it. In one thread a read
 * of false is always followed by an add that succeeds, and nothing runs in between on either registration path.
 * These tests drive the coordinator's own registration step instead, which is where the rule now lives, the same
 * way {@code LateRegistrationConcurrencyContractTest} reads the coordinator directly for a hazard it cannot
 * reproduce. Reactive counterpart of the blocking {@code DescriptorIdClaimTest}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DescriptorIdClaimTest {

    @Test
    void a_descriptor_registration_takes_its_id_and_a_later_one_for_that_id_is_refused_without_the_id_being_given_back() throws Exception {
        OccurrentReactiveAnnotationBeanPostProcessor coordinator = new OccurrentReactiveAnnotationBeanPostProcessor();

        registerDescriptor(coordinator, "winner", "shared-id", () -> {
        });

        assertThat(registeredIds(coordinator)).contains("shared-id");
        assertThatThrownBy(() -> registerDescriptor(coordinator, "loser", "shared-id", () -> {
        }))
                .isInstanceOf(DuplicateSubscriptionIdException.class)
                .hasMessageContaining("Duplicate subscription/projection id 'shared-id'");
        assertThat(registeredIds(coordinator)).describedAs("the id the first registration holds").contains("shared-id");
    }

    @Test
    void a_descriptor_registration_that_fails_gives_back_the_id_it_took() throws Exception {
        OccurrentReactiveAnnotationBeanPostProcessor coordinator = new OccurrentReactiveAnnotationBeanPostProcessor();

        assertThatThrownBy(() -> registerDescriptor(coordinator, "failing", "released-id", () -> {
            throw new IllegalStateException("the descriptor factory blew up");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(registeredIds(coordinator)).doesNotContain("released-id");
    }

    // Whatever a registrar spells the field, it has to be able to hold the registry to add to it, so a type check
    // catches a second claim written under any name. Object is left out because a registrar legitimately holds a
    // lock object and takes the bean as Object.
    @Test
    void no_registrar_can_hold_the_coordinators_id_registry() {
        Class<?> registry = ConcurrentHashMap.newKeySet().getClass();
        for (Class<?> registrar : List.of(SubscriptionAnnotationRegistrar.class, ProjectionAnnotationRegistrar.class,
                SnapshotAnnotationRegistrar.class)) {
            for (Field field : registrar.getDeclaredFields()) {
                assertThat(canHold(field.getType(), registry))
                        .describedAs("%s.%s can hold the id registry".formatted(registrar.getSimpleName(), field.getName()))
                        .isFalse();
            }
            for (Constructor<?> constructor : registrar.getDeclaredConstructors()) {
                for (Class<?> parameter : constructor.getParameterTypes()) {
                    assertThat(canHold(parameter, registry))
                            .describedAs("a %s constructor parameter can hold the id registry".formatted(registrar.getSimpleName()))
                            .isFalse();
                }
            }
            for (Method method : registrar.getDeclaredMethods()) {
                for (Class<?> parameter : method.getParameterTypes()) {
                    assertThat(canHold(parameter, registry))
                            .describedAs("a parameter of %s.%s can hold the id registry".formatted(registrar.getSimpleName(), method.getName()))
                            .isFalse();
                }
            }
        }
    }

    private static boolean canHold(Class<?> type, Class<?> registry) {
        return type != Object.class && type.isAssignableFrom(registry);
    }

    // registerDescriptor is private, and it has to be, since the id rule only holds while nothing else adds to the
    // registry. Reaching it reflectively is what lets the rule be asserted at all.
    private static void registerDescriptor(OccurrentReactiveAnnotationBeanPostProcessor coordinator, String beanName, String id, Runnable registration) {
        try {
            Method method = OccurrentReactiveAnnotationBeanPostProcessor.class.getDeclaredMethod(
                    "registerDescriptor", String.class, Method.class, String.class, String.class, Runnable.class);
            method.setAccessible(true);
            Method anyHandlerMethod = DescriptorIdClaimTest.class.getDeclaredMethod("canHold", Class.class, Class.class);
            method.invoke(coordinator, beanName, anyHandlerMethod, id,
                    "Duplicate subscription/projection id '%s', each id must be unique because it is the durable checkpoint key.".formatted(id),
                    registration);
        } catch (InvocationTargetException e) {
            throw e.getCause() instanceof RuntimeException cause ? cause : new IllegalStateException(e.getCause());
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Set<String> registeredIds(OccurrentReactiveAnnotationBeanPostProcessor coordinator) throws Exception {
        Field field = OccurrentReactiveAnnotationBeanPostProcessor.class.getDeclaredField("registeredIds");
        field.setAccessible(true);
        return (Set<String>) field.get(coordinator);
    }
}
