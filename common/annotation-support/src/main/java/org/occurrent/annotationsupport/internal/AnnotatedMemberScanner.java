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

package org.occurrent.annotationsupport.internal;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.beans.Introspector;
import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.RecordComponent;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Objects.requireNonNull;

/**
 * Finds the members of a class annotated with a given marker annotation and binds each to a
 * {@link MethodHandle} for reading. This is the reflection engine shared by the annotation-driven DCB
 * tag generator and command stream-id resolver, which differ only in what they do with the members
 * once found.
 * <p>
 * For a Java record the annotation is placed on the record components. For any other class, including
 * a Kotlin data class, it is placed on a no-arg getter method, or on a field using the Kotlin
 * {@code @get:...} / {@code @field:...} use-site targets. A member is read through its accessor where
 * one exists (a record component accessor or a getter), falling back to the field itself when a field
 * is annotated with no matching getter. Getters are scanned before fields so that a property annotated
 * on both its getter and its backing field yields a single member (the getter). The accessor is made
 * accessible and bound once, so the scanned class need not be {@code public}. Under the Java module
 * system the declaring package must be open for reflection.
 * <p>
 * Each concrete class is scanned once and the result cached per {@link Class} for the lifetime of this
 * scanner. The cache is a {@link ConcurrentHashMap} and the scanned members are immutable, so a single
 * scanner instance is safe to share and use concurrently. Reuse one scanner rather than constructing
 * one per class.
 */
@NullMarked
public final class AnnotatedMemberScanner {

    private final Class<? extends Annotation> annotationType;
    private final ConcurrentMap<Class<?>, List<ScannedMember>> cache = new ConcurrentHashMap<>();

    /**
     * Create a scanner for {@code annotationType}.
     *
     * @param annotationType the marker annotation to scan for, which must be {@code @Retention(RUNTIME)}
     */
    public AnnotatedMemberScanner(Class<? extends Annotation> annotationType) {
        this.annotationType = validateAnnotationType(annotationType);
    }

    /**
     * The annotation type this scanner looks for.
     */
    public Class<? extends Annotation> annotationType() {
        return annotationType;
    }

    /**
     * Find every member of {@code type} annotated with this scanner's annotation. Record components are
     * returned in declaration order. For any other class, getters are scanned before fields, walking from
     * the concrete class up its superclasses, and a property annotated on both is returned once. The order
     * among a single class's own methods or fields is whatever reflection reports, not source order. The
     * result is cached per {@link Class}.
     *
     * @param type the class to scan
     * @return the annotated members, empty when none are annotated
     * @throws AnnotatedMemberScanException if an annotated member cannot be bound for reading
     */
    public List<ScannedMember> scan(Class<?> type) {
        requireNonNull(type, "Type cannot be null");
        return cache.computeIfAbsent(type, this::doScan);
    }

    private List<ScannedMember> doScan(Class<?> clazz) {
        if (clazz.isRecord()) {
            List<ScannedMember> members = new ArrayList<>();
            for (RecordComponent rc : clazz.getRecordComponents()) {
                Annotation annotation = rc.getAnnotation(annotationType);
                if (annotation != null) {
                    members.add(new ScannedMember(rc.getName(), unreflect(rc.getAccessor()), annotation));
                }
            }
            return List.copyOf(members);
        }

        // Members are keyed by property name and getters scanned before fields so that a property
        // annotated on both its getter and its backing field (the Kotlin case) yields a single member.
        Map<String, ScannedMember> membersByName = new LinkedHashMap<>();
        for (Class<?> current = clazz; current != null && current != Object.class; current = current.getSuperclass()) {
            scanMethods(current, membersByName);
            scanFields(current, clazz, membersByName);
        }
        return List.copyOf(membersByName.values());
    }

    private void scanMethods(Class<?> clazz, Map<String, ScannedMember> membersByName) {
        for (Method method : clazz.getDeclaredMethods()) {
            Annotation annotation = method.getAnnotation(annotationType);
            if (annotation == null || method.getParameterCount() != 0 || method.isSynthetic()
                    || Modifier.isStatic(method.getModifiers()) || method.getReturnType() == void.class) {
                continue;
            }
            String name = propertyNameFromGetter(method);
            membersByName.putIfAbsent(name, new ScannedMember(name, unreflect(method), annotation));
        }
    }

    private void scanFields(Class<?> declaringClass, Class<?> concreteClass, Map<String, ScannedMember> membersByName) {
        for (Field field : declaringClass.getDeclaredFields()) {
            Annotation annotation = field.getAnnotation(annotationType);
            if (annotation == null || field.isSynthetic() || Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            if (membersByName.containsKey(field.getName())) {
                continue;
            }
            // Prefer reading through the getter (Kotlin generates a public getter for every val), falling back
            // to the field only when it has no matching accessor. The getter is looked up on the concrete class
            // so a getter declared or overridden only on a subclass is still found.
            Method getter = findGetter(concreteClass, field.getName());
            MethodHandle accessor = getter != null ? unreflect(getter) : unreflectField(field);
            membersByName.put(field.getName(), new ScannedMember(field.getName(), accessor, annotation));
        }
    }

    private static @Nullable Method findGetter(Class<?> clazz, String fieldName) {
        String capitalized = Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
        Set<String> candidateNames = Set.of("get" + capitalized, "is" + capitalized, fieldName);
        // Walk from the concrete class up so a subclass getter wins, and use getDeclaredMethods so a non-public
        // getter is seen too (it is made accessible when bound). getMethod would miss non-public accessors.
        for (Class<?> current = clazz; current != null && current != Object.class; current = current.getSuperclass()) {
            for (Method method : current.getDeclaredMethods()) {
                if (method.getParameterCount() == 0 && !method.isSynthetic() && !Modifier.isStatic(method.getModifiers())
                        && method.getReturnType() != void.class && candidateNames.contains(method.getName())) {
                    return method;
                }
            }
        }
        return null;
    }

    // Bind an accessor to a MethodHandle, making it accessible first so the scanned class need not be public.
    // Under the module system this requires the declaring package to be open for reflection.
    private MethodHandle unreflect(Method accessor) {
        try {
            accessor.setAccessible(true);
            return MethodHandles.lookup().unreflect(accessor);
        } catch (IllegalAccessException | RuntimeException e) {
            throw accessError(accessor.getDeclaringClass(), accessor.toString(), e);
        }
    }

    private MethodHandle unreflectField(Field field) {
        try {
            field.setAccessible(true);
            return MethodHandles.lookup().unreflectGetter(field);
        } catch (IllegalAccessException | RuntimeException e) {
            throw accessError(field.getDeclaringClass(), field.toString(), e);
        }
    }

    private AnnotatedMemberScanException accessError(Class<?> owner, String member, Throwable cause) {
        return new AnnotatedMemberScanException(
                "Cannot access @" + annotationType.getSimpleName() + " member " + member + " on " + owner.getName()
                        + ". Under the Java module system the declaring package must be open for reflection.", cause);
    }

    private static String propertyNameFromGetter(Method method) {
        String name = method.getName();
        if (name.startsWith("get") && name.length() > 3) {
            return Introspector.decapitalize(name.substring(3));
        }
        if (name.startsWith("is") && name.length() > 2) {
            return Introspector.decapitalize(name.substring(2));
        }
        return name;
    }

    private static Class<? extends Annotation> validateAnnotationType(Class<? extends Annotation> annotationType) {
        requireNonNull(annotationType, "Annotation type cannot be null");
        if (!annotationType.isAnnotation()) {
            throw new IllegalArgumentException("Annotation type must be an annotation");
        }
        Retention retention = annotationType.getAnnotation(Retention.class);
        if (retention == null || retention.value() != RetentionPolicy.RUNTIME) {
            throw new IllegalArgumentException("Annotation type must be annotated with @Retention(RUNTIME)");
        }
        return annotationType;
    }
}
