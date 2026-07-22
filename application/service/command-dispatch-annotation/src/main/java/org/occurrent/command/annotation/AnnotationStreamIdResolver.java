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

package org.occurrent.command.annotation;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.command.StreamIdResolver;
import org.occurrent.annotation.TargetStream;

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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Objects.requireNonNull;

/**
 * A {@link StreamIdResolver} that derives the target stream id from an annotated member of the command, the
 * annotation-driven counterpart to a hand-written {@code command -> streamId} function. By default it scans for
 * {@link TargetStream}, but a custom annotation type can be supplied when constructing the resolver. It is the
 * command, write-side mirror of the DCB {@code AnnotationTagGenerator}.
 * <p>
 * It enforces the exactly-one-property contract documented on {@link TargetStream}: a command with no annotated
 * property, or more than one, cannot be routed, and a {@code null} or blank value is likewise an error.
 * <p>
 * For a Java record, the annotation is placed on the record component. For any other class, including a Kotlin data
 * class, it is placed on a no-arg getter method, or on a field using the Kotlin {@code @get:...} / {@code @field:...}
 * use-site targets. A member is read through its accessor where one exists, falling back to the field itself. The
 * accessor is made accessible and bound to a {@link MethodHandle} once, so the command class need not be
 * {@code public}. Under the Java module system the declaring package must be open for reflection.
 * <p>
 * Each concrete command class is scanned once and the resulting accessor cached per {@link Class} for the lifetime of
 * this resolver, so reuse a single instance across commands rather than constructing one per command. The cache is a
 * {@link ConcurrentHashMap} and the cached accessors are immutable, so an instance is safe to share concurrently.
 * <p>
 * A custom annotation type must be annotated with {@code @Retention(RUNTIME)} and needs no elements; the annotation is
 * a marker, and the member's value alone is the stream id.
 *
 * @param <C> the command type
 */
@NullMarked
public final class AnnotationStreamIdResolver<C> implements StreamIdResolver<C> {

    private final Class<? extends Annotation> annotationType;
    private final ConcurrentMap<Class<?>, List<MethodHandle>> cache = new ConcurrentHashMap<>();

    /**
     * Create a resolver that scans for Occurrent's {@link TargetStream} annotation.
     */
    public AnnotationStreamIdResolver() {
        this(TargetStream.class);
    }

    /**
     * Create a resolver that scans for {@code annotationType}.
     *
     * @param annotationType the marker annotation to scan for; must be {@code @Retention(RUNTIME)}
     */
    public AnnotationStreamIdResolver(Class<? extends Annotation> annotationType) {
        this.annotationType = validateAnnotationType(annotationType);
    }

    @Override
    public String streamId(C command) {
        requireNonNull(command, "command cannot be null");
        List<MethodHandle> accessors = cache.computeIfAbsent(command.getClass(), this::scan);
        if (accessors.isEmpty()) {
            throw new AnnotationStreamIdResolverException("No @" + annotationType.getSimpleName() + " member on "
                    + command.getClass().getName() + ", so the target stream id cannot be derived", null);
        }
        if (accessors.size() > 1) {
            throw new AnnotationStreamIdResolverException("Found " + accessors.size() + " @" + annotationType.getSimpleName()
                    + " members on " + command.getClass().getName() + ", but a command has exactly one target stream id", null);
        }
        Object value = invoke(accessors.get(0), command);
        if (value == null) {
            throw new AnnotationStreamIdResolverException("@" + annotationType.getSimpleName() + " member on "
                    + command.getClass().getName() + " is null, so the target stream id cannot be derived", null);
        }
        String streamId = value.toString();
        if (streamId.isBlank()) {
            throw new AnnotationStreamIdResolverException("@" + annotationType.getSimpleName() + " member on "
                    + command.getClass().getName() + " is blank, so the target stream id cannot be derived", null);
        }
        return streamId;
    }

    private Object invoke(MethodHandle accessor, Object command) {
        try {
            return accessor.invoke(command);
        } catch (Throwable t) {
            throw new AnnotationStreamIdResolverException("Failed to read @" + annotationType.getSimpleName()
                    + " annotated member on " + command.getClass(), t);
        }
    }

    private List<MethodHandle> scan(Class<?> clazz) {
        if (clazz.isRecord()) {
            List<MethodHandle> accessors = new ArrayList<>();
            for (RecordComponent rc : clazz.getRecordComponents()) {
                if (rc.getAnnotation(annotationType) != null) {
                    accessors.add(unreflect(rc.getAccessor()));
                }
            }
            return List.copyOf(accessors);
        }

        // Methods are scanned before fields so that when a property is annotated on both its getter and its backing
        // field (the Kotlin case) the getter wins and the property yields a single accessor.
        Map<String, MethodHandle> accessorsByName = new LinkedHashMap<>();
        for (Class<?> current = clazz; current != null && current != Object.class; current = current.getSuperclass()) {
            scanMethods(current, accessorsByName);
            scanFields(current, clazz, accessorsByName);
        }
        return List.copyOf(accessorsByName.values());
    }

    private void scanMethods(Class<?> clazz, Map<String, MethodHandle> accessorsByName) {
        for (Method method : clazz.getDeclaredMethods()) {
            if (method.getAnnotation(annotationType) == null || method.getParameterCount() != 0 || method.isSynthetic()
                    || Modifier.isStatic(method.getModifiers()) || method.getReturnType() == void.class) {
                continue;
            }
            accessorsByName.putIfAbsent(propertyNameFromGetter(method), unreflect(method));
        }
    }

    private void scanFields(Class<?> declaringClass, Class<?> concreteClass, Map<String, MethodHandle> accessorsByName) {
        for (Field field : declaringClass.getDeclaredFields()) {
            if (field.getAnnotation(annotationType) == null || field.isSynthetic() || Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            if (accessorsByName.containsKey(field.getName())) {
                continue;
            }
            // Prefer reading through the getter (Kotlin generates a public getter for every val); fall back to the
            // field itself only when a field is annotated with no matching accessor.
            Method getter = findGetter(concreteClass, field.getName());
            accessorsByName.put(field.getName(), getter != null ? unreflect(getter) : unreflectField(field));
        }
    }

    private static @Nullable Method findGetter(Class<?> clazz, String fieldName) {
        String capitalized = Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
        Set<String> candidateNames = Set.of("get" + capitalized, "is" + capitalized, fieldName);
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

    private AnnotationStreamIdResolverException accessError(Class<?> owner, String member, Throwable cause) {
        return new AnnotationStreamIdResolverException(
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

    /**
     * Thrown when a command's target stream id cannot be derived from its annotated member.
     */
    public static final class AnnotationStreamIdResolverException extends RuntimeException {
        AnnotationStreamIdResolverException(String message, @Nullable Throwable cause) {
            super(message, cause);
        }
    }
}
