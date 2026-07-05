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

package org.occurrent.application.service.dcb.annotation;

import org.jspecify.annotations.NullMarked;
import org.occurrent.annotation.DcbTag;
import org.occurrent.application.service.dcb.TagGenerator;
import org.occurrent.eventstore.api.dcb.Tag;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Objects.requireNonNull;

/**
 * A {@link TagGenerator} that derives DCB {@link Tag tags} from members of the event annotated with
 * {@link DcbTag}, the annotation-driven counterpart to a hand-written {@link TagGenerator}.
 * <p>
 * For a Java record, {@link DcbTag} is placed on the record components. For any other class,
 * including a Kotlin data class, it is placed on a no-arg getter method, or on a field using the
 * Kotlin {@code @get:DcbTag} / {@code @field:DcbTag} use-site targets. A member is read through its
 * accessor where one exists (a record component accessor or a getter), falling back to the field
 * itself when a field is annotated with no matching getter. The accessor is made accessible and
 * bound to a {@link MethodHandle} once, so the event class need not be {@code public}. Under the
 * Java module system the declaring package must be open for reflection (the same requirement
 * reflective libraries such as Jackson have).
 * <p>
 * Each concrete event class is scanned for its {@link DcbTag}-annotated members once; the resulting
 * accessors are cached per {@link Class} for the lifetime of this generator instance and reused for
 * every subsequent event of that class. The cache is per-instance rather than shared globally, so
 * reuse a single {@link AnnotationTagGenerator} instance across events of the same application
 * rather than constructing a new one per event. The cached accessors are immutable and the cache
 * itself is a {@link ConcurrentHashMap}, so a single generator instance is safe to share and use
 * concurrently.
 */
@NullMarked
public final class AnnotationTagGenerator<E> implements TagGenerator<E> {

    private final ConcurrentMap<Class<?>, List<TagExtractor>> cache = new ConcurrentHashMap<>();

    @Override
    public Set<Tag> tags(E event) {
        requireNonNull(event);
        List<TagExtractor> extractors = this.cache.computeIfAbsent(event.getClass(), AnnotationTagGenerator::scan);
        if (extractors.isEmpty()) {
            return Set.of();
        }

        Set<Tag> tags = new LinkedHashSet<>();
        for (TagExtractor extractor : extractors) {
            Object value = invoke(extractor.accessor(), event);
            if (value == null) {
                continue;
            }
            String s = value.toString();
            if (s.isBlank()) {
                continue;
            }
            tags.add(Tag.of(extractor.key(), s));
        }
        return Collections.unmodifiableSet(tags);
    }

    private static Object invoke(MethodHandle accessor, Object event) {
        try {
            return accessor.invoke(event);
        } catch (Throwable t) {
            throw new AnnotationTagGeneratorException("Failed to read @" + DcbTag.class.getSimpleName() + " annotated member on " + event.getClass(), t);
        }
    }

    private static List<TagExtractor> scan(Class<?> clazz) {
        if (clazz.isRecord()) {
            List<TagExtractor> extractors = new ArrayList<>();
            for (RecordComponent rc : clazz.getRecordComponents()) {
                DcbTag annotation = rc.getAnnotation(DcbTag.class);
                if (annotation == null) {
                    continue;
                }
                extractors.add(new TagExtractor(resolveKey(annotation, rc.getName()), unreflect(rc.getAccessor())));
            }
            return List.copyOf(extractors);
        }

        // Methods are scanned before fields so that when a property is annotated on both its getter and its
        // backing field (the Kotlin case) the getter wins and the property yields a single tag.
        Map<String, TagExtractor> extractorsByKey = new LinkedHashMap<>();
        for (Class<?> current = clazz; current != null && current != Object.class; current = current.getSuperclass()) {
            scanMethods(current, extractorsByKey);
            scanFields(current, clazz, extractorsByKey);
        }
        return List.copyOf(extractorsByKey.values());
    }

    private static void scanMethods(Class<?> clazz, Map<String, TagExtractor> extractorsByKey) {
        for (Method method : clazz.getDeclaredMethods()) {
            DcbTag annotation = method.getAnnotation(DcbTag.class);
            if (annotation == null || method.getParameterCount() != 0 || method.isSynthetic()
                    || Modifier.isStatic(method.getModifiers()) || method.getReturnType() == void.class) {
                continue;
            }
            String key = resolveKey(annotation, propertyNameFromGetter(method));
            extractorsByKey.putIfAbsent(key, new TagExtractor(key, unreflect(method)));
        }
    }

    private static void scanFields(Class<?> declaringClass, Class<?> concreteClass, Map<String, TagExtractor> extractorsByKey) {
        for (Field field : declaringClass.getDeclaredFields()) {
            DcbTag annotation = field.getAnnotation(DcbTag.class);
            if (annotation == null || field.isSynthetic() || Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            String key = resolveKey(annotation, field.getName());
            if (extractorsByKey.containsKey(key)) {
                continue;
            }
            // Prefer reading through the getter (Kotlin generates a public getter for every val); fall back to the
            // field itself only when a field is annotated with no matching accessor. The getter is looked up on the
            // concrete class so a getter declared or overridden only on a subclass is still found.
            Method getter = findGetter(concreteClass, field.getName());
            extractorsByKey.put(key, new TagExtractor(key, getter != null ? unreflect(getter) : unreflectField(field)));
        }
    }

    private static Method findGetter(Class<?> clazz, String fieldName) {
        String capitalized = Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
        for (String candidateName : List.of("get" + capitalized, "is" + capitalized, fieldName)) {
            try {
                Method candidate = clazz.getMethod(candidateName);
                if (candidate.getParameterCount() == 0) {
                    return candidate;
                }
            } catch (NoSuchMethodException ignored) {
                // Try the next candidate name.
            }
        }
        return null;
    }

    // Bind an accessor to a MethodHandle, making it accessible first so the event class need not be public.
    // Under the module system this requires the declaring package to be open for reflection.
    private static MethodHandle unreflect(Method accessor) {
        try {
            accessor.setAccessible(true);
            return MethodHandles.lookup().unreflect(accessor);
        } catch (IllegalAccessException | RuntimeException e) {
            throw accessError(accessor.getDeclaringClass(), accessor.toString(), e);
        }
    }

    private static MethodHandle unreflectField(Field field) {
        try {
            field.setAccessible(true);
            return MethodHandles.lookup().unreflectGetter(field);
        } catch (IllegalAccessException | RuntimeException e) {
            throw accessError(field.getDeclaringClass(), field.toString(), e);
        }
    }

    private static AnnotationTagGeneratorException accessError(Class<?> owner, String member, Throwable cause) {
        return new AnnotationTagGeneratorException(
                "Cannot access @" + DcbTag.class.getSimpleName() + " member " + member + " on " + owner.getName()
                        + ". Under the Java module system the declaring package must be open for reflection.", cause);
    }

    private static String propertyNameFromGetter(Method method) {
        String name = method.getName();
        if (name.startsWith("get") && name.length() > 3) {
            return decapitalize(name.substring(3));
        }
        if (name.startsWith("is") && name.length() > 2) {
            return decapitalize(name.substring(2));
        }
        return name;
    }

    private static String decapitalize(String s) {
        return Character.toLowerCase(s.charAt(0)) + s.substring(1);
    }

    private static String resolveKey(DcbTag annotation, String defaultName) {
        String key = annotation.key();
        return key.isBlank() ? defaultName : key;
    }

    private record TagExtractor(String key, MethodHandle accessor) {
    }

    /**
     * Thrown when a {@link DcbTag}-annotated member cannot be scanned or read.
     */
    public static final class AnnotationTagGeneratorException extends RuntimeException {
        AnnotationTagGeneratorException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
