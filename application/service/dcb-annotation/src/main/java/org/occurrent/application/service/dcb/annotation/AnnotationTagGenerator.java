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
import org.jspecify.annotations.Nullable;
import org.occurrent.annotation.DcbTag;
import org.occurrent.application.service.dcb.TagGenerator;
import org.occurrent.eventstore.api.dcb.Tag;

import java.beans.Introspector;
import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
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
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * A {@link TagGenerator} that derives DCB {@link Tag tags} from annotated members of the event,
 * the annotation-driven counterpart to a hand-written {@link TagGenerator}. By default it scans
 * for {@link DcbTag}, but a custom annotation type can be supplied when constructing the generator.
 * <p>
 * For a Java record, the annotation is placed on the record components. For any other class,
 * including a Kotlin data class, it is placed on a no-arg getter method, or on a field using the
 * Kotlin {@code @get:...} / {@code @field:...} use-site targets. A member is read through its
 * accessor where one exists (a record component accessor or a getter), falling back to the field
 * itself when a field is annotated with no matching getter. The accessor is made accessible and
 * bound to a {@link MethodHandle} once, so the event class need not be {@code public}. Under the
 * Java module system the declaring package must be open for reflection (the same requirement
 * reflective libraries such as Jackson have).
 * <p>
 * Each concrete event class is scanned for its annotated members once; the resulting
 * accessors are cached per {@link Class} for the lifetime of this generator instance and reused for
 * every subsequent event of that class. The cache is per-instance rather than shared globally, so
 * reuse a single {@link AnnotationTagGenerator} instance across events of the same application
 * rather than constructing a new one per event. The cached accessors are immutable and the cache
 * itself is a {@link ConcurrentHashMap}, so a single generator instance is safe to share and use
 * concurrently.
 * <p>
 * When a custom annotation type is supplied without an explicit key resolver, a no-arg
 * {@code String value()} or {@code String key()} annotation element is used when present and
 * non-blank. If no such element exists, or if it returns a blank value, the member name is used as
 * the tag key. Custom annotations must be annotated with {@code @Retention(RUNTIME)}.
 */
@NullMarked
public final class AnnotationTagGenerator<E> implements TagGenerator<E> {

    private final Class<? extends Annotation> annotationType;
    private final Function<Annotation, @Nullable String> keyResolver;
    private final ConcurrentMap<Class<?>, List<TagExtractor>> cache = new ConcurrentHashMap<>();

    /**
     * Create a generator that scans for Occurrent's {@link DcbTag} annotation.
     */
    public AnnotationTagGenerator() {
        this(DcbTag.class, AnnotationTagGenerator::dcbTagKey);
    }

    // @DcbTag's key is its value(), with key() as an alias. Prefer value, fall back to key, and reject a
    // conflicting pair.
    private static @Nullable String dcbTagKey(DcbTag tag) {
        String value = tag.value();
        String key = tag.key();
        if (!value.isBlank() && !key.isBlank() && !value.equals(key)) {
            throw new AnnotationTagGeneratorException("@" + DcbTag.class.getSimpleName() + " has conflicting value \"" + value
                    + "\" and key \"" + key + "\", set only one", null);
        }
        return value.isBlank() ? key : value;
    }

    /**
     * Create a generator that scans for {@code annotationType}.
     * <p>
     * If the annotation has a no-arg {@code String value()} or {@code String key()} element, its
     * non-blank value is used as the tag key. Otherwise the annotated member's name is used.
     *
     * @param annotationType The annotation type to scan for
     */
    public AnnotationTagGenerator(Class<? extends Annotation> annotationType) {
        this.annotationType = validateAnnotationType(annotationType);
        this.keyResolver = defaultKeyResolver(this.annotationType);
    }

    /**
     * Create a generator that scans for {@code annotationType} and derives explicit tag keys with
     * {@code keyResolver}. If the resolver returns {@code null} or a blank value, the annotated
     * member's name is used.
     *
     * @param annotationType The annotation type to scan for
     * @param keyResolver Function that extracts the explicit tag key from an annotation instance
     * @param <A> The annotation type
     */
    public <A extends Annotation> AnnotationTagGenerator(Class<A> annotationType, Function<? super A, @Nullable String> keyResolver) {
        this.annotationType = validateAnnotationType(annotationType);
        requireNonNull(keyResolver, "Key resolver cannot be null");
        this.keyResolver = annotation -> keyResolver.apply(annotationType.cast(annotation));
    }

    @Override
    public Set<Tag> tags(E event) {
        requireNonNull(event);
        List<TagExtractor> extractors = this.cache.computeIfAbsent(event.getClass(), this::scan);
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
            try {
                tags.add(Tag.of(extractor.key(), s));
            } catch (IllegalArgumentException e) {
                throw new AnnotationTagGeneratorException(
                        "Invalid @" + annotationType.getSimpleName() + " value for key \"" + extractor.key() + "\" on "
                                + event.getClass().getName() + ": " + e.getMessage(), e);
            }
        }
        return Collections.unmodifiableSet(tags);
    }

    private Object invoke(MethodHandle accessor, Object event) {
        try {
            return accessor.invoke(event);
        } catch (Throwable t) {
            throw new AnnotationTagGeneratorException("Failed to read @" + annotationType.getSimpleName() + " annotated member on " + event.getClass(), t);
        }
    }

    private List<TagExtractor> scan(Class<?> clazz) {
        if (clazz.isRecord()) {
            List<TagExtractor> extractors = new ArrayList<>();
            for (RecordComponent rc : clazz.getRecordComponents()) {
                Annotation annotation = rc.getAnnotation(annotationType);
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

    private void scanMethods(Class<?> clazz, Map<String, TagExtractor> extractorsByKey) {
        for (Method method : clazz.getDeclaredMethods()) {
            Annotation annotation = method.getAnnotation(annotationType);
            if (annotation == null || method.getParameterCount() != 0 || method.isSynthetic()
                    || Modifier.isStatic(method.getModifiers()) || method.getReturnType() == void.class) {
                continue;
            }
            String key = resolveKey(annotation, propertyNameFromGetter(method));
            extractorsByKey.putIfAbsent(key, new TagExtractor(key, unreflect(method)));
        }
    }

    private void scanFields(Class<?> declaringClass, Class<?> concreteClass, Map<String, TagExtractor> extractorsByKey) {
        for (Field field : declaringClass.getDeclaredFields()) {
            Annotation annotation = field.getAnnotation(annotationType);
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

    private static @Nullable Method findGetter(Class<?> clazz, String fieldName) {
        String capitalized = Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
        Set<String> candidateNames = Set.of("get" + capitalized, "is" + capitalized, fieldName);
        // Walk the hierarchy from the concrete class up (so a subclass getter wins) and use getDeclaredMethods, which
        // also sees a non-public getter; it is made accessible when bound. getMethod would miss non-public accessors.
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

    // Bind an accessor to a MethodHandle, making it accessible first so the event class need not be public.
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

    private AnnotationTagGeneratorException accessError(Class<?> owner, String member, Throwable cause) {
        return new AnnotationTagGeneratorException(
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

    private String resolveKey(Annotation annotation, String defaultName) {
        @Nullable String key = keyResolver.apply(annotation);
        return key == null || key.isBlank() ? defaultName : key;
    }

    private static <A extends Annotation> Class<A> validateAnnotationType(Class<A> annotationType) {
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

    private static Function<Annotation, @Nullable String> defaultKeyResolver(Class<? extends Annotation> annotationType) {
        Method valueMethod = stringElement(annotationType, "value");
        Method keyMethod = stringElement(annotationType, "key");
        if (valueMethod == null && keyMethod == null) {
            return __ -> null;
        }
        // Resolve at read time: value() when it is non-blank, otherwise key().
        return annotation -> {
            String value = valueMethod == null ? null : invokeKeyMethod(annotationType, valueMethod, annotation);
            if (value != null && !value.isBlank()) {
                return value;
            }
            return keyMethod == null ? null : invokeKeyMethod(annotationType, keyMethod, annotation);
        };
    }

    private static @Nullable Method stringElement(Class<? extends Annotation> annotationType, String name) {
        try {
            Method method = annotationType.getMethod(name);
            return method.getReturnType() == String.class && method.getParameterCount() == 0 ? method : null;
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    private static String invokeKeyMethod(Class<? extends Annotation> annotationType, Method keyMethod, Annotation annotation) {
        try {
            return (String) keyMethod.invoke(annotation);
        } catch (IllegalAccessException | InvocationTargetException | RuntimeException e) {
            throw new AnnotationTagGeneratorException("Failed to read key from @" + annotationType.getSimpleName(), e);
        }
    }

    private record TagExtractor(String key, MethodHandle accessor) {
    }

    /**
     * Thrown when an annotated member cannot be scanned or read.
     */
    public static final class AnnotationTagGeneratorException extends RuntimeException {
        AnnotationTagGeneratorException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
