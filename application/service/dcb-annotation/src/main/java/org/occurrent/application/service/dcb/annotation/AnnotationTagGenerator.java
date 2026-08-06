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
import org.occurrent.annotationsupport.internal.AnnotatedMemberScanException;
import org.occurrent.annotationsupport.internal.AnnotatedMemberScanner;
import org.occurrent.annotationsupport.internal.ScannedMember;
import org.occurrent.application.service.dcb.TagGenerator;
import org.occurrent.eventstore.api.dcb.Tag;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandle;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
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
    private final AnnotatedMemberScanner scanner;
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
        this.scanner = new AnnotatedMemberScanner(annotationType);
        this.annotationType = scanner.annotationType();
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
        this.scanner = new AnnotatedMemberScanner(annotationType);
        this.annotationType = scanner.annotationType();
        requireNonNull(keyResolver, "Key resolver cannot be null");
        this.keyResolver = annotation -> keyResolver.apply(annotationType.cast(annotation));
    }

    @Override
    public Set<Tag> tags(E event) {
        requireNonNull(event);
        List<TagExtractor> extractors = this.cache.computeIfAbsent(event.getClass(), this::buildExtractors);
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

    // Turn the scanner's members into tag extractors, resolving each member's key and letting the getter win
    // when a property is annotated on both its getter and its backing field.
    private List<TagExtractor> buildExtractors(Class<?> clazz) {
        List<ScannedMember> members;
        try {
            members = scanner.scan(clazz);
        } catch (AnnotatedMemberScanException e) {
            throw new AnnotationTagGeneratorException(e.getMessage(), e);
        }
        Map<String, TagExtractor> extractorsByKey = new LinkedHashMap<>();
        for (ScannedMember member : members) {
            String key = resolveKey(member.annotation(), member.propertyName());
            extractorsByKey.putIfAbsent(key, new TagExtractor(key, member.accessor()));
        }
        return List.copyOf(extractorsByKey.values());
    }

    private Object invoke(MethodHandle accessor, Object event) {
        try {
            return accessor.invoke(event);
        } catch (Throwable t) {
            throw new AnnotationTagGeneratorException("Failed to read @" + annotationType.getSimpleName() + " annotated member on " + event.getClass(), t);
        }
    }

    private String resolveKey(Annotation annotation, String defaultName) {
        @Nullable String key = keyResolver.apply(annotation);
        return key == null || key.isBlank() ? defaultName : key;
    }

    private static Function<Annotation, @Nullable String> defaultKeyResolver(Class<? extends Annotation> annotationType) {
        @Nullable Method valueMethod = stringElement(annotationType, "value");
        @Nullable Method keyMethod = stringElement(annotationType, "key");
        if (valueMethod == null && keyMethod == null) {
            return __ -> null;
        }
        // Resolve at read time: value() when it is non-blank, otherwise key().
        return annotation -> {
            @Nullable String value = valueMethod == null ? null : invokeKeyMethod(annotationType, valueMethod, annotation);
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
