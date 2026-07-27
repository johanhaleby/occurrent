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
import org.occurrent.annotation.TargetStreamId;
import org.occurrent.annotationsupport.internal.AnnotatedMemberScanException;
import org.occurrent.annotationsupport.internal.AnnotatedMemberScanner;
import org.occurrent.annotationsupport.internal.ScannedMember;
import org.occurrent.command.StreamIdResolver;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandle;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * A {@link StreamIdResolver} that derives the target stream id from an annotated member of the command, the
 * annotation-driven counterpart to a hand-written {@code command -> streamId} function. By default it scans for
 * {@link TargetStreamId}, but a custom annotation type can be supplied when constructing the resolver. It is the
 * command, write-side mirror of the DCB {@code AnnotationTagGenerator}.
 * <p>
 * It enforces the exactly-one-property contract documented on {@link TargetStreamId}: a command with no annotated
 * property, or more than one, cannot be routed, and a {@code null} or blank value is likewise an error.
 * <p>
 * For a Java record, the annotation is placed on the record component. For any other class, including a Kotlin data
 * class, it is placed on a no-arg getter method, or on a field using the Kotlin {@code @get:...} / {@code @field:...}
 * use-site targets. A member is read through its accessor where one exists, falling back to the field itself. The
 * accessor is made accessible and bound to a {@link MethodHandle} once, so the command class need not be
 * {@code public}. Under the Java module system the declaring package must be open for reflection.
 * <p>
 * Each concrete command class is scanned once and the result cached per {@link Class} for the lifetime of this
 * resolver, so reuse a single instance across commands rather than constructing one per command. The scanner is
 * thread-safe and the cached members are immutable, so an instance is safe to share concurrently.
 * <p>
 * A custom annotation type must be annotated with {@code @Retention(RUNTIME)} and needs no elements. The annotation is
 * a marker, and the member's value alone is the stream id.
 *
 * @param <C> the command type
 */
@NullMarked
public final class AnnotationStreamIdResolver<C> implements StreamIdResolver<C> {

    private final Class<? extends Annotation> annotationType;
    private final AnnotatedMemberScanner scanner;

    /**
     * Create a resolver that scans for Occurrent's {@link TargetStreamId} annotation.
     */
    public AnnotationStreamIdResolver() {
        this(TargetStreamId.class);
    }

    /**
     * Create a resolver that scans for {@code annotationType}.
     *
     * @param annotationType the marker annotation to scan for, must be {@code @Retention(RUNTIME)}
     */
    public AnnotationStreamIdResolver(Class<? extends Annotation> annotationType) {
        this.scanner = new AnnotatedMemberScanner(annotationType);
        this.annotationType = scanner.annotationType();
    }

    @Override
    public String streamId(C command) {
        requireNonNull(command, "command cannot be null");
        List<ScannedMember> members = scan(command.getClass());
        if (members.isEmpty()) {
            throw new AnnotationStreamIdResolverException("No @" + annotationType.getSimpleName() + " member on "
                    + command.getClass().getName() + ", so the target stream id cannot be derived", null);
        }
        if (members.size() > 1) {
            throw new AnnotationStreamIdResolverException("Found " + members.size() + " @" + annotationType.getSimpleName()
                    + " members on " + command.getClass().getName() + ", but a command has exactly one target stream id", null);
        }
        Object value = invoke(members.get(0).accessor(), command);
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

    private List<ScannedMember> scan(Class<?> commandType) {
        try {
            return scanner.scan(commandType);
        } catch (AnnotatedMemberScanException e) {
            throw new AnnotationStreamIdResolverException(e.getMessage(), e);
        }
    }

    private Object invoke(MethodHandle accessor, Object command) {
        try {
            return accessor.invoke(command);
        } catch (Throwable t) {
            throw new AnnotationStreamIdResolverException("Failed to read @" + annotationType.getSimpleName()
                    + " annotated member on " + command.getClass(), t);
        }
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
