/*
 *
 *  Copyright 2021 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.application.typemapper;

import org.occurrent.application.typemapper.ClassName.Qualified;
import org.occurrent.application.typemapper.ClassName.Simple;

import java.util.Objects;
import java.util.function.Function;

/**
 * A reflection-based {@link CloudEventTypeMapper} that uses either the qualified or simple name of a domain event class
 * as cloud event type.
 *
 * @param <T> The base-type of your domain events
 */
public class ReflectionCloudEventTypeMapper<T> implements CloudEventTypeMapper<T> {
    private final ClassName className;

    public ReflectionCloudEventTypeMapper(ClassName className) {
        Objects.requireNonNull(className, ClassName.class.getSimpleName() + " cannot be null");
        this.className = className;
    }

    @Override
    public String getCloudEventType(Class<? extends T> type) {
        final String cloudEventType;
        if (className instanceof Simple) {
            cloudEventType = type.getSimpleName();
        } else if (className instanceof Qualified) {
            cloudEventType = type.getName();
        } else {
            throw new IllegalStateException("Internal error: Invalid class name setting " + className);
        }

        return cloudEventType;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <E extends T> Class<E> getDomainEventType(String cloudEventType) {
        final Class<E> domainEvenType;
        if (className instanceof Simple) {
            domainEvenType = (Class<E>) ((Simple<T>) className).domainEventTypeFromCloudEventType.apply(cloudEventType);
        } else if (className instanceof Qualified) {
            try {
                //noinspection unchecked
                domainEvenType = (Class<E>) Class.forName(cloudEventType);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new IllegalStateException("Internal error: Invalid class name setting " + className);
        }
        return domainEvenType;
    }

    /**
     * Create an instance of {@link ReflectionCloudEventTypeMapper} that uses the simple name of a class as cloud event type.
     * <p>
     * When getting the domain event type from the cloud event, the {@link ReflectionCloudEventTypeMapper} will prepend the {@code packageName} from the supplied {@code domainEventType}.
     * This assumes that <i>all</i> events have the same package name as the {@code domainEventType}.
     *
     * @return An instance of {@link ReflectionCloudEventTypeMapper} that uses the simple name of a class as cloud event type
     */
    public static <T> ReflectionCloudEventTypeMapper<T> simple(Class<T> domainEventType) {
        return new ReflectionCloudEventTypeMapper<>(ClassName.simple(domainEventType));
    }

    /**
     * Create an instance of {@link ReflectionCloudEventTypeMapper} that uses the simple name of a class as cloud event type.
     * <p>
     * When getting the domain event type from the cloud event, the {@link ReflectionCloudEventTypeMapper} will prepend the {@code packageName} from the supplied {@code domainEventType}.
     * This assumes that <i>all</i> events have the same package name as the {@code domainEventType}.
     *
     * @return An instance of {@link ReflectionCloudEventTypeMapper} that uses the simple name of a class as cloud event type
     */
    public static <T> ReflectionCloudEventTypeMapper<T> simple(Function<String, Class<? extends T>> packageNameFromCloudEventType) {
        return new ReflectionCloudEventTypeMapper<>(ClassName.simple(packageNameFromCloudEventType));
    }

    /**
     * @return An instance of {@link ReflectionCloudEventTypeMapper} that uses the fully qualified name of a class as cloud event type
     */
    public static <T> ReflectionCloudEventTypeMapper<T> qualified() {
        return new ReflectionCloudEventTypeMapper<>(ClassName.qualified());
    }

    public static <T> ReflectionCloudEventTypeMapper<T> fromClassName(ClassName className) {
        return new ReflectionCloudEventTypeMapper<>(className);
    }
}
