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

import java.util.Objects;

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
        switch (className) {
            case SIMPLE:
                return type.getSimpleName();
            case QUALIFIED:
                return type.getName();
        }
        throw new IllegalStateException("Internal error: Invalid class name setting " + className);
    }

    /**
     * @return An instance of {@link ReflectionCloudEventTypeMapper} that uses the simple name of a class as cloud event type
     */
    public static <T> ReflectionCloudEventTypeMapper<T> simple() {
        return new ReflectionCloudEventTypeMapper<>(ClassName.SIMPLE);
    }

    /**
     * @return An instance of {@link ReflectionCloudEventTypeMapper} that uses the fully qualified name of a class as cloud event type
     */
    public static <T> ReflectionCloudEventTypeMapper<T> qualified() {
        return new ReflectionCloudEventTypeMapper<>(ClassName.QUALIFIED);
    }
}
