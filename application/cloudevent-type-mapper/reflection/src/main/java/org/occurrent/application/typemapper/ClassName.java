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
import java.util.function.Function;

/**
 * The different class name options available for the {@link ReflectionCloudEventTypeMapper}.
 */
public abstract class ClassName {

    private ClassName() {
    }

    public static class Simple<T> extends ClassName {
        public final Function<String, Class<? extends T>> domainEventTypeFromCloudEventType;

        public Simple(Function<String, Class<? extends T>> domainEventTypeFromCloudEventType) {
            Objects.requireNonNull(domainEventTypeFromCloudEventType, "packageNameFromCloudEventType cannot be null");
            this.domainEventTypeFromCloudEventType = domainEventTypeFromCloudEventType;
        }
    }


    public static class Qualified extends ClassName {
    }

    /**
     * Use the simple name of a class to represent the cloud event type.
     * When getting the domain event type from the cloud event, prepend the {@code packageName}.
     * This assumes that <i>all</i> events have the same package name.
     */
    public static <T> Simple<T> simple(String packageName) {
        String packageNameBeforeDots = packageName == null ? "" : packageName.trim();
        final String packageNameToUse;
        if (packageNameBeforeDots.endsWith(".")) {
            packageNameToUse = packageNameBeforeDots;
        } else {
            packageNameToUse = packageNameBeforeDots + ".";
        }

        return simple(simpleName -> {
            try {
                //noinspection unchecked
                return (Class<T>) Class.forName(packageNameToUse + simpleName);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Use the simple name of a class to represent the cloud event type.
     * <p>
     * When getting the domain event type from the cloud event, prepend the {@code packageName} from the supplied {@code domainEventType}.
     * This assumes that <i>all</i> events have the same package name as the {@code domainEventType}.
     */
    public static <T> Simple<T> simple(Class<?> domainEventType) {
        Objects.requireNonNull(domainEventType, "domainEventType cannot be null");
        return simple(domainEventType.getPackage());
    }

    /**
     * Use the name of a this package to represent the cloud event type.
     * <p>
     * When getting the domain event type from the cloud event, prepend the {@code packageName} from the supplied {@code domainEventPackage}.
     * This assumes that <i>all</i> events have the same package name as {@code domainEventPackage}.
     */
    public static <T> Simple<T> simple(Package domainEventPackage) {
        Objects.requireNonNull(domainEventPackage, "domainEventPackage cannot be null");
        return simple(domainEventPackage.getName());
    }

    /**
     * Use the simple name of a class to represent the cloud event type and use the {@code packageNameFromCloudEventType} function
     * to reconstructs the domain event type from the simple class name of the domain event type.
     *
     * @param packageNameFromCloudEventType A function that returns the fully-qualified domain event class name from the simple domain event class name (i.e. the cloud event type).
     */
    public static <T> Simple<T> simple(Function<String, Class<? extends T>> packageNameFromCloudEventType) {
        return new Simple<>(packageNameFromCloudEventType);
    }

    /**
     * Use the (fully) qualified name of a class to represent the cloud event type.
     * This is not recommended in production systems.
     */
    public static Qualified qualified() {
        return new Qualified();
    }
}