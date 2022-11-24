/*
 *
 *  Copyright 2022 Johan Haleby
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

package org.occurrent.deadline.api.blocking;

import java.util.Optional;

/**
 * A {@code DeadlineConsumerRegistry} is a place where you can register consumers that'll be notified when a deadline has occurred.
 * There can only be one {@link DeadlineConsumer} per {@code category}. You are expected to {@link #register(String, DeadlineConsumer)}
 * {@link DeadlineConsumer} every time the application starts.
 */
public interface DeadlineConsumerRegistry {

    /**
     * Register a {@link DeadlineConsumer} that will be invoked for each deadline that occur for a given category.
     *
     * @param category         The category of the deadline (specified when scheduling a deadline)
     * @param deadlineConsumer The consumer that'll be invoked when a deadline occurs for the given category.
     * @return The {@link DeadlineConsumerRegistry} instance.
     */
    DeadlineConsumerRegistry register(String category, DeadlineConsumer<Object> deadlineConsumer);

    /**
     * Unregister a {@link DeadlineConsumer} from being invoked a given category.
     *
     * @param category The category of the deadline (specified when scheduling a deadline)
     * @return The {@link DeadlineConsumerRegistry} instance.
     */
    DeadlineConsumerRegistry unregister(String category);

    /**
     * Unregister all {@link DeadlineConsumer}'s
     *
     * @return The {@link DeadlineConsumerRegistry} instance.
     */
    DeadlineConsumerRegistry unregisterAll();

    /**
     * Get the {@link DeadlineConsumer}, if any, that is registered to receive deadlines for the given {@code category}.
     *
     * @return The {@link DeadlineConsumerRegistry} instance.
     */
    <T> Optional<DeadlineConsumer<T>> getConsumer(String category);

    /**
     * Register a {@link DeadlineConsumer} that is expected to contain data of a specific type.
     * It'll be invoked for each deadline that occur for a given category.
     *
     * @param category         The category of the deadline (specified when scheduling a deadline)
     * @param deadlineConsumer The consumer that'll be invoked when a deadline occurs for the given category.
     * @param <T>              The type of data in the {@link DeadlineConsumer}
     * @return The {@link DeadlineConsumerRegistry} instance.
     */
    default <T> DeadlineConsumerRegistry register(String category, Class<T> type, DeadlineConsumer<T> deadlineConsumer) {
        return register(category, (id, category1, deadline, data) -> deadlineConsumer.accept(id, category1, deadline, type.cast(data)));
    }

    /**
     * Check if the given {@code category} has a {@link DeadlineConsumer} registered.
     *
     * @return <code>true</code> if a {@link DeadlineConsumer} is registered for the supplied {@code category}, <code>false</code> otherwise.
     */
    default boolean hasConsumer(String category) {
        return getConsumer(category).isPresent();
    }
}