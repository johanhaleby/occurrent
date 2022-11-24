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

import java.util.Objects;
import java.util.UUID;

/**
 * A {@code DeadlineScheduler} is the place where {@link Deadline}'s are scheduled. You can register {@link DeadlineConsumer}'s
 * in a {@link DeadlineConsumerRegistry} which will be invoked once the {@link Deadline} is up.
 */
public interface DeadlineScheduler {

    /**
     * Schedule a deadline that will take place in the future.
     *
     * @param id       The unique id of the deadline
     * @param category The deadline category, for example "invoice-reminder"
     * @param deadline The actual date/time of when the deadline takes places
     * @param data     Data associated with the deadline.
     */
    void schedule(String id, String category, Deadline deadline, Object data);

    /**
     * Schedule a deadline that will take place in the future.
     *
     * @param id       The unique id of the deadline
     * @param category The deadline category, for example "invoice-reminder"
     * @param deadline The actual date/time of when the deadline takes places
     * @param data     Data associated with the deadline.
     */
    default void schedule(UUID id, String category, Deadline deadline, Object data) {
        Objects.requireNonNull(id, "id cannot be null");
        schedule(id.toString(), category, deadline, data);
    }

    /**
     * Cancel a deadline, it will no longer be applied in the future.
     */
    void cancel(String id);

    /**
     * Cancel a deadline, it will no longer be applied in the future.
     */
    default void cancel(UUID id) {
        Objects.requireNonNull(id, "id cannot be null");
        cancel(id.toString());
    }
}
