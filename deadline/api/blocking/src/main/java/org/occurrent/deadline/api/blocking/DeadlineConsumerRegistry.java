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

public interface DeadlineConsumerRegistry {
    DeadlineConsumerRegistry register(String category, DeadlineConsumer<Object> deadlineConsumer);

    DeadlineConsumerRegistry unregister(String category);

    DeadlineConsumerRegistry unregisterAll();

    <T> Optional<DeadlineConsumer<T>> getConsumer(String category);

    default <T> DeadlineConsumerRegistry register(String category, Class<T> type, DeadlineConsumer<T> deadlineConsumer) {
        return register(category, (id, category1, deadline, data) -> deadlineConsumer.accept(id, category1, deadline, type.cast(data)));
    }
}