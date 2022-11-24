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

package org.occurrent.deadline.inmemory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.deadline.api.blocking.Deadline;

import java.util.Date;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertAll;

@DisplayNameGeneration(ReplaceUnderscores.class)
public class InMemoryDeadlineTest {

    private InMemoryDeadlineConsumerRegistry deadlineConsumerRegistry;
    private InMemoryDeadlineScheduler deadlineScheduler;

    @BeforeEach
    void initialize() {
        BlockingDeque<Object> queue = new LinkedBlockingDeque<>();
        deadlineConsumerRegistry = new InMemoryDeadlineConsumerRegistry(queue);
        deadlineScheduler = new InMemoryDeadlineScheduler(queue);
    }

    @AfterEach
    void shutdown() {
        deadlineScheduler.shutdown();
        deadlineConsumerRegistry.shutdown();
    }

    @Test
    void inmemory_typed_deadline_registry() {
        // Given
        AtomicReference<ConsumedData<MyDTO>> completed = new AtomicReference<>();
        UUID deadlineId = UUID.randomUUID();

        deadlineConsumerRegistry.register("Something", MyDTO.class, (id, category, deadline, data) -> completed.set(new ConsumedData<>(id, category, deadline, data)));

        // When
        deadlineScheduler.schedule(deadlineId, "Something", Deadline.afterMillis(500), new MyDTO("something"));

        // Then
        ConsumedData<MyDTO> consumedData = await().atMost(2, TimeUnit.SECONDS).untilAtomic(completed, not(nullValue()));
        assertAll(
                () -> assertThat(consumedData.id).isEqualTo(deadlineId.toString()),
                () -> assertThat(consumedData.category).isEqualTo("Something"),
                () -> assertThat(consumedData.deadline.toDate()).isCloseTo(new Date(), 5000),
                () -> assertThat(consumedData.data.something).isEqualTo("something")
        );
    }

    static class ConsumedData<T> {
        final String id;
        final String category;
        final Deadline deadline;
        final T data;

        ConsumedData(String id, String category, Deadline deadline, T data) {
            this.id = id;
            this.category = category;
            this.deadline = deadline;
            this.data = data;
        }
    }

    static class MyDTO {
        private String something;

        @SuppressWarnings("unused")
        MyDTO() {
        }

        MyDTO(String something) {
            this.something = something;
        }

        public String getSomething() {
            return something;
        }

        public void setSomething(String something) {
            this.something = something;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", MyDTO.class.getSimpleName() + "[", "]")
                    .add("something='" + something + "'")
                    .toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof MyDTO)) return false;
            MyDTO myDTO = (MyDTO) o;
            return Objects.equals(something, myDTO.something);
        }

        @Override
        public int hashCode() {
            return Objects.hash(something);
        }
    }
}