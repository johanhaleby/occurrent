/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.example.eventstore.mongodb.spring.transactional;

import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.api.blocking.EventStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

@SpringBootTest(classes = TransactionalProjectionsWithSpringAndMongoDBApplication.class)
@Testcontainers
public class TransactionalProjectionsWithSpringAndMongoDBApplicationTest {

    @Container
    private static final MongoDBContainer mongoDBContainer;

    static {
        mongoDBContainer = new MongoDBContainer("mongo:4.2.8").withReuse(true);
        List<String> ports = new ArrayList<>();
        ports.add("27017:27017");
        mongoDBContainer.setPortBindings(ports);
    }

    @Autowired
    private NameApplicationService nameApplicationService;

    @Autowired
    private CurrentNameProjection currentNameProjection;

    @Mock
    private CurrentNameProjection currentNameProjectionMock;

    @Autowired
    private DomainEventStore eventStore;

    @Test
    void write_events_and_projection_in_the_same_tx() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        UUID id = UUID.randomUUID();

        // When
        nameApplicationService.defineName(id, now, "John Doe");

        // Then
        assertAll(
                () -> assertThat(currentNameProjection.findById(id.toString())).hasValue(new CurrentName(id.toString(), "John Doe")),
                () -> assertThat(eventStore.loadEventStream(id).events()).containsExactly(new NameDefined(id.toString(), now, "John Doe"))
        );
    }

    @Test
    void events_are_not_written_when_projection_fails() {
        replaceCurrentNameProjectionWithMock(() -> {
            // Given
            LocalDateTime now = LocalDateTime.now();
            UUID id = UUID.randomUUID();
            given(currentNameProjectionMock.save(any())).willThrow(IllegalArgumentException.class);

            // When
            Throwable throwable = catchThrowable(() -> nameApplicationService.defineName(id, now, "John Doe"));

            // Then
            EventStream<DomainEvent> eventStream = eventStore.loadEventStream(id);
            assertAll(
                    () -> assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException.class),
                    () -> assertThat(currentNameProjection.findById(id.toString())).isEmpty(),
                    () -> assertThat(eventStream.isEmpty()).isTrue(),
                    () -> assertThat(eventStream).isEmpty()
            );
        });
    }

    void replaceCurrentNameProjectionWithMock(Runnable runnable) {
        nameApplicationService.setCurrentNameProjection(currentNameProjectionMock);
        try {
            runnable.run();
        } finally {
            nameApplicationService.setCurrentNameProjection(currentNameProjection);
        }
    }
}