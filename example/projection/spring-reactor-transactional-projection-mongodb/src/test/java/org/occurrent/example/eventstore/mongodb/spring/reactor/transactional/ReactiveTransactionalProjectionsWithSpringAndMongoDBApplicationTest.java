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

package org.occurrent.example.eventstore.mongodb.spring.reactor.transactional;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.api.reactor.EventStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

@SpringBootTest(classes = TransactionalProjectionsWithSpringAndMongoDBApplication.class)
@Testcontainers
public class ReactiveTransactionalProjectionsWithSpringAndMongoDBApplicationTest {

    @Container
    private static final MongoDBContainer mongoDBContainer;

    static {
        mongoDBContainer = new MongoDBContainer("mongo:4.2.8");
        List<String> ports = new ArrayList<>();
        ports.add("27017:27017");
        mongoDBContainer.withReuse(true).setPortBindings(ports);
    }

    @BeforeAll
    static void enableOnOperatorDebug() {
        Hooks.onOperatorDebug();
    }

    @AfterAll
    static void disableOnOperatorDebug() {
        Hooks.resetOnOperatorDebug();
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
        nameApplicationService.defineName(id, now, "John Doe").block();

        // Then
        assertAll(
                () -> assertThat(currentNameProjection.findById(id.toString()).block()).isEqualTo(new CurrentName(id.toString(), "John Doe")),
                () -> Assertions.assertThat(requireNonNull(eventStore.loadEventStream(id).block()).eventList().block()).containsExactly(new NameDefined(id.toString(), now, "John Doe"))
        );
    }

    @Test
    void can_load_current_events_and_write_new_ones() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        UUID id = UUID.randomUUID();
        nameApplicationService.defineName(id, now, "Jane Doe").block();

        // When
        nameApplicationService.changeName(id, now, "John Doe").block();

        // Then
        assertAll(
                () -> assertThat(currentNameProjection.findById(id.toString()).block()).isEqualTo(new CurrentName(id.toString(), "John Doe")),
                () -> Assertions.assertThat(requireNonNull(eventStore.loadEventStream(id).block()).eventList().block())
                        .extracting("name")
                        .containsExactly("Jane Doe", "John Doe")
        );
    }

    @Test
    void events_are_not_written_when_projection_fails() {
        replaceCurrentNameProjectionWithMock(() -> {
            // Given
            LocalDateTime now = LocalDateTime.now();
            UUID id = UUID.randomUUID();
            given(currentNameProjectionMock.save(any())).willReturn(Mono.error(new IllegalArgumentException("expected")));

            // When
            Throwable throwable = catchThrowable(() -> nameApplicationService.defineName(id, now, "John Doe").block());

            // Then
            Mono<EventStream<DomainEvent>> eventStream = eventStore.loadEventStream(id);

            assertAll(
                    () -> assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException.class),
                    () -> assertThat(currentNameProjection.findById(id.toString()).block()).isNull(),
                    () -> assertThat(requireNonNull(eventStream.block()).isEmpty()).isTrue()
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