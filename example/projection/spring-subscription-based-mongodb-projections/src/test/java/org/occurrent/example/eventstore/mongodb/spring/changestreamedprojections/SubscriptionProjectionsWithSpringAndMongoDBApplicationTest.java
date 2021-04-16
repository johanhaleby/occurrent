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

package org.occurrent.example.eventstore.mongodb.spring.changestreamedprojections;

import org.junit.jupiter.api.Test;
import org.occurrent.example.eventstore.mongodb.spring.subscriptionprojections.CurrentName;
import org.occurrent.example.eventstore.mongodb.spring.subscriptionprojections.CurrentNameProjection;
import org.occurrent.example.eventstore.mongodb.spring.subscriptionprojections.NameApplicationService;
import org.occurrent.example.eventstore.mongodb.spring.subscriptionprojections.SubscriptionProjectionsWithSpringAndMongoDBApplication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SuppressWarnings("ResultOfMethodCallIgnored")
@SpringBootTest(classes = SubscriptionProjectionsWithSpringAndMongoDBApplication.class)
@Testcontainers
public class SubscriptionProjectionsWithSpringAndMongoDBApplicationTest {

    @Container
    private static final MongoDBContainer mongoDBContainer;

    static {
        mongoDBContainer = new MongoDBContainer("mongo:4.2.8");
        List<String> ports = new ArrayList<>();
        ports.add("27017:27017");
        mongoDBContainer.withReuse(true).setPortBindings(ports);
    }

    @Autowired
    private NameApplicationService nameApplicationService;

    @Autowired
    private CurrentNameProjection currentNameProjection;

    @Test
    void current_name_projection_is_updated_asynchronously_after_event_has_been_written() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        UUID id = UUID.randomUUID();

        // When
        nameApplicationService.defineName(id, now, "John Doe");

        // Then
        await().atMost(Duration.ofMillis(2000L)).untilAsserted(() ->
                assertThat(currentNameProjection.findById(id.toString())).hasValueSatisfying(cn -> Objects.equals(cn.getName(), "John Doe"))
        );
    }

    @Test
    void current_name_projection_is_updated_asynchronously_after_multiple_events_are_written_to_the_same_stream() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        UUID id = UUID.randomUUID();

        // When
        nameApplicationService.defineName(id, now, "Jane Doe");
        nameApplicationService.changeName(id, now, "John Doe");

        // Then
        await().atMost(Duration.ofMillis(2000L)).untilAsserted(() ->
                assertThat(currentNameProjection.findById(id.toString())).hasValueSatisfying(cn -> Objects.equals(cn.getName(), "John Doe"))
        );
    }
}