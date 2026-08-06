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

package org.occurrent.example.eventstore.mongodb.spring.projections.adhoc;


import org.junit.jupiter.api.Test;
import org.occurrent.example.eventstore.mongodb.spring.projections.adhoc.MostNumberOfWorkouts.PersonWithMostNumberOfWorkouts;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.LocalDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

@SpringBootTest(classes = SpringAdhocProjectionApplication.class)
@Testcontainers
public class SpringAdhocProjectionTest {
    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    // This application reads its url from configuration rather than from the container, so the container's mapped port
    // has to be published as a property. A literal localhost:27017 in application.yaml would tie the test to a fixed
    // host port, which is what stopped two runs from coexisting.
    @DynamicPropertySource
    static void mongoDbUri(DynamicPropertyRegistry registry) {
        registry.add("spring.mongodb.uri", mongoDBContainer::getReplicaSetUrl);
    }

    @Autowired
    private MostNumberOfWorkouts mostNumberOfWorkouts;

    @Autowired
    private WorkoutRecorder workoutRecorder;

    @Test
    void projections_can_query_the_event_store() {
        // Given
        WorkoutWasCompleted workout1 = new WorkoutWasCompleted(UUID.randomUUID(), LocalDateTime.now(), "cycling", "John Doe");
        WorkoutWasCompleted workout2 = new WorkoutWasCompleted(UUID.randomUUID(), LocalDateTime.now(), "boxing", "Jane Doe");
        WorkoutWasCompleted workout3 = new WorkoutWasCompleted(UUID.randomUUID(), LocalDateTime.now(), "cycling", "Some Doe");
        WorkoutWasCompleted workout4 = new WorkoutWasCompleted(UUID.randomUUID(), LocalDateTime.now(), "running", "John Doe");
        WorkoutWasCompleted workout5 = new WorkoutWasCompleted(UUID.randomUUID(), LocalDateTime.now(), "canoeing", "Jane Doe");
        WorkoutWasCompleted workout6 = new WorkoutWasCompleted(UUID.randomUUID(), LocalDateTime.now(), "cheese-rolling", "John Doe");
        WorkoutWasCompleted workout7 = new WorkoutWasCompleted(UUID.randomUUID(), LocalDateTime.now(), "wingsuite", "Adventure Joe");

        // When
        workoutRecorder.recordWorkoutCompleted(workout1, workout2, workout3, workout4, workout5, workout6, workout7);

        // Then
        PersonWithMostNumberOfWorkouts personWithMostNumberOfWorkouts = mostNumberOfWorkouts.personWithMostNumberOfWorkouts();
        assertAll(
                () -> assertThat(personWithMostNumberOfWorkouts.getName()).isEqualTo("John Doe"),
                () -> assertThat(personWithMostNumberOfWorkouts.getCount()).isEqualTo(3)
        );
    }
}