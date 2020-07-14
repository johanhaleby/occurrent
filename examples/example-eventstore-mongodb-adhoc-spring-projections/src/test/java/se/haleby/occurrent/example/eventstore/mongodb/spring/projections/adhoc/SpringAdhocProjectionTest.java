package se.haleby.occurrent.example.eventstore.mongodb.spring.projections.adhoc;


import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import se.haleby.occurrent.example.eventstore.mongodb.spring.projections.adhoc.MostNumberOfWorkouts.PersonWithMostNumberOfWorkouts;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

@SpringBootTest(classes = SpringAdhocProjectionApplication.class)
@Testcontainers
public class SpringAdhocProjectionTest {
    @Container
    private static final MongoDBContainer mongoDBContainer;

    static {
        mongoDBContainer = new MongoDBContainer("mongo:4.2.7");
        List<String> ports = new ArrayList<>();
        ports.add("27017:27017");
        mongoDBContainer.setPortBindings(ports);
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