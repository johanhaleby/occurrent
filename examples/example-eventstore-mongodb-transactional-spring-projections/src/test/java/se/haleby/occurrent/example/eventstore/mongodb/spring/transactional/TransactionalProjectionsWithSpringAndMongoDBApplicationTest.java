package se.haleby.occurrent.example.eventstore.mongodb.spring.transactional;

import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import se.haleby.occurrent.domain.NameDefined;

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
        mongoDBContainer = new MongoDBContainer("mongo:4.2.7");
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
        assertAll(() -> {
            assertThat(currentNameProjection.findById(id.toString())).hasValue(new CurrentName(id.toString(), "John Doe"));
            assertThat(eventStore.loadEventStream(id)).containsExactly(new NameDefined(now, "John Doe"));
        });
    }

    @Test
    void events_are_not_written_stored_when_projection_fails() {
        replaceCurrentNameProjectionWithMock(() -> {
            // Given
            LocalDateTime now = LocalDateTime.now();
            UUID id = UUID.randomUUID();
            given(currentNameProjectionMock.save(any())).willThrow(IllegalArgumentException.class);

            // When
            Throwable throwable = catchThrowable(() -> nameApplicationService.defineName(id, now, "John Doe"));

            // Then
            assertAll(() -> {
                assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException.class);
                assertThat(currentNameProjection.findById(id.toString())).isEmpty();
                assertThat(eventStore.loadEventStream(id)).isNull();
            });
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