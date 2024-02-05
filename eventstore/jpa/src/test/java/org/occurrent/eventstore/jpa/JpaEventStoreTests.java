package org.occurrent.eventstore.jpa;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import jakarta.persistence.EntityManager;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.runner.RunWith;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStream;
import org.occurrent.eventstore.jpa.batteries.CloudEventDao;
import org.occurrent.eventstore.jpa.batteries.StreamEventDaoConverter;
import org.occurrent.eventstore.jpa.batteries.TestEventLog;
import org.occurrent.eventstore.jpa.batteries.TestEventLogOperations;
import org.occurrent.eventstore.jpa.utils.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

//@Testcontainers
//@DataJpaTest
//@Import(TestConfig.class)
//@SpringBootTest(classes = TestConfig.class)
//@ContextConfiguration(TestConfig.class)
//@RunWith(SpringRunner.class)

//@EnableAutoConfiguration
//@ContextConfiguration(classes = TestConfig.class)
//@DataJpaTest
//@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)

//@ContextConfiguration(classes = TestConfig.class)
//@RunWith(SpringJUnit4ClassRunner.class)
//@SpringBootTest(classes = TestConfig.class)
@ContextConfiguration(classes = RepositoryConfig.class)
//@RunWith(SpringJUnit4ClassRunner.class)
@ExtendWith(SpringExtension.class)
class JpaEventStoreTests {

  @Autowired EntityManager em;
  @Autowired TestEventLog log;
  EventStore eventStore;

  TestHelper testHelper;

  @BeforeEach
  void create_entity_manager() {
    //https://medium.com/dandelion-tutorials/using-spring-data-jpa-for-integration-tests-without-spring-boot-starter-9f87877d098f
    eventStore =
        JPAEventStore.<CloudEventDao>builder()
            .eventLog(log)
            .eventLogOperations(new TestEventLogOperations())
            .converter(new StreamEventDaoConverter(new ObjectMapper()))
            .build();
    TestDb.initializeTables(em);
    testHelper = new TestHelper(eventStore);
  }

  @AfterEach
  void cleanup() {}

  @Test
  void does_not_change_event_store_content_when_writing_an_empty_stream_of_events() {
    // When
    testHelper.persist("name", Stream.empty());

    // Then
    EventStream<CloudEvent> eventStream = eventStore.read("name");

    assertThat(eventStream.isEmpty()).isTrue();
  }
}
