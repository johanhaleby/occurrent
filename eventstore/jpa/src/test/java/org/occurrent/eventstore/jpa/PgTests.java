package org.occurrent.eventstore.jpa;


import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.EntityManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.jpa.batteries.CloudEventDao;
import org.occurrent.eventstore.jpa.batteries.StreamEventDaoConverter;
import org.occurrent.eventstore.jpa.batteries.TestEventLog;
import org.occurrent.eventstore.jpa.batteries.TestEventLogOperations;
import org.occurrent.eventstore.jpa.utils.*;
import org.occurrent.eventstore.jpa.utils.pg.PgConfig;
import org.occurrent.eventstore.jpa.utils.pg.PgTestContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

// import org.springframework.test.context.ContextConfiguration;
// import org.springframework.test.context.junit.jupiter.SpringExtension;

// @Testcontainers
// @DataJpaTest
// @Import(TestConfig.class)
// @SpringBootTest(classes = TestConfig.class)
// @ContextConfiguration(TestConfig.class)
// @RunWith(SpringRunner.class)

// @EnableAutoConfiguration
// @ContextConfiguration(classes = TestConfig.class)
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)

// @ContextConfiguration(classes = TestConfig.class)
// @RunWith(SpringJUnit4ClassRunner.class)
// @SpringBootTest(classes = TestConfig.class)
// @ContextConfiguration(classes = RepositoryConfig.class)
// @RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {PgConfig.class, RepositoryConfig.class})
@ExtendWith(SpringExtension.class)
class PgTests extends BaseTest {

  @Autowired EntityManager em;
  @Autowired TestEventLog log;
  EventStore eventStore;

  TestHelper testHelper;

  @BeforeEach
  void create_entity_manager() {
    // https://medium.com/dandelion-tutorials/using-spring-data-jpa-for-integration-tests-without-spring-boot-starter-9f87877d098f
    eventStore =
        JPAEventStore.<CloudEventDao>builder()
            .eventLog(log)
            .eventLogOperations(new TestEventLogOperations())
            .converter(new StreamEventDaoConverter(new ObjectMapper()))
            .build();
    PgTestContainer.initializeTables(em);
    testHelper = new TestHelper(eventStore);
  }

  @AfterEach
  void cleanup() {}

  @Override
  protected TestHelper testHelper() {
    return testHelper;
  }

  //  @Test
  //  void does_not_change_event_store_content_when_writing_an_empty_stream_of_events() {
  //    // When
  //    testHelper.persist("name", Stream.empty());
  //
  //    // Then
  //    EventStream<CloudEvent> eventStream = eventStore.read("name");
  //
  //    assertThat(eventStream.isEmpty()).isTrue();
  //  }
}
