package org.occurrent.eventstore.jpa;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.occurrent.eventstore.jpa.batteries.CloudEventDao;
import org.occurrent.eventstore.jpa.batteries.StreamEventDaoConverter;
import org.occurrent.eventstore.jpa.batteries.TestEventLog;
import org.occurrent.eventstore.jpa.batteries.TestEventLogOperations;
import org.occurrent.eventstore.jpa.utils.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ContextConfiguration(
    classes = {JpaBlockingEventStoreTestPostgres.PGConfig.class, RepositoryConfig.class})
@ExtendWith(SpringExtension.class)
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JpaBlockingEventStoreTestPostgres
    extends JpaBlockingEventStoreTestBase<JPAEventStore<CloudEventDao>> {
  static final String USER_NAME = "user";
  static final String PASSWORD = "password";
  static final String DB = "db";

  @Container
  static final PostgreSQLContainer<?> postgres =
      new PostgreSQLContainer<>("postgres:15-alpine")
          .withUsername(USER_NAME)
          .withPassword(PASSWORD)
          .withDatabaseName(DB);

  @TestConfiguration
  static class PGConfig {
    @Bean("dataSource")
    DataSource dataSource() {
      postgres.start();
      return DataSourceBuilder.create()
          .driverClassName("org.postgresql.Driver")
          .url(
              String.format(
                  "jdbc:postgresql://%s:%s/%s?stringtype=unspecified",
                  postgres.getHost(), postgres.getFirstMappedPort(), DB))
          .username(USER_NAME)
          .password(PASSWORD)
          .build();
    }
  }

  @Autowired EntityManagerFactory emf;
  @Autowired TestEventLog log;

  @Override
  JPAEventStore<CloudEventDao> getNewEventStore() {
    return JPAEventStore.<CloudEventDao>builder()
        .eventLog(log)
        .eventLogOperations(new TestEventLogOperations())
        .converter(new StreamEventDaoConverter(new ObjectMapper()))
        .build();
  }

  private void execute(String... sqls) {
    var em = emf.createEntityManager();
    em.getTransaction().begin();

    for (var sql : sqls) {
      em.createNativeQuery(sql).executeUpdate();
    }
    em.getTransaction().commit();
    em.close();
  }

  @BeforeEach
  void truncate() {
    var truncateSQL = "TRUNCATE TABLE \"public\".\"cloud_events\";";
    execute(truncateSQL);
  }

  @BeforeAll
  void create_table() {
    // https://medium.com/dandelion-tutorials/using-spring-data-jpa-for-integration-tests-without-spring-boot-starter-9f87877d098f

    // JPA creates the table wrong. Drop it and create it correctly.
    var dropSql = "DROP TABLE IF EXISTS \"public\".\"cloud_events\";";

    var sql =
        """
          CREATE TABLE "public"."cloud_events" (
              "id" BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
              "stream_revision" BIGINT NOT NULL,
              "stream_id" TEXT NOT NULL,
              "event_uuid" UUID NOT NULL UNIQUE,

              -- TODO uncomment these lines below
              -- Should match the stuff in CloudEventDAO
              -- Should be queryable using the Occurrent DSL (no JPA filter errors)


              -- "source" TEXT NOT NULL,
              -- "type" TEXT NOT NULL,
              "timestamp" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              -- "subject" text,
              -- "data_content_type" text,
              "data" text, --TODO - currently just storing in JSON. Probably wont play nicely with filters.
              -- "data_schema" text,
              -- "spec_version" text,
              CONSTRAINT "cloud-events-pk" PRIMARY KEY ("id")
          );
          """;

    execute(dropSql, sql);
  }
}
