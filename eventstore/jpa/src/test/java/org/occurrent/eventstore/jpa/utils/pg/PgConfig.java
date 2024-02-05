package org.occurrent.eventstore.jpa.utils.pg;

import javax.sql.DataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PgConfig {
  @Bean("dataSource")
  DataSource dataSource() {
    return PgTestContainer.dataSourceInternal();
  }
}
