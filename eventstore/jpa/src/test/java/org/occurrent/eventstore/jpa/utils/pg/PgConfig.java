package org.occurrent.eventstore.jpa.utils.pg;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.sql.DataSource;

@Configuration
public class PgConfig {
    @Bean("dataSource")
    DataSource dataSource() {
        return PgTestContainer.dataSourceInternal();
    }
}
