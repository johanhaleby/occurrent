package org.occurrent.eventstore.jpa.utils;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@Configuration
@ComponentScan("org.occurrent.eventstore.jpa")
@EnableJpaRepositories("org.occurrent.eventstore.jpa")
@Import(OrmConfig.class)
public class RepositoryConfig {

}
