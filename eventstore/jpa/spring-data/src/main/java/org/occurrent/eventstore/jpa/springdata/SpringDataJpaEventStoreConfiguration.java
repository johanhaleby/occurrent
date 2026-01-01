package org.occurrent.eventstore.jpa.springdata;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@Configuration
@EnableJpaRepositories
@ComponentScan
public class SpringDataJpaEventStoreConfiguration {

}
