package org.occurrent.eventstore.jpa.utils;

import jakarta.persistence.EntityManagerFactory;
import org.occurrent.eventstore.jpa.batteries.CloudEventDao;
import org.springframework.context.annotation.*;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.Database;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;

//@Configuration
//@ComponentScan("org.occurrent.eventstore.jpa")
//@EnableJpaRepositories("org.occurrent.eventstore.jpa")
//@EnableTransactionManagement
////@Import(OrmConfig.class)

@Configuration
@EnableTransactionManagement
public class OrmConfig {

    @Bean("dataSource")
    DataSource dataSource() {
        return TestDb.dataSource();
    }

    @Bean("entityManagerFactory")
    LocalContainerEntityManagerFactoryBean entityManagerFactory(DataSource dataSource) {
        HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
        vendorAdapter.setDatabase(Database.POSTGRESQL);
        vendorAdapter.setShowSql(true);
        vendorAdapter.setGenerateDdl(true);

        LocalContainerEntityManagerFactoryBean factory = new LocalContainerEntityManagerFactoryBean();
        factory.setJpaVendorAdapter(vendorAdapter);
        factory.setPackagesToScan(CloudEventDao.class.getPackageName());
        factory.setDataSource(dataSource);

        return factory;
    }
    @Bean("transactionManager")
    PlatformTransactionManager transactionManager(
            EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
    }
}


