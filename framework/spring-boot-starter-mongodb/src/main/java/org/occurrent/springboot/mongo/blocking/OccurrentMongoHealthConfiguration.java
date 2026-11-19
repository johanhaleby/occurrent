/*
 * Copyright 2026 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.occurrent.springboot.mongo.blocking;

import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.health.autoconfigure.contributor.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.health.contributor.CompositeHealthContributor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Two health contributors when {@code spring-boot-health} is on the classpath. {@code competingConsumers} has one
 * indicator per subscription of each competing consumer subscription model, down when the wrapped model refused
 * the subscription and while a call for it keeps failing. {@code competingConsumerLeases} has one indicator per MongoDB
 * lease strategy bean, down while its scheduled lease refresh fails. Each is turned off by
 * {@code management.health.competingconsumers.enabled=false} and
 * {@code management.health.competingconsumerleases.enabled=false}, or by {@code management.health.defaults.enabled=false}
 * unless its own key is {@code true}. Neither is in a probe group unless the application adds it to one.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(CompositeHealthContributor.class)
class OccurrentMongoHealthConfiguration {

    // On the method, since Spring loads every condition class on the configuration class before evaluating any,
    // which fails without spring-boot-health before @ConditionalOnClass can skip it. Spring Boot names a
    // contributor after the bean without its HealthContributor suffix. An application bean under any of the three
    // names that give the same contributor name replaces the starter's, since two fail startup
    @Bean
    @ConditionalOnEnabledHealthIndicator("competingconsumers")
    @ConditionalOnMissingBean(name = {"competingConsumers", "competingConsumersHealthIndicator",
            "competingConsumersHealthContributor"})
    CompositeHealthContributor competingConsumersHealthContributor(ConfigurableListableBeanFactory beanFactory) {
        return new CompetingConsumersHealthContributor(beanFactory);
    }

    @Bean
    @ConditionalOnEnabledHealthIndicator("competingconsumerleases")
    @ConditionalOnMissingBean(name = {"competingConsumerLeases", "competingConsumerLeasesHealthIndicator",
            "competingConsumerLeasesHealthContributor"})
    CompositeHealthContributor competingConsumerLeasesHealthContributor(ConfigurableListableBeanFactory beanFactory) {
        return new CompetingConsumerLeasesHealthContributor(beanFactory);
    }
}
