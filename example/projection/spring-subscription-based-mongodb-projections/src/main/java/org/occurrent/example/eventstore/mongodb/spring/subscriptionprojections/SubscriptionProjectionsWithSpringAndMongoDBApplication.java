/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.example.eventstore.mongodb.spring.subscriptionprojections;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.api.blocking.PositionAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionPositionStorage;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionPositionStorage;
import org.occurrent.subscription.util.blocking.DurableSubscriptionModel;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

/**
 * Bootstrap the application
 */
@SpringBootApplication
@EnableMongoRepositories
public class SubscriptionProjectionsWithSpringAndMongoDBApplication {

    private static final String EVENTS_COLLECTION = "events";

    @Bean
    public MongoTransactionManager transactionManager(MongoDatabaseFactory dbFactory) {
        return new MongoTransactionManager(dbFactory);
    }

    @Bean
    public EventStore eventStore(MongoTemplate mongoTemplate, MongoTransactionManager transactionManager) {
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName(EVENTS_COLLECTION).transactionConfig(transactionManager).timeRepresentation(TimeRepresentation.RFC_3339_STRING).build();
        return new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
    }

    @Bean
    public PositionAwareSubscriptionModel positionAwareSubscriptionModel(MongoTemplate mongoTemplate) {
        return new SpringMongoSubscriptionModel(mongoTemplate, EVENTS_COLLECTION, TimeRepresentation.RFC_3339_STRING);
    }

    @Bean
    public SubscriptionPositionStorage storage(MongoTemplate mongoTemplate) {
        return new SpringMongoSubscriptionPositionStorage(mongoTemplate, "subscriptions");
    }

    @Primary
    @Bean
    public PositionAwareSubscriptionModel autoPersistingSubscriptionModel(PositionAwareSubscriptionModel subscription, SubscriptionPositionStorage storage) {
        return new DurableSubscriptionModel(subscription, storage);
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}