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

package org.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringBlockingMongoEventStore;
import org.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.infrastructure.Serialization;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.api.blocking.BlockingSubscriptionPositionStorage;
import org.occurrent.subscription.api.blocking.PositionAwareBlockingSubscription;
import org.occurrent.subscription.mongodb.spring.blocking.SpringBlockingSubscriptionForMongoDB;
import org.occurrent.subscription.mongodb.spring.blocking.SpringBlockingSubscriptionPositionStorageForMongoDB;
import org.occurrent.subscription.util.blocking.BlockingSubscriptionWithAutomaticPositionPersistence;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.retry.annotation.EnableRetry;

import java.net.URI;

/**
 * Bootstrap the application
 */
@SpringBootApplication
@EnableRetry
@EnableMongoRepositories
@EnableRabbit
public class Bootstrap {
    private static final String NUMBER_GUESSING_GAME_TOPIC = "number-guessing-game";
    private static final String EVENTS_COLLECTION_NAME = "events";

    public static void main(String[] args) {
        SpringApplication.run(Bootstrap.class, args);
    }

    @Bean
    public MongoTransactionManager transactionManager(MongoDatabaseFactory dbFactory) {
        return new MongoTransactionManager(dbFactory);
    }

    @Bean
    public SpringBlockingMongoEventStore eventStore(MongoTemplate template, MongoTransactionManager transactionManager) {
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName(EVENTS_COLLECTION_NAME).transactionConfig(transactionManager).timeRepresentation(TimeRepresentation.DATE).build();
        return new SpringBlockingMongoEventStore(template, eventStoreConfig);
    }

    @Bean
    public PositionAwareBlockingSubscription subscription(MongoTemplate mongoTemplate) {
        return new SpringBlockingSubscriptionForMongoDB(mongoTemplate, EVENTS_COLLECTION_NAME, TimeRepresentation.DATE);
    }

    @Bean
    public BlockingSubscriptionPositionStorage storage(MongoTemplate mongoTemplate) {
        return new SpringBlockingSubscriptionPositionStorageForMongoDB(mongoTemplate, "subscriptions");
    }

    @Bean
    public BlockingSubscriptionWithAutomaticPositionPersistence subscriptionWithAutomaticPersistence(PositionAwareBlockingSubscription subscription, BlockingSubscriptionPositionStorage storage) {
        return new BlockingSubscriptionWithAutomaticPositionPersistence(subscription, storage);
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public Serialization serialization(ObjectMapper objectMapper) {
        return new Serialization(objectMapper, URI.create("urn:occurrent:domain:numberguessinggame"));
    }

    @Bean
    public MessageConverter amqpMessageConverter(ObjectMapper objectMapper) {
        return new Jackson2JsonMessageConverter(objectMapper);
    }

    @Bean
    TopicExchange numberGuessingGameTopic() {
        return new TopicExchange(NUMBER_GUESSING_GAME_TOPIC);
    }
}