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

package org.occurrent.example.springevent;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.api.reactor.PositionAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.SubscriptionPositionStorage;
import org.occurrent.subscription.mongodb.spring.reactor.SpringMongoDBSubscription;
import org.occurrent.subscription.mongodb.spring.reactor.SpringMongoDBSubscriptionPositionStorage;
import org.occurrent.subscription.util.reactor.AutoPersistingSubscriptionModel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;

import static com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping.EVERYTHING;

/**
 * Bootstrap the application
 */
@SpringBootApplication
public class ForwardEventsFromMongoDBToSpringApplication {

    @Value("${spring.data.mongodb.uri}")
    private String mongoUri;

    @Bean(destroyMethod = "close")
    public MongoClient mongoClient() {
        return MongoClients.create(connectionString());
    }

    @Bean
    public EventStore eventStore(MongoClient mongoClient) {
        ConnectionString connectionString = connectionString();
        String database = connectionString.getDatabase();
        String collection = connectionString.getCollection();

        return new MongoEventStore(mongoClient, database, collection, new EventStoreConfig(TimeRepresentation.RFC_3339_STRING));
    }

    @Bean
    public SubscriptionPositionStorage reactorSubscriptionPositionStorage(ReactiveMongoOperations mongoOperations) {
        return new SpringMongoDBSubscriptionPositionStorage(mongoOperations, "subscriptions");
    }

    @Bean
    public PositionAwareSubscriptionModel subscriptionModel(ReactiveMongoOperations mongoOperations) {
        return new SpringMongoDBSubscription(mongoOperations, "events", TimeRepresentation.RFC_3339_STRING);
    }

    @Bean
    public AutoPersistingSubscriptionModel autoPersistingSubscription(PositionAwareSubscriptionModel subscription, SubscriptionPositionStorage storage) {
        return new AutoPersistingSubscriptionModel(subscription, storage);
    }

    @Bean
    public ConnectionString connectionString() {
        return new ConnectionString(mongoUri + ".events");
    }

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        // Configure jackson to add type information to each serialized object
        // Allows deserializing interfaces such as DomainEvent
        objectMapper.activateDefaultTyping(new LaissezFaireSubTypeValidator(), EVERYTHING);
        return objectMapper;
    }
}