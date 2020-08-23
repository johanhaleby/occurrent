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
import org.occurrent.subscription.api.reactor.PositionAwareReactorSubscription;
import org.occurrent.subscription.api.reactor.ReactorSubscriptionPositionStorage;
import org.occurrent.subscription.mongodb.spring.reactor.SpringReactorSubscriptionForMongoDB;
import org.occurrent.subscription.mongodb.spring.reactor.SpringReactorSubscriptionPositionStorageForMongoDB;
import org.occurrent.subscription.mongodb.spring.reactor.SpringReactorSubscriptionThatStoresSubscriptionPositionInMongoDB;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;

import static com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping.EVERYTHING;

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
    public ReactorSubscriptionPositionStorage reactorSubscriptionPositionStorage(ReactiveMongoOperations mongoOperations) {
        return new SpringReactorSubscriptionPositionStorageForMongoDB(mongoOperations, "subscriptions");
    }

    @Bean
    public PositionAwareReactorSubscription subscription(ReactiveMongoOperations mongoOperations) {
        return new SpringReactorSubscriptionForMongoDB(mongoOperations, "events", TimeRepresentation.RFC_3339_STRING);
    }

    @Bean
    public SpringReactorSubscriptionThatStoresSubscriptionPositionInMongoDB autoPersistingSubscription(PositionAwareReactorSubscription subscription, ReactorSubscriptionPositionStorage storage) {
        return new SpringReactorSubscriptionThatStoresSubscriptionPositionInMongoDB(subscription, storage);
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