/*
 * Copyright 2021 Johan Haleby
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

import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.domain.DomainEvent;
import tools.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.api.reactor.PositionAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.SubscriptionPositionStorage;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorSubscriptionPositionStorage;
import org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;

import java.net.URI;

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
        return new ReactorSubscriptionPositionStorage(mongoOperations, "subscriptions");
    }

    @Bean
    public PositionAwareSubscriptionModel subscriptionModel(ReactiveMongoOperations mongoOperations) {
        return new ReactorMongoSubscriptionModel(mongoOperations, "events", TimeRepresentation.RFC_3339_STRING);
    }

    @Bean
    public ReactorDurableSubscriptionModel autoPersistingSubscription(PositionAwareSubscriptionModel subscription, SubscriptionPositionStorage storage) {
        return new ReactorDurableSubscriptionModel(subscription, storage);
    }

    @Bean
    public ConnectionString connectionString() {
        return new ConnectionString(mongoUri + ".events");
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public CloudEventConverter<DomainEvent> domainEventConverter(ObjectMapper objectMapper) {
        return new JacksonCloudEventConverter.Builder<DomainEvent>(objectMapper, URI.create("http://name"))
                .typeMapper(ReflectionCloudEventTypeMapper.simple(DomainEvent.class))
                .build();
    }
}
