/*
 *
 *  Copyright 2021 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.springboot.mongo.blocking;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import org.jetbrains.annotations.NotNull;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.application.service.blocking.generic.GenericApplicationService;
import org.occurrent.dsl.query.blocking.DomainEventQueries;
import org.occurrent.dsl.subscription.blocking.Subscriptions;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.springboot.mongo.blocking.OccurrentProperties.EventStoreProperties;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy.CompetingConsumerListener;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionPositionStorage;
import org.occurrent.subscription.blocking.competingconsumers.CompetingConsumerSubscriptionModel;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelConfig;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoLeaseCompetingConsumerStrategy;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionPositionStorage;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.List;
import java.util.Optional;

import static org.occurrent.subscription.blocking.durable.catchup.SubscriptionPositionStorageConfig.useSubscriptionPositionStorage;
import static org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModelConfig.withConfig;

/**
 * Occurrent Spring autoconfiguration support for blocking MongoDB event store and subscriptions
 */
@AutoConfiguration(after = MongoAutoConfiguration.class)
@ConditionalOnClass({SpringMongoEventStore.class, SpringMongoSubscriptionModel.class})
@EnableConfigurationProperties(OccurrentProperties.class)
public class OccurrentMongoAutoConfiguration<E> {

    @Bean
    OccurrentAnnotationBeanPostProcessor occurrentAnnotationBeanPostProcessor() {
        return new OccurrentAnnotationBeanPostProcessor();
    }

    @Bean
    @ConditionalOnMissingBean(MongoTransactionManager.class)
    @ConditionalOnProperty(name = "occurrent.event-store.enabled", havingValue = "true", matchIfMissing = true)
    public MongoTransactionManager mongoTransactionManager(MongoDatabaseFactory dbFactory) {
        return new MongoTransactionManager(dbFactory, TransactionOptions.builder().readConcern(ReadConcern.MAJORITY).writeConcern(WriteConcern.MAJORITY).build());
    }

    @Bean
    @ConditionalOnMissingBean(EventStoreConfig.class)
    @ConditionalOnProperty(name = "occurrent.event-store.enabled", havingValue = "true", matchIfMissing = true)
    public EventStoreConfig occurrentEventStoreConfig(MongoTransactionManager transactionManager, OccurrentProperties occurrentProperties) {
        EventStoreProperties eventStoreProperties = occurrentProperties.getEventStore();
        return new EventStoreConfig.Builder().eventStoreCollectionName(eventStoreProperties.getCollection()).transactionConfig(transactionManager).timeRepresentation(eventStoreProperties.getTimeRepresentation()).build();
    }

    @Bean
    @ConditionalOnMissingBean(SpringMongoEventStore.class)
    @ConditionalOnProperty(name = "occurrent.event-store.enabled", havingValue = "true", matchIfMissing = true)
    public SpringMongoEventStore occurrentSpringMongoEventStore(MongoTemplate template, EventStoreConfig eventStoreConfig) {
        return new SpringMongoEventStore(template, eventStoreConfig);
    }

    @Bean
    @ConditionalOnMissingBean(SubscriptionPositionStorage.class)
    @ConditionalOnProperty(name = "occurrent.subscription.enabled", havingValue = "true", matchIfMissing = true)
    public SubscriptionPositionStorage occurrentSubscriptionPositionStorage(MongoTemplate mongoTemplate, OccurrentProperties occurrentProperties) {
        return new SpringMongoSubscriptionPositionStorage(mongoTemplate, occurrentProperties.getSubscription().getCollection());
    }

    @Bean
    @ConditionalOnMissingBean(SpringMongoLeaseCompetingConsumerStrategy.class)
    @ConditionalOnProperty(name = "occurrent.subscription.enabled", havingValue = "true", matchIfMissing = true)
    public SpringMongoLeaseCompetingConsumerStrategy occurrentCompetingConsumerStrategy(MongoTemplate mongoTemplate, List<CompetingConsumerListener> competingConsumerListeners) {
        SpringMongoLeaseCompetingConsumerStrategy strategy = SpringMongoLeaseCompetingConsumerStrategy.withDefaults(mongoTemplate);
        competingConsumerListeners.forEach(strategy::addListener);
        return strategy;
    }

    @Bean
    @ConditionalOnMissingBean(SubscriptionModel.class)
    @ConditionalOnProperty(name = "occurrent.subscription.enabled", havingValue = "true", matchIfMissing = true)
    public SubscriptionModel occurrentCompetingDurableSubscriptionModel(MongoTemplate mongoTemplate, SpringMongoLeaseCompetingConsumerStrategy competingConsumerStrategy, SubscriptionPositionStorage storage,
                                                                        OccurrentProperties occurrentProperties, EventStoreQueries eventStoreQueries) {
        EventStoreProperties eventStoreProperties = occurrentProperties.getEventStore();
        SpringMongoSubscriptionModel mongoSubscriptionModel = new SpringMongoSubscriptionModel(mongoTemplate, withConfig(eventStoreProperties.getCollection(), eventStoreProperties.getTimeRepresentation())
                .restartSubscriptionsOnChangeStreamHistoryLost(occurrentProperties.getSubscription().isRestartOnChangeStreamHistoryLost()));
        DurableSubscriptionModel durableSubscriptionModel = new DurableSubscriptionModel(mongoSubscriptionModel, storage);
        // TODO THINK ABOUT BACKWARD COMPATIBILTIY! ALla subscriptions med StartAt default kommer efter denna ändring att börja läsa från tidernas begynnelse (om det inte finns en position sparad).
        // Det vill man nog inte. Vad händer om man gör subscribe "now"?! Kan vi komma på ett bättre sett, tex att Catchup INTE börjar läsa upp vid "default", utan endast om man explicit
        // säger beginning of time?! Det är nog enda lösningen. Vid default med CatchupSubscriptionModel, delegera alltid till wrapped!!! Gör också så att även om man valt "beggingin of time" så ska den fortsätta
        // från vad CatchupSubscriptionModel har skrivit ner (alltså om position är TimeBased), det är ju bara time-based i storage om man kraschat, och då vill man fortsätta därifrån.
        CatchupSubscriptionModel catchupSubscriptionModel = new CatchupSubscriptionModel(durableSubscriptionModel, eventStoreQueries,
                new CatchupSubscriptionModelConfig(useSubscriptionPositionStorage(storage)
                        .andPersistSubscriptionPositionDuringCatchupPhaseForEveryNEvents(1000)));
        return new CompetingConsumerSubscriptionModel(catchupSubscriptionModel, competingConsumerStrategy);
    }

    @Bean
    @ConditionalOnMissingBean(CloudEventConverter.class)
    @ConditionalOnBean(CloudEventTypeMapper.class)
    public CloudEventConverter<E> occurrentCloudEventConverter(Optional<ObjectMapper> objectMapper, OccurrentProperties occurrentProperties, CloudEventTypeMapper<E> cloudEventTypeMapper) {
        ObjectMapper om = objectMapper.orElseGet(ObjectMapper::new);
        return new JacksonCloudEventConverter.Builder<E>(om, occurrentProperties.getCloudEventConverter().getCloudEventSource())
                .typeMapper(cloudEventTypeMapper)
                .build();
    }

    @Bean
    @Conditional(OnMissingCloudEventConverterAndCloudEventTypeMapperCondition.class)
    public CloudEventTypeMapper<E> occurrentTypeMapper() {
        return newDefaultCloudEventTypeMapper();
    }

    @NotNull
    private CloudEventTypeMapper<E> newDefaultCloudEventTypeMapper() {
        return ReflectionCloudEventTypeMapper.qualified();
    }

    @Bean
    @ConditionalOnMissingBean(Subscriptions.class)
    @ConditionalOnProperty(name = "occurrent.subscription.enabled", havingValue = "true", matchIfMissing = true)
    public Subscriptions<E> occurrentSubscriptionDsl(Subscribable subscribable, CloudEventConverter<E> cloudEventConverter) {
        return new Subscriptions<>(subscribable, cloudEventConverter);
    }

    @Bean
    @ConditionalOnMissingBean(DomainEventQueries.class)
    @ConditionalOnProperty(name = "occurrent.event-store.enabled", havingValue = "true", matchIfMissing = true)
    public DomainEventQueries<E> occurrentDomainEventQueries(EventStoreQueries eventStoreQueries, CloudEventConverter<E> cloudEventConverter) {
        return new DomainEventQueries<>(eventStoreQueries, cloudEventConverter);
    }

    @Bean
    @ConditionalOnMissingBean(ApplicationService.class)
    @ConditionalOnProperty(name = "occurrent.event-store.enabled", havingValue = "true", matchIfMissing = true)
    public ApplicationService<E> occurrentApplicationService(EventStore eventStore, CloudEventConverter<E> cloudEventConverter, OccurrentProperties occurrentProperties) {
        boolean enableDefaultRetryStrategy = occurrentProperties.getApplicationService().isEnableDefaultRetryStrategy();
        return enableDefaultRetryStrategy ? new GenericApplicationService<>(eventStore, cloudEventConverter) : new GenericApplicationService<>(eventStore, cloudEventConverter, RetryStrategy.none());
    }
}