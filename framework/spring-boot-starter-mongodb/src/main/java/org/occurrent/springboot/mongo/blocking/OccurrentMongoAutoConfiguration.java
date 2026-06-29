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

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.application.service.blocking.dcb.DcbApplicationService;
import org.occurrent.application.service.blocking.dcb.GenericDcbApplicationService;
import org.occurrent.application.service.blocking.dcb.TagGenerator;
import org.occurrent.application.service.blocking.generic.GenericApplicationService;
import org.occurrent.dsl.dcb.blocking.DcbDomainEventQueries;
import org.occurrent.dsl.dcb.blocking.DcbSubscriptions;
import org.occurrent.dsl.query.blocking.DomainEventQueries;
import org.occurrent.dsl.subscription.blocking.StreamSubscriptions;
import org.occurrent.dsl.subscription.blocking.Subscriptions;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.DcbQuery;
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
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.mongodb.autoconfigure.MongoAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Fallback;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.List;

import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.subscription.blocking.durable.catchup.SubscriptionPositionStorageConfig.useSubscriptionPositionStorage;
import static org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModelConfig.withConfig;

/**
 * Occurrent Spring autoconfiguration support for blocking MongoDB event store and subscriptions
 */
@AutoConfiguration(after = MongoAutoConfiguration.class)
@ConditionalOnClass({SpringMongoEventStore.class, SpringMongoSubscriptionModel.class})
@EnableConfigurationProperties(OccurrentProperties.class)
@Import(Jackson3CloudEventConverterConfiguration.class)
public class OccurrentMongoAutoConfiguration<E> {

    private static final Logger log = LoggerFactory.getLogger(OccurrentMongoAutoConfiguration.class);

    @Bean
    @ConditionalOnProperty(name = "occurrent.subscription.enabled", havingValue = "true", matchIfMissing = true)
    static OccurrentAnnotationBeanPostProcessor occurrentAnnotationBeanPostProcessor() {
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
        return new EventStoreConfig.Builder()
                .eventStoreCollectionName(eventStoreProperties.getCollection())
                .transactionConfig(transactionManager)
                .timeRepresentation(eventStoreProperties.getTimeRepresentation())
                .eventStoreCapabilities(eventStoreProperties.getCapabilities())
                .build();
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
                                                                        OccurrentProperties occurrentProperties, EventStoreQueries eventStoreQueries, ObjectProvider<DcbEventStore> dcbEventStore) {
        EventStoreProperties eventStoreProperties = occurrentProperties.getEventStore();
        SpringMongoSubscriptionModel mongoSubscriptionModel = new SpringMongoSubscriptionModel(mongoTemplate, withConfig(eventStoreProperties.getCollection(), eventStoreProperties.getTimeRepresentation())
                .restartSubscriptionsOnChangeStreamHistoryLost(occurrentProperties.getSubscription().isRestartOnChangeStreamHistoryLost()));
        DurableSubscriptionModel durableSubscriptionModel = new DurableSubscriptionModel(mongoSubscriptionModel, storage);
        CatchupSubscriptionModelConfig catchupConfig = new CatchupSubscriptionModelConfig(useSubscriptionPositionStorage(storage)
                .andPersistSubscriptionPositionDuringCatchupPhaseForEveryNEvents(1000));
        // DCB catch-up replays by dcbposition over the DCB event store. The DcbQuery.all() is shared by every
        // DcbSubscriptions subscription, which each narrow to their own DcbQuery in the consumer, so a single
        // all-matching catch-up is correct. Stream catch-up replays by event time over the stream query API.
        boolean stream = eventStoreProperties.getCapabilities().contains(STREAM);
        DcbEventStore dcbStore = eventStoreProperties.getCapabilities().contains(DCB) ? dcbEventStore.getIfAvailable() : null;
        SubscriptionModel subscriptionModel;
        if (stream && dcbStore != null) {
            // STREAM and DCB together: one dual-mode model routes each subscription to stream or DCB catch-up.
            subscriptionModel = new CatchupSubscriptionModel(durableSubscriptionModel, eventStoreQueries, dcbStore, DcbQuery.all(), catchupConfig);
        } else if (stream) {
            subscriptionModel = new CatchupSubscriptionModel(durableSubscriptionModel, eventStoreQueries, catchupConfig);
        } else if (dcbStore != null) {
            subscriptionModel = new CatchupSubscriptionModel(durableSubscriptionModel, dcbStore, DcbQuery.all(), catchupConfig);
        } else {
            subscriptionModel = durableSubscriptionModel;
        }
        return new CompetingConsumerSubscriptionModel(subscriptionModel, competingConsumerStrategy);
    }

    @Bean
    @Lazy
    @Fallback
    @Conditional(OnMissingCloudEventConverterAndCloudEventTypeMapperCondition.class)
    public CloudEventTypeMapper<E> occurrentTypeMapper() {
        return newDefaultCloudEventTypeMapper();
    }

    @NonNull
    private CloudEventTypeMapper<E> newDefaultCloudEventTypeMapper() {
        return ReflectionCloudEventTypeMapper.qualified();
    }

    @Bean
    @ConditionalOnMissingBean(StreamSubscriptions.class)
    @ConditionalOnProperty(name = "occurrent.subscription.enabled", havingValue = "true", matchIfMissing = true)
    @SuppressWarnings("deprecation") // The released bean type stays Subscriptions for binary compatibility; it is-a StreamSubscriptions, so new code can inject either.
    public Subscriptions<E> occurrentSubscriptionDsl(Subscribable subscribable, CloudEventConverter<E> cloudEventConverter) {
        return new Subscriptions<>(subscribable, cloudEventConverter);
    }

    /**
     * DCB subscription DSL, auto-configured when the DCB event-store capability is enabled. In DCB-only mode the
     * underlying subscription model wraps a {@link CatchupSubscriptionModel} in DCB mode, so a subscription started at a
     * {@code DcbSubscriptionPosition} replays history by dcbposition before switching to live delivery. Started without
     * such a position it is live only, as before.
     */
    @Bean
    @ConditionalOnMissingBean(DcbSubscriptions.class)
    @Conditional(OnDcbEventStoreCapabilityCondition.class)
    @ConditionalOnProperty(name = "occurrent.subscription.enabled", havingValue = "true", matchIfMissing = true)
    public DcbSubscriptions<E> occurrentDcbSubscriptions(SubscriptionModel subscriptionModel, CloudEventConverter<E> cloudEventConverter) {
        return new DcbSubscriptions<>(subscriptionModel, cloudEventConverter);
    }

    @Bean
    @ConditionalOnMissingBean(DomainEventQueries.class)
    @Conditional(OnDomainEventQueriesCapabilityCondition.class)
    @ConditionalOnProperty(name = "occurrent.event-store.enabled", havingValue = "true", matchIfMissing = true)
    public DomainEventQueries<E> occurrentDomainEventQueries(EventStoreQueries eventStoreQueries, CloudEventConverter<E> cloudEventConverter) {
        return new DomainEventQueries<>(eventStoreQueries, cloudEventConverter);
    }

    /**
     * DCB query DSL, auto-configured when the DCB event-store capability is enabled. It wraps the
     * {@link DomainEventQueries} bean so a DCB application gets one object for both DCB and stream queries.
     */
    @Bean
    @ConditionalOnMissingBean(DcbDomainEventQueries.class)
    @Conditional(OnDcbEventStoreCapabilityCondition.class)
    @ConditionalOnProperty(name = "occurrent.event-store.enabled", havingValue = "true", matchIfMissing = true)
    public DcbDomainEventQueries<E> occurrentDcbDomainEventQueries(DomainEventQueries<E> domainEventQueries) {
        return new DcbDomainEventQueries<>(domainEventQueries);
    }

    @Bean
    @ConditionalOnMissingBean(ApplicationService.class)
    @Conditional(OnStreamEventStoreCapabilityCondition.class)
    @ConditionalOnProperty(name = {"occurrent.event-store.enabled", "occurrent.application-service.enabled"}, havingValue = "true", matchIfMissing = true)
    public ApplicationService<E> occurrentApplicationService(EventStore eventStore, CloudEventConverter<E> cloudEventConverter, OccurrentProperties occurrentProperties) {
        boolean enableDefaultRetryStrategy = occurrentProperties.getApplicationService().isEnableDefaultRetryStrategy();
        return enableDefaultRetryStrategy ? new GenericApplicationService<>(eventStore, cloudEventConverter) : new GenericApplicationService<>(eventStore, cloudEventConverter, RetryStrategy.none());
    }

    @Bean
    @Conditional(OnDcbEventStoreCapabilityCondition.class)
    @ConditionalOnProperty(name = {"occurrent.event-store.enabled", "occurrent.application-service.enabled"}, havingValue = "true", matchIfMissing = true)
    static BeanFactoryPostProcessor occurrentDcbApplicationServiceRegistrar() {
        return beanFactory -> {
            if (!(beanFactory instanceof BeanDefinitionRegistry registry)) {
                return;
            }
            boolean hasDcbApplicationService = beanFactory.getBeanNamesForType(DcbApplicationService.class, false, false).length > 0;
            boolean hasTagGenerator = beanFactory.getBeanNamesForType(TagGenerator.class, false, false).length > 0;
            if (hasDcbApplicationService) {
                return;
            }
            if (!hasTagGenerator) {
                log.warn("Occurrent DCB event-store capability is enabled but no {} bean was found, so a {} is not auto-configured. " +
                                "Define a {} bean (it derives the DCB tags written with each event) to enable auto-configuration, or provide your own {} bean.",
                        TagGenerator.class.getName(), DcbApplicationService.class.getName(), TagGenerator.class.getSimpleName(), DcbApplicationService.class.getSimpleName());
                return;
            }
            RootBeanDefinition beanDefinition = new RootBeanDefinition(DcbApplicationService.class);
            beanDefinition.setInstanceSupplier(() -> createDcbApplicationService(beanFactory));
            registry.registerBeanDefinition("occurrentDcbApplicationService", beanDefinition);
        };
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static DcbApplicationService<?> createDcbApplicationService(ConfigurableListableBeanFactory beanFactory) {
        DcbEventStore eventStore = beanFactory.getBean(DcbEventStore.class);
        CloudEventConverter cloudEventConverter = beanFactory.getBean(CloudEventConverter.class);
        TagGenerator tagGenerator = beanFactory.getBean(TagGenerator.class);
        OccurrentProperties occurrentProperties = beanFactory.getBean(OccurrentProperties.class);
        boolean enableDefaultRetryStrategy = occurrentProperties.getApplicationService().isEnableDefaultRetryStrategy();
        if (enableDefaultRetryStrategy) {
            return new GenericDcbApplicationService<>(eventStore, cloudEventConverter, tagGenerator);
        }
        return new GenericDcbApplicationService<>(eventStore, cloudEventConverter, tagGenerator, RetryStrategy.none());
    }
}
