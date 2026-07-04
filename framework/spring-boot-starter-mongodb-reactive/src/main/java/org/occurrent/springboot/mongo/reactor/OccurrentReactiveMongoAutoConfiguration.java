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

package org.occurrent.springboot.mongo.reactor;

import org.jspecify.annotations.NonNull;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.dcb.TagGenerator;
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.application.service.reactor.dcb.DcbApplicationService;
import org.occurrent.application.service.reactor.dcb.GenericDcbApplicationService;
import org.occurrent.application.service.reactor.generic.GenericApplicationService;
import org.occurrent.dsl.dcb.reactor.DcbDomainEventQueries;
import org.occurrent.dsl.dcb.reactor.DcbSubscriptions;
import org.occurrent.dsl.query.reactor.DomainEventQueries;
import org.occurrent.dsl.subscription.reactor.StreamSubscriptions;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.EventStoreQueries;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.mongo.common.DcbApplicationServiceDiagnostics;
import org.occurrent.springboot.mongo.common.Jackson3CloudEventConverterConfiguration;
import org.occurrent.springboot.mongo.common.OccurrentProperties;
import org.occurrent.springboot.mongo.common.OccurrentProperties.EventStoreProperties;
import org.occurrent.springboot.mongo.common.OnDcbEventStoreCapabilityCondition;
import org.occurrent.springboot.mongo.common.OnDomainEventQueriesCapabilityCondition;
import org.occurrent.springboot.mongo.common.OnMissingCloudEventConverterAndCloudEventTypeMapperCondition;
import org.occurrent.springboot.mongo.common.OnStreamEventStoreCapabilityCondition;
import org.occurrent.subscription.api.reactor.PositionAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import org.occurrent.subscription.api.reactor.SubscriptionPositionStorage;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModelConfig;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorSubscriptionPositionStorage;
import org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModel;
import org.occurrent.subscription.reactor.durable.catchup.ReactorDcbCatchupSubscriptionModel;
import org.occurrent.subscription.reactor.durable.catchup.ReactorStreamCatchupSubscriptionModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.mongodb.autoconfigure.MongoReactiveAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Fallback;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import reactor.util.retry.Retry;

import static org.occurrent.eventstore.api.EventStoreCapability.DCB;

/**
 * Occurrent Spring autoconfiguration support for the reactive (Project Reactor) MongoDB event store and subscriptions.
 * It mirrors the blocking {@code OccurrentMongoAutoConfiguration} for the reactive stack. Enable it with
 * {@link EnableOccurrentReactive} and the {@code spring-boot-starter-mongodb-reactive} dependency.
 */
@AutoConfiguration(after = MongoReactiveAutoConfiguration.class)
@ConditionalOnClass({ReactorMongoEventStore.class, ReactorMongoSubscriptionModel.class})
@EnableConfigurationProperties(OccurrentProperties.class)
@Import(Jackson3CloudEventConverterConfiguration.class)
public class OccurrentReactiveMongoAutoConfiguration<E> {

    private static final Logger log = LoggerFactory.getLogger(OccurrentReactiveMongoAutoConfiguration.class);

    @Bean
    @ConditionalOnProperty(name = "occurrent.subscription.enabled", havingValue = "true", matchIfMissing = true)
    static OccurrentReactiveAnnotationBeanPostProcessor occurrentReactiveAnnotationBeanPostProcessor() {
        return new OccurrentReactiveAnnotationBeanPostProcessor();
    }

    @Bean
    @ConditionalOnMissingBean(ReactiveMongoTransactionManager.class)
    @ConditionalOnProperty(name = "occurrent.event-store.enabled", havingValue = "true", matchIfMissing = true)
    public ReactiveMongoTransactionManager reactiveMongoTransactionManager(ReactiveMongoDatabaseFactory dbFactory) {
        return new ReactiveMongoTransactionManager(dbFactory);
    }

    @Bean
    @ConditionalOnMissingBean(EventStoreConfig.class)
    @ConditionalOnProperty(name = "occurrent.event-store.enabled", havingValue = "true", matchIfMissing = true)
    public EventStoreConfig occurrentEventStoreConfig(ReactiveMongoTransactionManager transactionManager, OccurrentProperties occurrentProperties) {
        EventStoreProperties eventStoreProperties = occurrentProperties.getEventStore();
        EventStoreConfig.Builder builder = new EventStoreConfig.Builder()
                .eventStoreCollectionName(eventStoreProperties.getCollection())
                .transactionConfig(transactionManager)
                .timeRepresentation(eventStoreProperties.getTimeRepresentation())
                .eventStoreCapabilities(eventStoreProperties.getCapabilities());
        // The property is only applied when set. true enables position explicitly (kept on even for an unpositioned
        // store), false opts a STREAM-only store out. withoutStreamPosition() is rejected with DCB, so skip it then.
        Boolean streamPosition = eventStoreProperties.getStream().getPosition();
        if (Boolean.TRUE.equals(streamPosition)) {
            builder.withStreamPosition();
        } else if (Boolean.FALSE.equals(streamPosition) && !eventStoreProperties.getCapabilities().contains(EventStoreCapability.DCB)) {
            builder.withoutStreamPosition();
        }
        return builder.build();
    }

    @Bean
    @ConditionalOnMissingBean(EventStore.class)
    @ConditionalOnProperty(name = "occurrent.event-store.enabled", havingValue = "true", matchIfMissing = true)
    public ReactorMongoEventStore occurrentReactorMongoEventStore(ReactiveMongoTemplate template, EventStoreConfig eventStoreConfig) {
        return new ReactorMongoEventStore(template, eventStoreConfig);
    }

    @Bean
    @ConditionalOnMissingBean(SubscriptionPositionStorage.class)
    @ConditionalOnProperty(name = "occurrent.subscription.enabled", havingValue = "true", matchIfMissing = true)
    public SubscriptionPositionStorage occurrentSubscriptionPositionStorage(ReactiveMongoOperations mongo, OccurrentProperties occurrentProperties) {
        return new ReactorSubscriptionPositionStorage(mongo, occurrentProperties.getSubscription().getCollection());
    }

    /**
     * The composed reactive subscription model. Unlike the blocking side (which also layers a competing consumer),
     * the reactive stack has no competing-consumer model, so the chain is {@code Durable(Catchup(mongo))} with the
     * durable model on the outside as the {@link Subscribable}/lifecycle authority. A DCB catch-up layer is added when
     * the DCB capability is enabled and a reactive {@link DcbEventStore} is available, giving DCB position replay. For a
     * STREAM-only store that writes position (on by default, opt out with
     * {@code occurrent.event-store.stream.position=false}), a {@link ReactorStreamCatchupSubscriptionModel} layer is
     * added instead, so a {@code @StreamSubscription} started from the beginning replays stream history by position
     * before going live. A combined STREAM+DCB store keeps the DCB catch-up layer only, the two catch-up models each
     * accept only their own filter kind so they are not chained. {@code destroyMethod = "shutdown"} disposes the running
     * subscriptions on context close.
     */
    @Bean(destroyMethod = "shutdown")
    @ConditionalOnMissingBean({SubscriptionModel.class, Subscribable.class})
    @ConditionalOnProperty(name = "occurrent.subscription.enabled", havingValue = "true", matchIfMissing = true)
    public ReactorDurableSubscriptionModel occurrentDurableSubscriptionModel(ReactiveMongoOperations mongo, SubscriptionPositionStorage storage,
                                                                             OccurrentProperties occurrentProperties, ObjectProvider<DcbEventStore> dcbEventStore,
                                                                             ObjectProvider<ReactorMongoEventStore> reactorEventStore) {
        EventStoreProperties eventStoreProperties = occurrentProperties.getEventStore();
        ReactorMongoSubscriptionModel mongoSubscriptionModel = new ReactorMongoSubscriptionModel(mongo, eventStoreProperties.getCollection(), eventStoreProperties.getTimeRepresentation(),
                ReactorMongoSubscriptionModelConfig.withConfig().restartSubscriptionsOnChangeStreamHistoryLost(occurrentProperties.getSubscription().isRestartOnChangeStreamHistoryLost()));
        // DCB catch-up replays by position over the DCB event store. The DcbQuery.all() is shared by every
        // DcbSubscriptions subscription, which each narrow to their own DcbQuery in the consumer, so a single
        // all-matching catch-up is correct.
        DcbEventStore dcbStore = eventStoreProperties.getCapabilities().contains(DCB) ? dcbEventStore.getIfAvailable() : null;
        final PositionAwareSubscriptionModel inner;
        if (dcbStore != null) {
            inner = new ReactorDcbCatchupSubscriptionModel(mongoSubscriptionModel, dcbStore, DcbQuery.all());
        } else {
            ReactorMongoEventStore eventStore = reactorEventStore.getIfAvailable();
            if (eventStore != null && eventStore.writesPosition()) {
                // STREAM-only store with position on: replay stream history by position. Filter.all() is narrowed by
                // each subscription's own filter, like the DCB catch-up wiring above.
                inner = new ReactorStreamCatchupSubscriptionModel(mongoSubscriptionModel, eventStore, Filter.all());
            } else {
                inner = mongoSubscriptionModel;
            }
        }
        return new ReactorDurableSubscriptionModel(inner, storage);
    }

    @Bean
    @Lazy
    @Fallback
    @Conditional(OnMissingCloudEventConverterAndCloudEventTypeMapperCondition.class)
    public CloudEventTypeMapper<E> occurrentTypeMapper() {
        return ReflectionCloudEventTypeMapper.qualified();
    }

    @Bean
    @ConditionalOnMissingBean(StreamSubscriptions.class)
    @ConditionalOnProperty(name = "occurrent.subscription.enabled", havingValue = "true", matchIfMissing = true)
    public StreamSubscriptions<E> occurrentStreamSubscriptions(Subscribable subscribable, CloudEventConverter<E> cloudEventConverter) {
        return new StreamSubscriptions<>(subscribable, cloudEventConverter);
    }

    /**
     * DCB subscription DSL, auto-configured when the DCB event-store capability is enabled. In DCB mode the underlying
     * subscription model wraps a {@link ReactorDcbCatchupSubscriptionModel}, so a subscription started at a
     * {@code GlobalSubscriptionPosition} replays history by position before switching to live delivery.
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
        return enableDefaultRetryStrategy ? new GenericApplicationService<>(eventStore, cloudEventConverter) : new GenericApplicationService<>(eventStore, cloudEventConverter, Retry.max(0));
    }

    /**
     * See {@link DcbApplicationServiceDiagnostics} for why this resolves {@link TagGenerator} through
     * {@link ObjectProvider} instead of {@code @ConditionalOnBean(TagGenerator.class)}, and why returning {@code null}
     * is the correct way to express "not auto-configured" here.
     */
    @Bean
    @ConditionalOnMissingBean(DcbApplicationService.class)
    @Conditional(OnDcbEventStoreCapabilityCondition.class)
    @ConditionalOnProperty(name = {"occurrent.event-store.enabled", "occurrent.application-service.enabled"}, havingValue = "true", matchIfMissing = true)
    public DcbApplicationService<E> occurrentDcbApplicationService(DcbEventStore eventStore, CloudEventConverter<E> cloudEventConverter,
                                                                     ObjectProvider<TagGenerator<E>> tagGeneratorProvider, OccurrentProperties occurrentProperties) {
        TagGenerator<E> tagGenerator = tagGeneratorProvider.getIfAvailable();
        if (tagGenerator == null) {
            DcbApplicationServiceDiagnostics.warnTagGeneratorMissing(log, DcbApplicationService.class);
            return null;
        }
        boolean enableDefaultRetryStrategy = occurrentProperties.getApplicationService().isEnableDefaultRetryStrategy();
        return enableDefaultRetryStrategy
                ? new GenericDcbApplicationService<>(eventStore, cloudEventConverter, tagGenerator)
                : new GenericDcbApplicationService<>(eventStore, cloudEventConverter, tagGenerator, Retry.max(0));
    }
}
