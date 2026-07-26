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
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.blocking.SynchronousEventDispatcher;
import org.occurrent.application.service.blocking.TransactionExecutor;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.application.service.blocking.dcb.DcbApplicationService;
import org.occurrent.application.service.blocking.dcb.GenericDcbApplicationService;
import org.occurrent.application.service.blocking.generic.GenericApplicationService;
import org.occurrent.application.service.dcb.TagGenerator;
import org.occurrent.application.service.dcb.annotation.AnnotationTagGenerator;
import org.occurrent.application.service.spring.SpringTransactionExecutor;
import org.occurrent.command.StreamIdResolver;
import org.occurrent.command.annotation.AnnotationStreamIdResolver;
import org.occurrent.dsl.dcb.blocking.DcbDomainEventQueries;
import org.occurrent.dsl.dcb.blocking.DcbSubscriptions;
import org.occurrent.dsl.query.blocking.DomainEventQueries;
import org.occurrent.dsl.saga.SagaInstances;
import org.occurrent.dsl.saga.SagaInstancesRegistry;
import org.occurrent.dsl.saga.internal.SagaInstancesRegistryImpl;
import org.occurrent.dsl.subscription.blocking.StreamSubscriptions;
import org.occurrent.dsl.subscription.blocking.Subscriptions;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.springboot.common.*;
import org.occurrent.springboot.common.OccurrentProperties.EventStoreProperties;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy.CompetingConsumerListener;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.blocking.competingconsumers.CompetingConsumerSubscriptionModel;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelConfig;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoLeaseCompetingConsumerStrategy;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModelConfig;
import org.occurrent.subscription.synchronous.blocking.SynchronousSubscriptionModel;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.mongodb.autoconfigure.MongoAutoConfiguration;
import org.springframework.context.annotation.*;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.List;

import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;
import static org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModelConfig.withConfig;

/**
 * Occurrent Spring autoconfiguration support for blocking MongoDB event store and subscriptions
 */
@AutoConfiguration(after = MongoAutoConfiguration.class)
@ConditionalOnClass({SpringMongoEventStore.class, SpringMongoSubscriptionModel.class})
@EnableConfigurationProperties(OccurrentProperties.class)
@Import(Jackson3CloudEventConverterConfiguration.class)
public class OccurrentMongoAutoConfiguration<E> {

    @Bean
    @ConditionalOnProperty(name = "occurrent.subscription.enabled", havingValue = "true", matchIfMissing = true)
    static OccurrentBlockingAnnotationBeanPostProcessor occurrentBlockingAnnotationBeanPostProcessor() {
        return new OccurrentBlockingAnnotationBeanPostProcessor();
    }

    /**
     * Lets an application observe the instances of every {@code @Saga} in the context. It is defined here, rather than
     * registered as a singleton the way each saga's own {@link SagaInstances} is, so that it exists during refresh and
     * can be constructor-injected. The {@code @Saga} registrar fills it in afterwards, which is why it is empty until
     * the scan has run: a saga factory cannot be invoked before the beans it collaborates with are wired. See
     * {@link SagaInstancesRegistry} for what that means for a caller.
     * <p>
     * Gated on the same property as the annotation post-processor that populates it, because it has nothing to hold
     * when annotation processing is off. It is blocking-only, since {@code @Saga} is: the reactive starter has no saga
     * registrar.
     */
    @Bean
    @ConditionalOnMissingBean(SagaInstancesRegistry.class)
    @ConditionalOnProperty(name = "occurrent.subscription.enabled", havingValue = "true", matchIfMissing = true)
    public SagaInstancesRegistryImpl occurrentSagaInstancesRegistry() {
        // The declared return type is the implementation, not the SagaInstancesRegistry interface an application
        // injects, so that the registrar's by-type lookup of the writable type matches from the bean definition rather
        // than only once the singleton has been instantiated. Declaring the interface here happens to work today
        // because population runs from afterSingletonsInstantiated, but it would silently start resolving nothing if
        // this bean became @Lazy or population moved earlier, leaving an empty registry forever.
        return new SagaInstancesRegistryImpl();
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
    @ConditionalOnMissingBean(SpringMongoEventStore.class)
    @ConditionalOnProperty(name = "occurrent.event-store.enabled", havingValue = "true", matchIfMissing = true)
    public SpringMongoEventStore occurrentSpringMongoEventStore(MongoTemplate template, EventStoreConfig eventStoreConfig) {
        return new SpringMongoEventStore(template, eventStoreConfig);
    }

    @Bean
    @ConditionalOnMissingBean(CheckpointStorage.class)
    @ConditionalOnProperty(name = "occurrent.subscription.enabled", havingValue = "true", matchIfMissing = true)
    public CheckpointStorage occurrentCheckpointStorage(MongoTemplate mongoTemplate, OccurrentProperties occurrentProperties) {
        return new SpringMongoCheckpointStorage(mongoTemplate, occurrentProperties.getSubscription().getCollection());
    }

    @Bean
    @ConditionalOnMissingBean(SpringMongoLeaseCompetingConsumerStrategy.class)
    @ConditionalOnProperty(name = "occurrent.subscription.enabled", havingValue = "true", matchIfMissing = true)
    public SpringMongoLeaseCompetingConsumerStrategy occurrentCompetingConsumerStrategy(MongoTemplate mongoTemplate, List<CompetingConsumerListener> competingConsumerListeners) {
        SpringMongoLeaseCompetingConsumerStrategy strategy = SpringMongoLeaseCompetingConsumerStrategy.withDefaults(mongoTemplate);
        competingConsumerListeners.forEach(strategy::addListener);
        return strategy;
    }

    // @Primary so that a Subscribable injection point (for example the asynchronous subscription DSLs) resolves to
    // this asynchronous model rather than the register-only SynchronousSubscriptionModel, which is also a Subscribable.
    @Bean
    @Primary
    @ConditionalOnMissingBean(SubscriptionModel.class)
    @ConditionalOnProperty(name = "occurrent.subscription.enabled", havingValue = "true", matchIfMissing = true)
    public SubscriptionModel occurrentCompetingDurableSubscriptionModel(MongoTemplate mongoTemplate, SpringMongoLeaseCompetingConsumerStrategy competingConsumerStrategy, CheckpointStorage storage,
                                                                        OccurrentProperties occurrentProperties, EventStoreQueries eventStoreQueries, ObjectProvider<DcbEventStore> dcbEventStore, Environment environment) {
        EventStoreProperties eventStoreProperties = occurrentProperties.getEventStore();
        SpringMongoSubscriptionModelConfig mongoSubscriptionModelConfig = withConfig(eventStoreProperties.getCollection(), eventStoreProperties.getTimeRepresentation())
                .restartSubscriptionsOnChangeStreamHistoryLost(occurrentProperties.getSubscription().isRestartOnChangeStreamHistoryLost());
        if (environment.getProperty("spring.threads.virtual.enabled", Boolean.class, false)) {
            mongoSubscriptionModelConfig = mongoSubscriptionModelConfig.useVirtualThreads();
        }
        SpringMongoSubscriptionModel mongoSubscriptionModel = new SpringMongoSubscriptionModel(mongoTemplate, mongoSubscriptionModelConfig);
        // Checkpoints after every event by default, see DurableSubscriptionModel javadoc for the EveryN.every(n)
        // throughput tradeoff if checkpoint write volume becomes a bottleneck.
        DurableSubscriptionModel durableSubscriptionModel = new DurableSubscriptionModel(mongoSubscriptionModel, storage);
        CatchupSubscriptionModelConfig catchupConfig = new CatchupSubscriptionModelConfig(useCheckpointStorage(storage)
                .andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1000));
        // DCB catch-up replays by position over the DCB event store. The DcbCriteria.all() is shared by every
        // DcbSubscriptions subscription, which each narrow to their own DcbCriteria in the consumer, so a single
        // all-matching catch-up is correct. Stream catch-up replays by event time over the stream query API.
        boolean stream = eventStoreProperties.getCapabilities().contains(STREAM);
        DcbEventStore dcbStore = eventStoreProperties.getCapabilities().contains(DCB) ? dcbEventStore.getIfAvailable() : null;
        SubscriptionModel subscriptionModel;
        if (stream && dcbStore != null) {
            // STREAM and DCB together: one dual-mode model routes each subscription to stream or DCB catch-up.
            subscriptionModel = new CatchupSubscriptionModel(durableSubscriptionModel, eventStoreQueries, dcbStore, DcbCriteria.all(), catchupConfig);
        } else if (stream) {
            subscriptionModel = new CatchupSubscriptionModel(durableSubscriptionModel, eventStoreQueries, catchupConfig);
        } else if (dcbStore != null) {
            subscriptionModel = new CatchupSubscriptionModel(durableSubscriptionModel, dcbStore, DcbCriteria.all(), catchupConfig);
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

    /**
     * The capability-agnostic subscription DSL, used by the {@code @Subscription} annotation. On a store with both the
     * {@code STREAM} and {@code DCB} capabilities it delivers both stream-written and DCB-appended events, filtered only
     * by event type.
     */
    @Bean
    @Primary
    @ConditionalOnMissingBean(Subscriptions.class)
    @ConditionalOnProperty(name = "occurrent.subscription.enabled", havingValue = "true", matchIfMissing = true)
    public Subscriptions<E> occurrentSubscriptionDsl(Subscribable subscribable, CloudEventConverter<E> cloudEventConverter) {
        return new Subscriptions<>(subscribable, cloudEventConverter);
    }

    /**
     * The stream subscription DSL, used by the {@code @StreamSubscription} annotation. It scopes delivery to the
     * {@code STREAM} capability.
     */
    @Bean
    @ConditionalOnMissingBean(StreamSubscriptions.class)
    @ConditionalOnProperty(name = "occurrent.subscription.enabled", havingValue = "true", matchIfMissing = true)
    public StreamSubscriptions<E> occurrentStreamSubscriptionDsl(Subscribable subscribable, CloudEventConverter<E> cloudEventConverter) {
        return new StreamSubscriptions<>(subscribable, cloudEventConverter);
    }

    /**
     * The register-only subscription model whose handlers the application service invokes synchronously, in-process,
     * after a write (see {@link SynchronousSubscriptionModel}). It is both the registrar the synchronous subscription
     * DSL registers on and the dispatcher the application service dispatches to, so both must resolve to this same
     * bean.
     */
    @Bean
    @ConditionalOnMissingBean(SynchronousSubscriptionModel.class)
    @ConditionalOnProperty(name = "occurrent.subscription.enabled", havingValue = "true", matchIfMissing = true)
    public SynchronousSubscriptionModel occurrentSynchronousSubscriptionModel() {
        return new SynchronousSubscriptionModel();
    }

    /**
     * A {@link TransactionExecutor} that spans the event-store write and the synchronous subscription handlers in one
     * Spring transaction, so a throwing handler rolls the write back. Wired into the application service beans below.
     * <p>
     * There is no free lunch: this only pays off while synchronous subscriptions are registered. It is auto-configured
     * whenever the event store is enabled (so it is available should a synchronous subscription be registered), and
     * can be replaced by defining your own {@link TransactionExecutor} bean, for example
     * {@link TransactionExecutor#noTransaction()} to make synchronous subscriptions best-effort instead of atomic.
     */
    @Bean
    @ConditionalOnMissingBean(TransactionExecutor.class)
    @ConditionalOnProperty(name = "occurrent.event-store.enabled", havingValue = "true", matchIfMissing = true)
    public SpringTransactionExecutor occurrentSpringTransactionExecutor(MongoTransactionManager transactionManager) {
        return new SpringTransactionExecutor(transactionManager);
    }

    /**
     * The synchronous counterpart of {@link #occurrentSubscriptionDsl(Subscribable, CloudEventConverter)}, used by the
     * {@code @SynchronousSubscription} annotation. It is the same {@link Subscriptions} type as the asynchronous DSL,
     * so it is given a distinct bean name (and the asynchronous one is {@link Primary}); the annotation processor
     * resolves this one by name.
     */
    @Bean(OccurrentBlockingAnnotationBeanPostProcessor.SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME)
    @ConditionalOnMissingBean(name = OccurrentBlockingAnnotationBeanPostProcessor.SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME)
    @ConditionalOnProperty(name = "occurrent.subscription.enabled", havingValue = "true", matchIfMissing = true)
    public Subscriptions<E> occurrentSynchronousSubscriptionDsl(SynchronousSubscriptionModel synchronousSubscriptionModel, CloudEventConverter<E> cloudEventConverter) {
        return new Subscriptions<>(synchronousSubscriptionModel, cloudEventConverter);
    }

    /**
     * DCB subscription DSL, auto-configured when the DCB event-store capability is enabled. In DCB-only mode the
     * underlying subscription model wraps a {@link CatchupSubscriptionModel} in DCB mode, so a subscription started at a
     * {@code GlobalCheckpoint} replays history by position before switching to live delivery. Started without
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
    public ApplicationService<E> occurrentApplicationService(EventStore eventStore, CloudEventConverter<E> cloudEventConverter, OccurrentProperties occurrentProperties,
                                                             ObjectProvider<SynchronousEventDispatcher> synchronousEventDispatcher, ObjectProvider<TransactionExecutor> transactionExecutor) {
        boolean enableDefaultRetryStrategy = occurrentProperties.getApplicationService().isEnableDefaultRetryStrategy();
        RetryStrategy retryStrategy = enableDefaultRetryStrategy ? GenericApplicationService.defaultRetryStrategy() : RetryStrategy.none();
        GenericApplicationService.Builder<E> builder = GenericApplicationService.builder(eventStore, cloudEventConverter).retryStrategy(retryStrategy);
        // Wire the synchronous subscription dispatcher and transaction executor when present, so a
        // @SynchronousSubscription handler runs on the writer's thread, atomically with the write. Both are resolved
        // through ObjectProvider because they only exist when the feature is applicable, and either can be absent or
        // user-replaced.
        synchronousEventDispatcher.ifAvailable(builder::synchronousSubscriptions);
        transactionExecutor.ifAvailable(builder::transactionExecutor);
        return builder.build();
    }

    /**
     * Auto-configures the {@link DcbApplicationService} when the DCB capability is enabled. The {@link TagGenerator} is
     * resolved through {@link ObjectProvider} rather than {@code @ConditionalOnBean(TagGenerator.class)} because
     * {@code @EnableOccurrent} imports this configuration with a plain {@code @Import}, so a {@code @ConditionalOnBean}
     * could be evaluated before a user's own {@link TagGenerator} bean is registered. {@code getIfAvailable()} resolves
     * at instantiation time instead, after all bean definitions exist.
     * <p>
     * A global {@link TagGenerator} is optional. A {@code DcbDecider} carries the tags for the events it emits, so a
     * decider-based application needs none. Decider-less DCB (a raw {@code execute}, or {@code @DcbTag}) relies on this
     * global tagger (an explicit bean or the {@link AnnotationTagGenerator} fallback) or on per-execute tags supplied
     * through {@code DcbExecuteOptions}. When events are produced and no tagger of any kind is available, the append
     * fails loudly.
     */
    @Bean
    @ConditionalOnMissingBean(DcbApplicationService.class)
    @Conditional(OnDcbEventStoreCapabilityCondition.class)
    @ConditionalOnProperty(name = {"occurrent.event-store.enabled", "occurrent.application-service.enabled"}, havingValue = "true", matchIfMissing = true)
    public DcbApplicationService<E> occurrentDcbApplicationService(DcbEventStore eventStore, CloudEventConverter<E> cloudEventConverter,
                                                                     ObjectProvider<TagGenerator<E>> tagGeneratorProvider, OccurrentProperties occurrentProperties,
                                                                     ObjectProvider<SynchronousEventDispatcher> synchronousEventDispatcher, ObjectProvider<TransactionExecutor> transactionExecutor) {
        boolean enableDefaultRetryStrategy = occurrentProperties.getApplicationService().isEnableDefaultRetryStrategy();
        RetryStrategy retryStrategy = enableDefaultRetryStrategy ? GenericDcbApplicationService.defaultRetryStrategy() : RetryStrategy.none();
        GenericDcbApplicationService.Builder<E> builder = GenericDcbApplicationService.builder(eventStore, cloudEventConverter).retryStrategy(retryStrategy);
        tagGeneratorProvider.ifAvailable(builder::tagGenerator);
        // Wire the synchronous subscription dispatcher and transaction executor when present (see occurrentApplicationService).
        synchronousEventDispatcher.ifAvailable(builder::synchronousSubscriptions);
        transactionExecutor.ifAvailable(builder::transactionExecutor);
        return builder.build();
    }

    /**
     * Supplies a default {@link TagGenerator} backed by {@link AnnotationTagGenerator} when the
     * {@code dcb-annotation-taggenerator} module is on the classpath and the user has not defined their own
     * {@link TagGenerator} bean. The module is an {@code optional} dependency of this starter, so it is never
     * dragged onto a consumer's classpath transitively; the {@link ConditionalOnClass} guard means this nested
     * configuration class itself is never loaded (avoiding {@link NoClassDefFoundError}) when the module is absent.
     * <p>
     * {@code @Fallback} rather than {@code @ConditionalOnMissingBean(TagGenerator.class)}: {@link OccurrentMongoAutoConfiguration}
     * is activated via {@code @EnableOccurrent}'s {@code @Import}, not {@code spring.factories}/{@code AutoConfiguration.imports},
     * so it is not guaranteed to be processed after the importing user configuration's own {@code @Bean} methods -
     * {@code @ConditionalOnMissingBean} can therefore run before a user-defined {@code TagGenerator} bean is
     * registered and let both beans through. A {@code @Fallback} bean is instead excluded at dependency-resolution
     * time (see {@code occurrentTypeMapper()} above for the same pattern), which is unaffected by registration order.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(AnnotationTagGenerator.class)
    static class AnnotationTagGeneratorConfiguration {

        /**
         * Declared as a raw {@link TagGenerator} (not {@code AnnotationTagGenerator<Object>|<E>}) so that Spring's
         * generic bean matching resolves it for {@code ObjectProvider<TagGenerator<E>>} at any type argument
         * {@code E}: a raw-typed bean definition matches any parameterization of the target generic when resolved
         * through {@link ObjectProvider}.
         */
        @Bean
        @Lazy
        @Fallback
        @SuppressWarnings({"rawtypes"})
        TagGenerator occurrentAnnotationTagGenerator() {
            return new AnnotationTagGenerator<>();
        }
    }

    /**
     * Supplies a default {@link StreamIdResolver} that derives a command's target stream id from a {@code @TargetStreamId}
     * annotated member, so a command producer can route by annotation without a hand-written {@code command -> streamId}
     * function. A user {@link StreamIdResolver} bean overrides it. Same {@code @Fallback} and raw-type reasoning as
     * {@link AnnotationTagGeneratorConfiguration}, and {@code @ConditionalOnClass} so the class loads only when the
     * command-dispatch-annotation module is present.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(AnnotationStreamIdResolver.class)
    static class StreamIdResolverConfiguration {

        @Bean
        @Lazy
        @Fallback
        @SuppressWarnings({"rawtypes"})
        StreamIdResolver occurrentAnnotationStreamIdResolver() {
            return new AnnotationStreamIdResolver<>();
        }
    }
}
