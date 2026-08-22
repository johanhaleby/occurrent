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
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.application.service.blocking.SynchronousEventDispatcher;
import org.occurrent.application.service.blocking.TransactionExecutor;
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
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.dsl.query.blocking.DomainEventQueries;
import org.occurrent.dsl.subscription.blocking.StreamSubscriptions;
import org.occurrent.dsl.subscription.blocking.Subscriptions;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.filtermatching.jackson.JacksonDataFieldReader;
import org.occurrent.retry.Backoff;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.springboot.blocking.*;
import org.occurrent.springboot.common.*;
import org.occurrent.springboot.common.OccurrentProperties.EventStoreProperties;
import org.occurrent.subscription.api.blocking.*;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy.CompetingConsumerListener;
import org.occurrent.subscription.blocking.competingconsumers.CompetingConsumerSubscriptionModel;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModelConfig;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelConfig;
import org.occurrent.subscription.util.predicate.EveryN;
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
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.*;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.time.Duration;
import java.util.List;

import static java.util.Objects.requireNonNull;
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
@Import({Jackson3CloudEventConverterConfiguration.class, OccurrentBlockingAnnotationConfiguration.class})
public class OccurrentMongoAutoConfiguration<E> {

    /**
     * The MongoDB half of the workaround for
     * <a href="https://github.com/spring-projects/spring-framework/issues/32904">spring-framework#32904</a>: force
     * {@link MongoOperations} into existence before a subscription is started. The result is deliberately discarded.
     */
    @Bean
    StartupWorkaround occurrentMongoOperationsStartupWorkaround(ApplicationContext applicationContext) {
        return () -> applicationContext.getBean(MongoOperations.class);
    }

    /**
     * The zero-config MongoDB read-model store a {@code @Projection} falls back to when it declares none.
     * <p>
     * {@code @Fallback} rather than {@code @ConditionalOnMissingBean}: this configuration is activated by
     * {@code @EnableOccurrent}'s plain {@code @Import}, so the condition can be evaluated before an application's own
     * provider bean is registered, letting both through. A {@code @Fallback} bean is excluded at dependency-resolution
     * time instead, which registration order cannot affect. Same reasoning as {@code occurrentTypeMapper()} below.
     */
    @Bean
    @Fallback
    DefaultProjectionStoreProvider occurrentMongoDefaultProjectionStoreProvider(ApplicationContext applicationContext) {
        return new MongoProjectionStoreProvider(applicationContext);
    }

    /** The zero-config MongoDB snapshot store a {@code @Snapshot} falls back to when it declares none. {@code @Fallback} for the reason above. */
    @Bean
    @Fallback
    DefaultSnapshotStoreProvider occurrentMongoDefaultSnapshotStoreProvider(ApplicationContext applicationContext) {
        return new MongoSnapshotStoreProvider(applicationContext);
    }

    /** The zero-config MongoDB saga state store a {@code @Saga} falls back to when it declares none. {@code @Fallback} for the reason above. */
    @Bean
    @Fallback
    DefaultSagaStateStoreProvider occurrentMongoDefaultSagaStateStoreProvider(ApplicationContext applicationContext) {
        return new MongoSagaStateStoreProvider(applicationContext);
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
                .eventStoreCollectionName(eventStoreProperties.resolveCollection())
                .transactionConfig(transactionManager)
                .timeRepresentation(eventStoreProperties.resolveTimeRepresentation())
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
    @Conditional(OnSubscriptionsNotDisabledCondition.class)
    public CheckpointStorage occurrentCheckpointStorage(MongoTemplate mongoTemplate, OccurrentProperties occurrentProperties) {
        return new SpringMongoCheckpointStorage(mongoTemplate, occurrentProperties.getSubscription().resolveCollection());
    }

    /**
     * The zero-config {@link AppliedAppendStore} an application gets when it declares none itself. A
     * {@code @Projection(recordAppliedAppends = true)} projection resolves this same bean.
     * <p>
     * {@code @Fallback} alongside the condition, for the same reason {@code occurrentTypeMapper()} is one. This
     * configuration is activated by {@code @EnableOccurrent}'s plain {@code @Import}, so
     * {@code @ConditionalOnMissingBean} can be evaluated before an application's own {@link AppliedAppendStore} bean
     * is registered, letting both through. A {@code @Fallback} bean is excluded at dependency-resolution time
     * instead, which registration order cannot affect.
     */
    @Bean
    @Fallback
    @ConditionalOnMissingBean(AppliedAppendStore.class)
    public AppliedAppendStore occurrentAppliedAppendStore(MongoTemplate mongoTemplate, OccurrentProperties occurrentProperties) {
        OccurrentProperties.ProjectionProperties.AppliedAppendProperties appliedAppend = occurrentProperties.getProjection().getAppliedAppend();
        OccurrentProperties.ProjectionProperties.AppliedAppendProperties.WaitBackoffProperties waitBackoff = appliedAppend.getWaitBackoff();
        Backoff pollBackoff = Backoff.exponential(waitBackoff.getInitial(), waitBackoff.getMax(), waitBackoff.getMultiplier());
        return new MongoAppliedAppendStore(mongoTemplate, appliedAppend.getCollection(), appliedAppend.getRetention(),
                RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f), pollBackoff);
    }

    /**
     * The lease-based competing-consumer strategy, which decides both which node delivers a subscription's events and
     * which lease version fences its checkpoint writes (ADR 116).
     * <p>
     * {@code @Fallback} so an application's own {@link CompetingConsumerStrategy} bean of any type replaces this one
     * rather than competing with it, at every injection point and in the fence's own lookup. The
     * {@code @ConditionalOnMissingBean} keeps this bean from being built at all when the application declares a lease
     * strategy of the same type, which is the case a condition can decide. It cannot decide the interface-typed case,
     * because this configuration is also activated by {@code @EnableOccurrent}'s plain {@code @Import} and the
     * condition is then evaluated before the application's own bean is registered, the same reason
     * {@link #occurrentTypeMapper()} is a {@code @Fallback}.
     */
    @Bean
    @Fallback
    @ConditionalOnMissingBean(SpringMongoLeaseCompetingConsumerStrategy.class)
    @Conditional(OnSubscriptionsNotDisabledCondition.class)
    public SpringMongoLeaseCompetingConsumerStrategy occurrentCompetingConsumerStrategy(MongoTemplate mongoTemplate, List<CompetingConsumerListener> competingConsumerListeners) {
        SpringMongoLeaseCompetingConsumerStrategy strategy = SpringMongoLeaseCompetingConsumerStrategy.withDefaults(mongoTemplate);
        competingConsumerListeners.forEach(strategy::addListener);
        return strategy;
    }

    /**
     * The {@link ComposedDefaultStartPosition} bean {@link #occurrentCompetingDurableSubscriptionModel} fills. A
     * plain {@code @Bean} rather than {@code @ConditionalOnMissingBean}: an application has no reason to supply its
     * own, since nothing public composes this stack's default subscription model outside this configuration, and
     * the one caller that fills it is right below.
     */
    @Bean
    public ComposedDefaultStartPosition occurrentComposedDefaultStartPosition() {
        return new ComposedDefaultStartPosition();
    }

    // @Primary so that a Subscribable injection point (for example the asynchronous subscription DSLs) resolves to
    // this asynchronous model rather than the register-only SynchronousSubscriptionModel, which is also a Subscribable.
    // Named rather than inferred because under SubscriptionMode.MANUAL the bean is a wrapper, and the @PreDestroy on
    // CompetingConsumerSubscriptionModel only counts while that class is the bean class. Closing the context has to
    // reach it, or the Mongo listener container and the lease refresh thread outlive the application.
    //
    // The register-only models are ignored when deciding whether the application brought its own. They are
    // SubscriptionModels, but they have no start position, no checkpoint and no catch-up, so one of them standing in
    // for this bean would silently take away every asynchronous subscription. A user declaring their own
    // SynchronousSubscriptionModel, which the @ConditionalOnMissingBean further down invites, must still get this one.
    @Bean(destroyMethod = "shutdown")
    @Primary
    @ConditionalOnMissingBean(value = SubscriptionModel.class, ignored = RegisteringSubscribable.class)
    @Conditional(OnSubscriptionsNotDisabledCondition.class)
    public SubscriptionModel occurrentCompetingDurableSubscriptionModel(MongoTemplate mongoTemplate, CheckpointStorage storage,
                                                                        OccurrentProperties occurrentProperties, EventStoreQueries eventStoreQueries, ObjectProvider<DcbEventStore> dcbEventStore,
                                                                        ObjectProvider<CompetingConsumerStrategy> competingConsumerStrategyProvider, Environment environment,
                                                                        ComposedDefaultStartPosition composedDefaultStartPosition) {
        // Resolved through the provider rather than taken as a parameter of its own, so several strategy beans with no
        // @Primary fail with the message that names the remedy instead of Spring's report of an unsatisfied parameter.
        // The strategy is forced here either way, since this model holds one for the life of the bean.
        CompetingConsumerStrategy competingConsumerStrategy = requireNonNull(CompetingConsumerStrategies.resolveUnique(competingConsumerStrategyProvider),
                "A competing-consumer strategy is required to build " + CompetingConsumerSubscriptionModel.class.getSimpleName());
        EventStoreProperties eventStoreProperties = occurrentProperties.getEventStore();
        SpringMongoSubscriptionModelConfig mongoSubscriptionModelConfig = withConfig(eventStoreProperties.resolveCollection(), eventStoreProperties.resolveTimeRepresentation())
                .restartSubscriptionsOnChangeStreamHistoryLost(occurrentProperties.getSubscription().resolveRestartOnChangeStreamHistoryLost());
        if (environment.getProperty("spring.threads.virtual.enabled", Boolean.class, false)) {
            mongoSubscriptionModelConfig = mongoSubscriptionModelConfig.useVirtualThreads();
        }
        SpringMongoSubscriptionModel mongoSubscriptionModel = new SpringMongoSubscriptionModel(mongoTemplate, mongoSubscriptionModelConfig);
        // Resolved lazily, once per bean, and reused at every write site below (ADR 116). ObjectProvider rather than
        // the strategy bean itself, so a checkpoint-writing model does not force the strategy, and through it every
        // CompetingConsumerListener bean, into existence before this bean is fully constructed.
        CheckpointWriteVersionSource writeVersionSource = new CompetingConsumerCheckpointWriteVersionSource(competingConsumerStrategyProvider,
                occurrentProperties.getSubscription().getCompetingConsumer()::isFenceCheckpoints);
        // Checkpoints after every event by default, see DurableSubscriptionModel javadoc for the EveryN.every(n)
        // throughput tradeoff if checkpoint write volume becomes a bottleneck.
        DurableSubscriptionModelConfig durableConfig = new DurableSubscriptionModelConfig(EveryN.everyEvent())
                .startWhenNoStartPositionCanBeRecorded(occurrentProperties.getSubscription().isStartWhenNoStartPositionCanBeRecorded());
        DurableSubscriptionModel durableSubscriptionModel = new DurableSubscriptionModel(mongoSubscriptionModel, storage, durableConfig, writeVersionSource);
        CatchupSubscriptionModelConfig catchupConfig = new CatchupSubscriptionModelConfig(useCheckpointStorage(storage, writeVersionSource)
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
        CompetingConsumerSubscriptionModel competingConsumerSubscriptionModel = new CompetingConsumerSubscriptionModel(subscriptionModel, competingConsumerStrategy);
        // Registering must not reach the stack at all, rather than reach it and be stopped afterwards. A catch-up
        // replay reads the event store directly, so a subscription resuming from a stored checkpoint would deliver
        // history to a handler nobody started. The Mongo model supplies the position to pin, since it is the one
        // reading the feed.
        SubscriptionModel composedSubscriptionModel = occurrentProperties.getSubscription().resolveMode() != SubscriptionMode.MANUAL
                ? competingConsumerSubscriptionModel
                : ManualStartSubscriptionModel.stoppedByDefault(competingConsumerSubscriptionModel, mongoSubscriptionModel, storage);
        // Told the exact bean this method is about to return, not an inner layer, since that is what
        // occurrentAsynchronousSubscribable and every DSL wrapping it actually resolve, and what a projection's own
        // capability is compared against (issue 871). A default StartAt resolves to StartAt.subscriptionModelDefault()
        // (see StartPositionSupport), which every shape this method composes (durable alone, or wrapped in
        // stream/DCB/dual catch-up) classifies as live, the same as a checkpoint that is neither global nor
        // time-based, so a wiped checkpoint changes nothing for it either (ADR 132 decision 7, issue 865).
        composedDefaultStartPosition.suppliedBy(composedSubscriptionModel);
        composedDefaultStartPosition.defaultBypassesCatchup();
        return composedSubscriptionModel;
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
     * <p>
     * Resolved through {@link #occurrentAsynchronousSubscribable(ApplicationContext)} rather than injected by type: a
     * {@code Subscribable} parameter would also match the register-only {@link SynchronousSubscriptionModel}, which is
     * ambiguous the moment an application supplies its own asynchronous model without marking it {@code @Primary} (see
     * {@link AsynchronousSubscribables}).
     */
    @Bean
    @Primary
    @ConditionalOnMissingBean(Subscriptions.class)
    @Conditional(OnSubscriptionsNotDisabledCondition.class)
    public Subscriptions<E> occurrentSubscriptionDsl(ApplicationContext applicationContext, CloudEventConverter<E> cloudEventConverter) {
        return new Subscriptions<>(occurrentAsynchronousSubscribable(applicationContext), cloudEventConverter);
    }

    /**
     * The stream subscription DSL, used by the {@code @StreamSubscription} annotation. It scopes delivery to the
     * {@code STREAM} capability. See {@link #occurrentSubscriptionDsl(ApplicationContext, CloudEventConverter)} for why
     * the asynchronous model is resolved rather than injected by type.
     */
    @Bean
    @ConditionalOnMissingBean(StreamSubscriptions.class)
    @Conditional(OnSubscriptionsNotDisabledCondition.class)
    public StreamSubscriptions<E> occurrentStreamSubscriptionDsl(ApplicationContext applicationContext, CloudEventConverter<E> cloudEventConverter) {
        return new StreamSubscriptions<>(occurrentAsynchronousSubscribable(applicationContext), cloudEventConverter);
    }

    private static Subscribable occurrentAsynchronousSubscribable(ApplicationContext applicationContext) {
        return AsynchronousSubscribables.resolve(applicationContext, Subscribable.class, RegisteringSubscribable.class);
    }

    /**
     * A reader for a field inside an event's {@code data} payload, so a subscription can filter on one. Contributed only
     * when {@code occurrent-common-inmemory-filter-matching-jackson} is on the classpath, which is how an application
     * asks for this: the reader needs a JSON library, and adding it to every application that never filters on a
     * payload is a cost with no return (ADR 87). Define your own {@link DataFieldReader} bean to replace it.
     */
    @Bean
    @ConditionalOnMissingBean(DataFieldReader.class)
    @ConditionalOnClass(name = "org.occurrent.filtermatching.jackson.JacksonDataFieldReader")
    public DataFieldReader occurrentDataFieldReader() {
        return new JacksonDataFieldReader();
    }

    /**
     * The register-only subscription model whose handlers the application service invokes synchronously, in-process,
     * after a write (see {@link SynchronousSubscriptionModel}). It is both the registrar the synchronous subscription
     * DSL registers on and the dispatcher the application service dispatches to, so both must resolve to this same
     * bean.
     */
    @Bean
    @ConditionalOnMissingBean(SynchronousSubscriptionModel.class)
    @Conditional(OnSubscriptionsNotDisabledCondition.class)
    public SynchronousSubscriptionModel occurrentSynchronousSubscriptionModel(OccurrentProperties occurrentProperties, ObjectProvider<DataFieldReader> dataFieldReaderProvider) {
        SynchronousSubscriptionModel synchronousSubscriptionModel = new SynchronousSubscriptionModel(dataFieldReaderProvider.getIfAvailable(DataFieldReader::refusing));
        // Stopped up front rather than after registration, so a synchronous handler registered under MANUAL is paused
        // from the outset and a write does not run it.
        if (occurrentProperties.getSubscription().resolveMode() != SubscriptionMode.AUTO) {
            synchronousSubscriptionModel.stop();
        }
        return synchronousSubscriptionModel;
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
    @Bean(OccurrentBlockingBeanNames.SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME)
    @ConditionalOnMissingBean(name = OccurrentBlockingBeanNames.SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME)
    @Conditional(OnSubscriptionsNotDisabledCondition.class)
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
    @Conditional({OnDcbEventStoreCapabilityCondition.class, OnSubscriptionsNotDisabledCondition.class})
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
