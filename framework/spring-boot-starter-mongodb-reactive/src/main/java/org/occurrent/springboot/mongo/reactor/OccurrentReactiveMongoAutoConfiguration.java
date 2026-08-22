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

import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.dcb.TagGenerator;
import org.occurrent.application.service.dcb.annotation.AnnotationTagGenerator;
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.application.service.reactor.ReactiveSynchronousEventDispatcher;
import org.occurrent.application.service.reactor.ReactiveTransactionExecutor;
import org.occurrent.application.service.reactor.dcb.DcbApplicationService;
import org.occurrent.application.service.reactor.dcb.GenericDcbApplicationService;
import org.occurrent.application.service.reactor.generic.GenericApplicationService;
import org.occurrent.application.service.spring.reactor.SpringReactiveTransactionExecutor;
import org.occurrent.dsl.dcb.reactor.DcbDomainEventQueries;
import org.occurrent.dsl.dcb.reactor.DcbSubscriptions;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.dsl.query.reactor.DomainEventQueries;
import org.occurrent.dsl.subscription.reactor.StreamSubscriptions;
import org.occurrent.dsl.subscription.reactor.Subscriptions;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.EventStoreQueries;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.filtermatching.jackson.JacksonDataFieldReader;
import org.occurrent.retry.Backoff;
import org.occurrent.springboot.common.*;
import org.occurrent.springboot.common.OccurrentProperties.EventStoreProperties;
import org.occurrent.springboot.reactor.ComposedCatchupModel;
import org.occurrent.springboot.reactor.DefaultReactiveSnapshotStoreProvider;
import org.occurrent.springboot.reactor.OccurrentReactiveAnnotationConfiguration;
import org.occurrent.springboot.reactor.OccurrentReactorBeanNames;
import org.occurrent.springboot.reactor.PositionOrderedEventStores;
import org.occurrent.subscription.api.reactor.*;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModelConfig;
import org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModel;
import org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModelConfig;
import org.occurrent.subscription.reactor.durable.catchup.ReactorCatchupSubscriptionModel;
import org.occurrent.subscription.util.predicate.EveryN;
import org.occurrent.subscription.synchronous.reactor.SynchronousSubscriptionModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.mongodb.autoconfigure.MongoReactiveAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.*;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import reactor.util.retry.Retry;

import java.time.Duration;


import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * Occurrent Spring autoconfiguration support for the reactive (Project Reactor) MongoDB event store and subscriptions.
 * It mirrors the blocking {@code OccurrentMongoAutoConfiguration} for the reactive stack. Enable it with
 * {@link EnableOccurrentReactive} and the {@code spring-boot-starter-mongodb-reactive} dependency.
 */
@AutoConfiguration(after = MongoReactiveAutoConfiguration.class)
@ConditionalOnClass({ReactorMongoEventStore.class, ReactorMongoSubscriptionModel.class})
@EnableConfigurationProperties(OccurrentProperties.class)
@Import({Jackson3CloudEventConverterConfiguration.class, OccurrentReactiveAnnotationConfiguration.class})
public class OccurrentReactiveMongoAutoConfiguration<E> {

    private static final Logger log = LoggerFactory.getLogger(OccurrentReactiveMongoAutoConfiguration.class);

    /**
     * The MongoDB half of the workaround for
     * <a href="https://github.com/spring-projects/spring-framework/issues/32904">spring-framework#32904</a>: force
     * {@link ReactiveMongoOperations} into existence before a subscription is started. The result is deliberately discarded.
     */
    @Bean
    StartupWorkaround occurrentReactiveMongoOperationsStartupWorkaround(ApplicationContext applicationContext) {
        return () -> applicationContext.getBean(ReactiveMongoOperations.class);
    }

    /**
     * The zero-config MongoDB snapshot store a {@code @Snapshot} falls back to when it declares none.
     * <p>
     * {@code @Fallback} rather than {@code @ConditionalOnMissingBean}: this configuration is activated by
     * {@code @EnableOccurrentReactive}'s plain {@code @Import}, so the condition can be evaluated before an application's own
     * provider bean is registered, letting both through. A {@code @Fallback} bean is excluded at dependency-resolution
     * time instead, which registration order cannot affect. Same reasoning as {@code occurrentTypeMapper()} below.
     */
    @Bean
    @Fallback
    DefaultReactiveSnapshotStoreProvider occurrentMongoDefaultReactiveSnapshotStoreProvider(ApplicationContext applicationContext) {
        return new MongoReactiveSnapshotStoreProvider(applicationContext);
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
    @ConditionalOnMissingBean(EventStore.class)
    @ConditionalOnProperty(name = "occurrent.event-store.enabled", havingValue = "true", matchIfMissing = true)
    public ReactorMongoEventStore occurrentReactorMongoEventStore(ReactiveMongoTemplate template, EventStoreConfig eventStoreConfig) {
        return new ReactorMongoEventStore(template, eventStoreConfig);
    }

    @Bean
    @ConditionalOnMissingBean(CheckpointStorage.class)
    @Conditional(OnSubscriptionsNotDisabledCondition.class)
    public CheckpointStorage occurrentCheckpointStorage(ReactiveMongoOperations mongo, OccurrentProperties occurrentProperties) {
        return new ReactorCheckpointStorage(mongo, occurrentProperties.getSubscription().resolveCollection());
    }

    /**
     * The zero-config {@link AppliedAppendStore} an application gets when it declares none itself. A
     * {@code @Projection(recordAppliedAppends = true)} projection resolves this same bean.
     * <p>
     * {@code @Fallback} alongside the condition, for the same reason {@code occurrentTypeMapper()} is one. This
     * configuration is activated by {@code @EnableOccurrentReactive}'s plain {@code @Import}, so
     * {@code @ConditionalOnMissingBean} can be evaluated before an application's own {@link AppliedAppendStore} bean
     * is registered, letting both through. A {@code @Fallback} bean is excluded at dependency-resolution time
     * instead, which registration order cannot affect.
     */
    @Bean
    @Fallback
    @ConditionalOnMissingBean(AppliedAppendStore.class)
    public AppliedAppendStore occurrentAppliedAppendStore(ReactiveMongoOperations mongo, OccurrentProperties occurrentProperties) {
        OccurrentProperties.ProjectionProperties.AppliedAppendProperties appliedAppend = occurrentProperties.getProjection().getAppliedAppend();
        OccurrentProperties.ProjectionProperties.AppliedAppendProperties.WaitBackoffProperties waitBackoff = appliedAppend.getWaitBackoff();
        Backoff pollBackoff = Backoff.exponential(waitBackoff.getInitial(), waitBackoff.getMax(), waitBackoff.getMultiplier());
        // Retry.backoff counts retries rather than total calls, so one less than the configured attempts is what
        // makes this match the blocking starter. The filter keeps an index this store can never create from being
        // attempted again on a schedule, the same as ReactiveMongoAppliedAppendStore.defaultRetry().
        Retry storeRetry = Retry.backoff(appliedAppend.getMaxAttempts() - 1L, Duration.ofMillis(100))
                .maxBackoff(Duration.ofSeconds(2))
                .filter(e -> !(e instanceof ReactiveMongoAppliedAppendStore.ConflictingIndexException))
                .onRetryExhaustedThrow((spec, signal) -> signal.failure());
        return new ReactiveMongoAppliedAppendStore(mongo, appliedAppend.getCollection(), appliedAppend.getRetention(), storeRetry, pollBackoff);
    }

    /**
     * The composed reactive subscription model. Unlike the blocking side (which also layers a competing consumer),
     * the reactive stack has no competing-consumer model, so the chain is {@code Durable(Catchup(mongo))} with the
     * durable model on the outside as the {@link Subscribable}/lifecycle authority. The catch-up layer is a
     * {@link ReactorCatchupSubscriptionModel}, which routes each subscription to stream or DCB replay by its filter and
     * start position. A combined STREAM+DCB store gets the dual-mode model, so a {@code @StreamSubscription} started
     * from the beginning replays stream history by position while DCB subscriptions replay by DCB position. A STREAM-only
     * store that writes position (on by default, opt out with {@code occurrent.event-store.stream.position=false}) gets
     * stream-only catch-up, a DCB-only store gets DCB-only catch-up, and a STREAM-only store with position off gets no
     * catch-up layer. {@code destroyMethod = "shutdown"} disposes the running subscriptions on context close.
     */
    // @Primary so that a Subscribable injection point (for example the asynchronous subscription DSLs) resolves to
    // this asynchronous model rather than the register-only SynchronousSubscriptionModel, which is also a Subscribable.
    //
    // The register-only models are ignored when deciding whether the application brought its own. They have no start
    // position, no checkpoint and no catch-up, so one of them standing in for this bean would silently take away every
    // asynchronous subscription. That reaches a declared SynchronousSubscriptionModel, which the
    // @ConditionalOnMissingBean further down invites, and a PushSubscriptionModel, which a push projection needs one
    // of per consumer.
    @Bean(destroyMethod = "shutdown")
    @Primary
    @ConditionalOnMissingBean(value = {FluxSubscriptionModel.class, Subscribable.class}, ignored = RegisteringSubscribable.class)
    @Conditional(OnSubscriptionsNotDisabledCondition.class)
    public ReactorDurableSubscriptionModel occurrentDurableSubscriptionModel(ReactiveMongoOperations mongo, CheckpointStorage storage,
                                                                             OccurrentProperties occurrentProperties, ObjectProvider<DcbEventStore> dcbEventStore,
                                                                             ApplicationContext applicationContext, ComposedCatchupModel composedCatchupModel) {
        EventStoreProperties eventStoreProperties = occurrentProperties.getEventStore();
        ReactorMongoSubscriptionModel mongoSubscriptionModel = new ReactorMongoSubscriptionModel(mongo, eventStoreProperties.resolveCollection(), eventStoreProperties.resolveTimeRepresentation(),
                ReactorMongoSubscriptionModelConfig.withConfig().restartSubscriptionsOnChangeStreamHistoryLost(occurrentProperties.getSubscription().resolveRestartOnChangeStreamHistoryLost()));
        ReactorDurableSubscriptionModelConfig durableConfig = new ReactorDurableSubscriptionModelConfig(EveryN.everyEvent())
                .startWhenNoStartPositionCanBeRecorded(occurrentProperties.getSubscription().isStartWhenNoStartPositionCanBeRecorded());
        CheckpointAwareSubscriptionModel catchupLayer = composeCatchupLayer(mongoSubscriptionModel, eventStoreProperties, dcbEventStore, applicationContext);
        // Handed to the holder before it disappears inside the durable wrapper below: ReplayAwareSubscriptions is
        // findable on catchupLayer itself, a ReactorCatchupSubscriptionModel when one composed, but not on the
        // ReactorDurableSubscriptionModel wrapping it, since this stack's capability lookup does not unwrap
        // (ADR 132 decision 8, #842).
        composedCatchupModel.suppliedBy(catchupLayer);
        // A default StartAt resolves to StartAt.subscriptionModelDefault() (see StartPositionSupport), which both
        // the stream and DCB catch-up layers composeCatchupLayer can build classify as live, the same as a
        // checkpoint that is neither global nor time-based, so a wiped checkpoint changes nothing for it either.
        composedCatchupModel.defaultBypassesCatchup();
        ReactorDurableSubscriptionModel durableSubscriptionModel = new ReactorDurableSubscriptionModel(catchupLayer, storage, durableConfig);
        if (occurrentProperties.getSubscription().resolveMode() != SubscriptionMode.AUTO) {
            // Stopped here rather than after the annotations are scanned, so every subscription is registered on a
            // model that is already stopped and none of them delivers anything until the application starts it. No
            // reactor model implements Spring's Lifecycle, so nothing starts it back up on its own.
            durableSubscriptionModel.stop();
        }
        return durableSubscriptionModel;
    }

    /**
     * The {@link ComposedCatchupModel} bean {@link #occurrentDurableSubscriptionModel} fills. A plain {@code @Bean}
     * rather than {@code @ConditionalOnMissingBean}: an application has no reason to supply its own, since nothing
     * public composes a catch-up layer outside this configuration, and the one caller that fills it is right above.
     */
    @Bean
    public ComposedCatchupModel occurrentComposedCatchupModel() {
        return new ComposedCatchupModel();
    }

    /**
     * Wraps {@code liveModel} in whatever catch-up model the store supports, or returns it unwrapped when the store
     * supports no replay at all. Package-private rather than inlined above so the composition can be asserted without a
     * running MongoDB.
     */
    static CheckpointAwareSubscriptionModel composeCatchupLayer(CheckpointAwareSubscriptionModel liveModel, EventStoreProperties eventStoreProperties,
                                                                ObjectProvider<DcbEventStore> dcbEventStore, ApplicationContext applicationContext) {
        // A combined store has one event store bean that is both the DCB store and the position-ordered stream reader,
        // so it fills both roles. DcbCriteria.all() and Filter.all() are shared by every subscription, which each narrow
        // to their own query or filter in the consumer.
        DcbEventStore dcbStore = eventStoreProperties.getCapabilities().contains(DCB) ? dcbEventStore.getIfAvailable() : null;
        // Resolved through the same neutral narrowing the annotation machinery's replay probe uses, so a user-supplied
        // event store that reads in position order gets a catch-up layer instead of a probe that promises replay over a
        // bare change stream.
        PositionOrderedReader streamStore = PositionOrderedEventStores.find(applicationContext);
        // Stream catch-up needs the STREAM capability, not just a position. A DCB-only store also writes position, so
        // gating on writesPosition() alone would wrongly wire stream catch-up for it.
        boolean streamCatchup = eventStoreProperties.getCapabilities().contains(STREAM) && streamStore != null && streamStore.writesPosition();
        if (dcbStore != null && streamCatchup) {
            return new ReactorCatchupSubscriptionModel(liveModel, streamStore, dcbStore, DcbCriteria.all(), Filter.all());
        } else if (dcbStore != null) {
            return new ReactorCatchupSubscriptionModel(liveModel, dcbStore, DcbCriteria.all());
        } else if (streamCatchup) {
            return new ReactorCatchupSubscriptionModel(liveModel, streamStore, Filter.all());
        }
        return liveModel;
    }

    @Bean
    @Lazy
    @Fallback
    @Conditional(OnMissingCloudEventConverterAndCloudEventTypeMapperCondition.class)
    public CloudEventTypeMapper<E> occurrentTypeMapper() {
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
    public Subscriptions<E> occurrentSubscriptions(ApplicationContext applicationContext, CloudEventConverter<E> cloudEventConverter) {
        return new Subscriptions<>(occurrentAsynchronousSubscribable(applicationContext), cloudEventConverter);
    }

    /**
     * The stream subscription DSL, used by the {@code @StreamSubscription} annotation. See
     * {@link #occurrentSubscriptions(ApplicationContext, CloudEventConverter)} for why the asynchronous model is
     * resolved rather than injected by type.
     */
    @Bean
    @ConditionalOnMissingBean(StreamSubscriptions.class)
    @Conditional(OnSubscriptionsNotDisabledCondition.class)
    public StreamSubscriptions<E> occurrentStreamSubscriptions(ApplicationContext applicationContext, CloudEventConverter<E> cloudEventConverter) {
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
     * The register-only reactive subscription model whose handlers the application service composes into its write
     * chain synchronously, before {@code execute} completes (see {@link SynchronousSubscriptionModel}). It is both the
     * registrar the synchronous subscription DSL registers on and the dispatcher the application service dispatches to,
     * so both must resolve to this same bean.
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
     * A {@link ReactiveTransactionExecutor} that spans the event-store write and the synchronous subscription handlers
     * in one reactive transaction, so a handler whose {@code Mono} errors rolls the write back. Wired into the reactive
     * application service beans below.
     * <p>
     * There is no free lunch: this only pays off while synchronous subscriptions are registered. It is auto-configured
     * whenever the event store is enabled, and can be replaced by defining your own {@link ReactiveTransactionExecutor}
     * bean, for example {@link ReactiveTransactionExecutor#noTransaction()} to make synchronous subscriptions
     * best-effort instead of atomic.
     */
    @Bean
    @ConditionalOnMissingBean(ReactiveTransactionExecutor.class)
    @ConditionalOnProperty(name = "occurrent.event-store.enabled", havingValue = "true", matchIfMissing = true)
    public SpringReactiveTransactionExecutor occurrentSpringReactiveTransactionExecutor(ReactiveMongoTransactionManager transactionManager) {
        return new SpringReactiveTransactionExecutor(transactionManager);
    }

    /**
     * The synchronous counterpart of {@link #occurrentSubscriptions(Subscribable, CloudEventConverter)}, used by the
     * {@code @SynchronousSubscription} annotation. It is the same {@link Subscriptions} type as the asynchronous DSL,
     * so it is given a distinct bean name (and the asynchronous one is {@link Primary}); the annotation processor
     * resolves this one by name.
     */
    @Bean(OccurrentReactorBeanNames.SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME)
    @ConditionalOnMissingBean(name = OccurrentReactorBeanNames.SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME)
    @Conditional(OnSubscriptionsNotDisabledCondition.class)
    public Subscriptions<E> occurrentSynchronousSubscriptions(SynchronousSubscriptionModel synchronousSubscriptionModel, CloudEventConverter<E> cloudEventConverter) {
        return new Subscriptions<>(synchronousSubscriptionModel, cloudEventConverter);
    }

    /**
     * DCB subscription DSL, auto-configured when the DCB event-store capability is enabled. In DCB mode the underlying
     * subscription model wraps a {@code ReactorCatchupSubscriptionModel}, so a subscription started at a
     * {@code GlobalCheckpoint} replays history by position before switching to live delivery.
     */
    @Bean
    @ConditionalOnMissingBean(DcbSubscriptions.class)
    @Conditional({OnDcbEventStoreCapabilityCondition.class, OnSubscriptionsNotDisabledCondition.class})
    public DcbSubscriptions<E> occurrentDcbSubscriptions(FluxSubscriptionModel subscriptionModel, CloudEventConverter<E> cloudEventConverter) {
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
    public ApplicationService<E> occurrentApplicationService(EventStore eventStore, CloudEventConverter<E> cloudEventConverter, OccurrentProperties occurrentProperties,
                                                             ObjectProvider<ReactiveSynchronousEventDispatcher> synchronousEventDispatcher, ObjectProvider<ReactiveTransactionExecutor> transactionExecutor) {
        boolean enableDefaultRetryStrategy = occurrentProperties.getApplicationService().isEnableDefaultRetryStrategy();
        Retry retry = enableDefaultRetryStrategy ? GenericApplicationService.defaultRetry() : Retry.max(0);
        GenericApplicationService.Builder<E> builder = GenericApplicationService.builder(eventStore, cloudEventConverter).retry(retry);
        // Wire the synchronous subscription dispatcher and transaction executor into the builder when present, so a
        // @SynchronousSubscription handler is composed into the write chain, atomically with the write. Both are
        // resolved through ObjectProvider because they only exist when the feature is applicable, and either can be
        // absent or user-replaced.
        synchronousEventDispatcher.ifAvailable(builder::synchronousSubscriptions);
        transactionExecutor.ifAvailable(builder::transactionExecutor);
        return builder.build();
    }

    /**
     * Auto-configures the {@link DcbApplicationService} when the DCB capability is enabled. The {@link TagGenerator} is
     * resolved through {@link ObjectProvider} rather than {@code @ConditionalOnBean(TagGenerator.class)} because
     * {@code @EnableOccurrentReactive} imports this configuration with a plain {@code @Import}, so a
     * {@code @ConditionalOnBean} could be evaluated before a user's own {@link TagGenerator} bean is registered.
     * {@code getIfAvailable()} resolves at instantiation time instead, after all bean definitions exist.
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
                                                                     ObjectProvider<ReactiveSynchronousEventDispatcher> synchronousEventDispatcher, ObjectProvider<ReactiveTransactionExecutor> transactionExecutor) {
        boolean enableDefaultRetryStrategy = occurrentProperties.getApplicationService().isEnableDefaultRetryStrategy();
        Retry retry = enableDefaultRetryStrategy ? GenericDcbApplicationService.defaultRetry() : Retry.max(0);
        GenericDcbApplicationService.Builder<E> builder = GenericDcbApplicationService.builder(eventStore, cloudEventConverter).retry(retry);
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
     * {@code @Fallback} rather than {@code @ConditionalOnMissingBean(TagGenerator.class)}: {@link OccurrentReactiveMongoAutoConfiguration}
     * is activated via {@code @EnableOccurrentReactive}'s {@code @Import}, not {@code spring.factories}/{@code AutoConfiguration.imports},
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
        @SuppressWarnings({"rawtypes", "unchecked"})
        TagGenerator occurrentAnnotationTagGenerator() {
            return new AnnotationTagGenerator<>();
        }
    }
}
