/*
 *
 *  Copyright 2026 Johan Haleby
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

import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.CheckpointWriteVersionSource;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy.CompetingConsumerListener;
import org.occurrent.subscription.api.blocking.DelegatingSubscriptionModel;
import org.occurrent.subscription.api.blocking.ManualStartSubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelConfig;
import org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.UseCheckpointInStorage;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoLeaseCompetingConsumerStrategy;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.lang.reflect.Field;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The starter passes {@code strategy::fencingToken} at every place it builds a checkpoint-writing model (ADR 116),
 * lazily and through {@link org.springframework.beans.factory.ObjectProvider#getIfUnique()}. Container-free. A mocked
 * {@link MongoTemplate} and {@link SpringMongoEventStore} stand in for the collaborators these tests never touch, so
 * a checkpoint write is never actually attempted. Each wiring site is instead proven by reading, through reflection,
 * the {@link CheckpointWriteVersionSource} the model was constructed with and asking it directly, the way
 * {@code OccurrentMongoAutoConfigurationCharacterizationTest} reads private fields to characterize the same bean.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerFencingWiringTest {

    private static final String SUBSCRIPTION_ID = "fenced-subscription";

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(OccurrentMongoAutoConfiguration.class))
            .withUserConfiguration(
                    OccurrentMongoAutoConfigurationCharacterizationTest.EnabledOccurrentConfiguration.class,
                    OccurrentMongoAutoConfigurationCharacterizationTest.TestEventTypeMapperConfiguration.class)
            .withBean(MongoDatabaseFactory.class, () -> mock(MongoDatabaseFactory.class))
            .withBean(MongoTemplate.class, () -> mock(MongoTemplate.class))
            .withBean(SpringMongoEventStore.class, () -> mock(SpringMongoEventStore.class))
            .withPropertyValues(
                    "occurrent.event-store.enabled=true",
                    "occurrent.subscription.mode=auto",
                    "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:test",
                    "occurrent.application-service.enable-default-retry-strategy=false"
            );

    @Test
    void one_strategy_bean_wires_a_working_source_into_the_durable_model_and_the_catch_up_config() {
        SpringMongoLeaseCompetingConsumerStrategy strategy = mock(SpringMongoLeaseCompetingConsumerStrategy.class);
        when(strategy.fencingToken(SUBSCRIPTION_ID)).thenReturn(OptionalLong.of(42L));

        contextRunner
                .withBean(SpringMongoLeaseCompetingConsumerStrategy.class, () -> strategy)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    SubscriptionModel competingConsumerSubscriptionModel = context.getBean(SubscriptionModel.class);
                    CatchupSubscriptionModel catchupSubscriptionModel = findDelegate(competingConsumerSubscriptionModel, CatchupSubscriptionModel.class);
                    DurableSubscriptionModel durableSubscriptionModel = findDelegate(catchupSubscriptionModel, DurableSubscriptionModel.class);

                    CheckpointWriteVersionSource durableModelSource = getField(durableSubscriptionModel, "writeVersionSource", CheckpointWriteVersionSource.class);
                    assertThat(durableModelSource.writeVersion(SUBSCRIPTION_ID)).isEqualTo(OptionalLong.of(42L));

                    Object streamCatchup = getField(catchupSubscriptionModel, "streamCatchupSubscriptionModel", Object.class);
                    CatchupSubscriptionModelConfig catchupConfig = (CatchupSubscriptionModelConfig) getFieldFromClass(streamCatchup, "config",
                            "org.occurrent.subscription.blocking.durable.catchup.AbstractCatchupSubscriptionModel");
                    UseCheckpointInStorage catchupPhaseConfig = (UseCheckpointInStorage) catchupConfig.subscriptionStorageConfig;
                    assertThat(catchupPhaseConfig.checkpointWriteVersionSource()).isNotNull();
                    assertThat(catchupPhaseConfig.checkpointWriteVersionSource().writeVersion(SUBSCRIPTION_ID)).isEqualTo(OptionalLong.of(42L));
                });
    }

    @Test
    void one_strategy_bean_wires_a_working_source_into_the_manual_start_position_pin() {
        SpringMongoLeaseCompetingConsumerStrategy strategy = mock(SpringMongoLeaseCompetingConsumerStrategy.class);
        when(strategy.fencingToken(SUBSCRIPTION_ID)).thenReturn(OptionalLong.of(99L));

        contextRunner
                .withBean(SpringMongoLeaseCompetingConsumerStrategy.class, () -> strategy)
                .withPropertyValues("occurrent.subscription.mode=manual")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    SubscriptionModel subscriptionModel = context.getBean(SubscriptionModel.class);
                    assertThat(subscriptionModel).isInstanceOf(ManualStartSubscriptionModel.class);

                    CheckpointWriteVersionSource source = getField(subscriptionModel, "writeVersionSource", CheckpointWriteVersionSource.class);
                    assertThat(source.writeVersion(SUBSCRIPTION_ID)).isEqualTo(OptionalLong.of(99L));
                });
    }

    @Test
    void two_strategy_beans_still_start_the_context_with_no_fence_wired() {
        SpringMongoLeaseCompetingConsumerStrategy strategy = mock(SpringMongoLeaseCompetingConsumerStrategy.class);
        // Stubbed to prove the ambiguity is what suppresses the fence, not this strategy simply having no token.
        when(strategy.fencingToken(SUBSCRIPTION_ID)).thenReturn(OptionalLong.of(7L));

        contextRunner
                // The concrete-typed occurrentCompetingDurableSubscriptionModel parameter still resolves this one
                // bean uniquely, while a second CompetingConsumerStrategy of a different type makes the
                // ObjectProvider<CompetingConsumerStrategy> lookup ambiguous, which is what the fence reacts to.
                .withBean(SpringMongoLeaseCompetingConsumerStrategy.class, () -> strategy)
                .withBean(CompetingConsumerStrategy.class, RivalCompetingConsumerStrategy::new)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    SubscriptionModel competingConsumerSubscriptionModel = context.getBean("occurrentCompetingDurableSubscriptionModel", SubscriptionModel.class);
                    CatchupSubscriptionModel catchupSubscriptionModel = findDelegate(competingConsumerSubscriptionModel, CatchupSubscriptionModel.class);
                    DurableSubscriptionModel durableSubscriptionModel = findDelegate(catchupSubscriptionModel, DurableSubscriptionModel.class);

                    CheckpointWriteVersionSource durableModelSource = getField(durableSubscriptionModel, "writeVersionSource", CheckpointWriteVersionSource.class);
                    assertThat(durableModelSource.writeVersion(SUBSCRIPTION_ID)).isEmpty();
                });
    }

    @Test
    void a_user_declared_checkpoint_storage_bean_is_not_wrapped() {
        CheckpointStorage userStorage = mock(CheckpointStorage.class);
        SpringMongoLeaseCompetingConsumerStrategy strategy = mock(SpringMongoLeaseCompetingConsumerStrategy.class);
        when(strategy.fencingToken(SUBSCRIPTION_ID)).thenReturn(OptionalLong.of(1L));

        contextRunner
                .withBean(CheckpointStorage.class, () -> userStorage)
                .withBean(SpringMongoLeaseCompetingConsumerStrategy.class, () -> strategy)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(CheckpointStorage.class)).isSameAs(userStorage);

                    SubscriptionModel competingConsumerSubscriptionModel = context.getBean(SubscriptionModel.class);
                    CatchupSubscriptionModel catchupSubscriptionModel = findDelegate(competingConsumerSubscriptionModel, CatchupSubscriptionModel.class);
                    DurableSubscriptionModel durableSubscriptionModel = findDelegate(catchupSubscriptionModel, DurableSubscriptionModel.class);
                    CheckpointStorage storageUsedByModel = getField(durableSubscriptionModel, "storage", CheckpointStorage.class);
                    assertThat(storageUsedByModel).isSameAs(userStorage);
                });
    }

    private static <T> T findDelegate(SubscriptionModel subscriptionModel, Class<T> type) {
        SubscriptionModel current = subscriptionModel;
        while (true) {
            if (type.isInstance(current)) {
                return type.cast(current);
            }
            if (current instanceof DelegatingSubscriptionModel delegatingSubscriptionModel) {
                current = delegatingSubscriptionModel.getDelegatedSubscriptionModel();
            } else {
                throw new IllegalStateException("Could not find delegate of type " + type.getName());
            }
        }
    }

    // Searches the class hierarchy, since a wiring site's field is sometimes declared on an abstract superclass
    // (AbstractCatchupSubscriptionModel.config) rather than the concrete runtime class.
    private static <T> T getField(Object target, String fieldName, Class<T> type) {
        Class<?> current = target.getClass();
        while (current != null) {
            try {
                Field field = current.getDeclaredField(fieldName);
                field.setAccessible(true);
                return type.cast(field.get(target));
            } catch (NoSuchFieldException e) {
                current = current.getSuperclass();
            } catch (IllegalAccessException e) {
                throw new IllegalStateException("Could not read field " + fieldName + " from " + target.getClass().getName(), e);
            }
        }
        throw new IllegalStateException("Could not find field " + fieldName + " in " + target.getClass().getName() + " or a superclass");
    }

    @NonNull
    private static Object getFieldFromClass(Object target, String fieldName, String declaringClassName) {
        try {
            Class<?> declaringClass = Class.forName(declaringClassName);
            Field field = declaringClass.getDeclaredField(fieldName);
            field.setAccessible(true);
            Object value = field.get(target);
            if (value == null) {
                throw new IllegalStateException(fieldName + " on " + declaringClassName + " was null");
            }
            return value;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Could not read field " + fieldName + " from " + declaringClassName, e);
        }
    }

    // A second CompetingConsumerStrategy of a different type than SpringMongoLeaseCompetingConsumerStrategy, so
    // ObjectProvider<CompetingConsumerStrategy>.getIfUnique() sees two candidates and answers null. Never actually
    // used for locking in these tests.
    private static final class RivalCompetingConsumerStrategy implements CompetingConsumerStrategy {
        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            return false;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            return false;
        }

        @Override
        public void addListener(CompetingConsumerListener listenerConsumer) {
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {
        }
    }
}
