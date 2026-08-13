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

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.ManualStartSubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * {@code occurrent.subscription.mode=manual} records a start position at registration with a conditional write, so the
 * starter refuses a checkpoint storage that cannot evaluate one rather than leaving that to the first registration.
 * Container-free, the way {@link CompetingConsumerFencingWiringTest} is, since nothing
 * here reaches MongoDB. The competing-consumer fence is turned off in both tests so that the storage reaches this
 * refusal rather than the fence's own.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SubscriptionModeManualWiringTest {

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
                    "occurrent.subscription.mode=manual",
                    "occurrent.subscription.competing-consumer.fence-checkpoints=false",
                    "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:test",
                    "occurrent.application-service.enable-default-retry-strategy=false"
            );

    @Test
    void a_checkpoint_storage_that_cannot_evaluate_write_conditions_refuses_to_start() {
        contextRunner
                .withBean(CheckpointStorage.class, UnconditionalCheckpointStorage::new)
                .run(context -> assertThat(context).getFailure().rootCause()
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining(UnconditionalCheckpointStorage.class.getName()));
    }

    @Test
    void a_checkpoint_storage_that_evaluates_them_gets_the_manual_start_model() {
        contextRunner
                .withBean(CheckpointStorage.class, ConditionalCheckpointStorage::new)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(SubscriptionModel.class)).isInstanceOf(ManualStartSubscriptionModel.class);
                });
    }

    // This one ignores the condition and writes anyway, and it leaves evaluatesWriteConditions() at its default of
    // false, which is the only thing the factory asks about. Another storage answering false may refuse the write
    // instead.
    private static class UnconditionalCheckpointStorage implements CheckpointStorage {
        final Map<String, Checkpoint> checkpoints = new HashMap<>();

        @Override
        public Checkpoint read(String subscriptionId) {
            return checkpoints.get(subscriptionId);
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            checkpoints.put(subscriptionId, checkpoint);
            return checkpoint;
        }

        @Override
        public OptionalLong writeVersion(String subscriptionId) {
            return OptionalLong.empty();
        }

        @Override
        public void delete(String subscriptionId) {
            checkpoints.remove(subscriptionId);
        }

        @Override
        public boolean exists(String subscriptionId) {
            return checkpoints.containsKey(subscriptionId);
        }
    }

    private static final class ConditionalCheckpointStorage extends UnconditionalCheckpointStorage {
        @Override
        public boolean evaluatesWriteConditions() {
            return true;
        }
    }
}
