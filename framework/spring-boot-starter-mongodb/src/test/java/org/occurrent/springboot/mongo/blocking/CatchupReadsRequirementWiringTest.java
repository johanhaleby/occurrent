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
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.internal.CatchupReads;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
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
 * A catch-up needs an event store that opens a read scope, so an {@link EventStoreQueries} bean that is not a MongoDB store fails the context at startup. Container-free, since nothing here reaches MongoDB.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupReadsRequirementWiringTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(OccurrentMongoAutoConfiguration.class))
            .withUserConfiguration(
                    OccurrentMongoAutoConfigurationCharacterizationTest.EnabledOccurrentConfiguration.class,
                    OccurrentMongoAutoConfigurationCharacterizationTest.TestEventTypeMapperConfiguration.class)
            .withBean(MongoDatabaseFactory.class, () -> mock(MongoDatabaseFactory.class))
            .withBean(MongoTemplate.class, () -> mock(MongoTemplate.class))
            .withBean(CheckpointStorage.class, InMemoryCheckpointStorage::new)
            .withPropertyValues(
                    "occurrent.event-store.enabled=false",
                    "occurrent.subscription.competing-consumer.fence-checkpoints=false",
                    "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:test",
                    "occurrent.application-service.enable-default-retry-strategy=false"
            );

    @Test
    void an_event_store_queries_bean_that_does_not_implement_catchup_reads_fails_the_context_at_startup() {
        contextRunner
                .withBean(EventStoreQueries.class, () -> mock(EventStoreQueries.class))
                .run(context -> assertThat(context).getFailure().rootCause()
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("CatchupReads"));
    }

    @Test
    void an_event_store_queries_bean_that_implements_catchup_reads_starts_with_a_catchup_over_it() {
        contextRunner
                .withBean(EventStoreQueries.class, () -> mock(QueriesWithCatchupReads.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(SubscriptionModel.class)).isNotNull();
                });
    }

    interface QueriesWithCatchupReads extends EventStoreQueries, CatchupReads {
    }

    private static final class InMemoryCheckpointStorage implements CheckpointStorage {
        private final Map<String, Checkpoint> checkpoints = new HashMap<>();

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
}
