/*
 * Copyright 2026 Johan Haleby
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

package org.occurrent.subscription.redis.spring.blocking;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;
import org.occurrent.tck.subscription.blocking.CheckpointStorageConformance;
import org.occurrent.tck.subscription.blocking.CheckpointStorageFixture;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;

@Testcontainers
class SpringRedisCheckpointStorageConformanceTest extends CheckpointStorageConformance {

    @Container
    private static final GenericContainer<?> redisContainer =
            new GenericContainer<>("redis:5.0.3-alpine").withExposedPorts(6379);

    // One connection factory for the class, since each one stands up a Netty event loop group of its own. Redis has no
    // collection to scope a test to, so the fixture flushes instead.
    private static LettuceConnectionFactory connectionFactory;
    private static RedisOperations<String, String> redisTemplate;

    @BeforeAll
    static void connect() {
        connectionFactory = new LettuceConnectionFactory(redisContainer.getHost(), redisContainer.getFirstMappedPort());
        connectionFactory.afterPropertiesSet();
        RedisTemplate<String, String> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        template.afterPropertiesSet();
        redisTemplate = template;
    }

    @AfterAll
    static void disconnect() {
        connectionFactory.destroy();
    }

    @Override
    protected CheckpointStorageFixture createFixture() {
        return new SpringRedisCheckpointStorageFixture(connectionFactory, redisTemplate);
    }

    private static class SpringRedisCheckpointStorageFixture implements CheckpointStorageFixture {

        private final CheckpointStorage storage;

        SpringRedisCheckpointStorageFixture(RedisConnectionFactory connectionFactory,
                                            RedisOperations<String, String> redisTemplate) {
            // The whole database, because a checkpoint is a top-level key here and there is nothing narrower to clear.
            // Same as FlushRedisExtension does for the tests next door.
            connectionFactory.getConnection().flushAll();
            this.storage = new SpringRedisCheckpointStorage(redisTemplate);
        }

        @Override
        public CheckpointStorage checkpointStorage() {
            return storage;
        }

        /**
         * This storage keeps the string a checkpoint reports and nothing else, so everything comes back a
         * {@link StringBasedCheckpoint}. That is the widest divergence between Occurrent's storages, and it is the one
         * that makes the string form of a checkpoint load-bearing rather than a convenience.
         */
        @Override
        public boolean preservesCheckpointType(Checkpoint checkpoint) {
            return checkpoint instanceof StringBasedCheckpoint;
        }

        /**
         * A MongoDB event store with its checkpoints in Redis is a supported combination, and it is the one that makes
         * these two worth declaring here: the change stream hands the storage a resume token or an operation time, and
         * a Redis round trip is where they lose their type. What has to survive is the string, since that is all
         * {@code MongoCommons.applyStartPosition} has left to rebuild a start position from.
         */
        @Override
        public List<Checkpoint> additionalCheckpoints() {
            return List.of(
                    new MongoResumeTokenCheckpoint(new BsonDocument("_data", new BsonString("82ABCDEF"))),
                    new MongoOperationTimeCheckpoint(new BsonTimestamp(1735689600, 1)));
        }
    }
}
