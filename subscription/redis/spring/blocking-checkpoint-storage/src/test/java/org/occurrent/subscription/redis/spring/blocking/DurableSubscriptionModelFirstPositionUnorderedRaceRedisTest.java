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

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StartPositionAlreadyPinnedException;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * What {@link DurableSubscriptionModel} does when another node stores a checkpoint between the position this node read
 * and its first-position write, over {@link SpringRedisCheckpointStorage}, which can't order two checkpoints and so
 * answers the three-argument {@code resolveFirstCheckpointRace} empty for any stored one.
 */
@Timeout(30)
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
class DurableSubscriptionModelFirstPositionUnorderedRaceRedisTest {

    private static final String FIRST_POSITION = "first-position";

    @Container
    private static final GenericContainer<?> redisContainer = new GenericContainer<>("redis:5.0.3-alpine").withExposedPorts(6379);

    @RegisterExtension
    FlushRedisExtension flushRedisExtension = new FlushRedisExtension(redisContainer.getHost(), redisContainer.getFirstMappedPort());

    private LettuceConnectionFactory lettuceConnectionFactory;
    private RedisOperations<String, String> redis;
    private String subscriptionId;

    @BeforeEach
    void connect() {
        lettuceConnectionFactory = new LettuceConnectionFactory(redisContainer.getHost(), redisContainer.getFirstMappedPort());
        lettuceConnectionFactory.afterPropertiesSet();
        RedisTemplate<String, String> template = new RedisTemplate<>();
        template.setConnectionFactory(lettuceConnectionFactory);
        template.afterPropertiesSet();
        redis = template;
        subscriptionId = UUID.randomUUID().toString();
    }

    @AfterEach
    void disconnect() {
        lettuceConnectionFactory.destroy();
    }

    @Test
    void a_different_first_position_another_node_stored_with_an_older_version_refuses_the_subscribe_and_is_left_as_it_was() {
        RaceHookStorage storage = new RaceHookStorage(redis);
        storage.beforeTheResolver = () -> storage.save(subscriptionId, new StringBasedCheckpoint("stale-nodes-position"), CheckpointWriteCondition.notOlderThan(1));
        Feed feed = new Feed();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage, id -> OptionalLong.of(2));

        Throwable thrown = catchThrowable(() -> durable.subscribe(subscriptionId, __ -> {
        }));

        assertAll(
                () -> assertThat(thrown).as("thrown by subscribe").isInstanceOfSatisfying(StartPositionAlreadyPinnedException.class, e -> {
                    assertThat(e.positionRead.asString()).as("position read").isEqualTo(FIRST_POSITION);
                    assertThat(e.positionStored.map(Checkpoint::asString)).as("position stored").hasValue("stale-nodes-position");
                }),
                () -> assertThat(storage.read(subscriptionId).asString()).as("stored position").isEqualTo("stale-nodes-position"),
                () -> assertThat(storage.writeVersion(subscriptionId)).as("stored version").hasValue(1L),
                () -> assertThat(feed.positionsStartedFrom).as("positions started from").isEmpty()
        );
    }

    @Test
    void a_different_first_position_another_node_stored_with_no_version_refuses_the_subscribe_and_is_left_as_it_was() {
        RaceHookStorage storage = new RaceHookStorage(redis);
        storage.beforeTheResolver = () -> storage.save(subscriptionId, new StringBasedCheckpoint("other-nodes-position"));
        Feed feed = new Feed();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage, id -> OptionalLong.of(2));

        Throwable thrown = catchThrowable(() -> durable.subscribe(subscriptionId, __ -> {
        }));

        assertAll(
                () -> assertThat(thrown).as("thrown by subscribe").isInstanceOfSatisfying(StartPositionAlreadyPinnedException.class,
                        e -> assertThat(e.positionStored.map(Checkpoint::asString)).as("position stored").hasValue("other-nodes-position")),
                () -> assertThat(storage.read(subscriptionId).asString()).as("stored position").isEqualTo("other-nodes-position"),
                () -> assertThat(storage.writeVersion(subscriptionId)).as("stored version").isEmpty(),
                () -> assertThat(feed.positionsStartedFrom).as("positions started from").isEmpty()
        );
    }

    @Test
    void a_different_first_position_another_node_stored_with_a_newer_version_refuses_the_subscribe_and_is_left_as_it_was() {
        RaceHookStorage storage = new RaceHookStorage(redis);
        storage.beforeTheResolver = () -> storage.save(subscriptionId, new StringBasedCheckpoint("newer-nodes-position"), CheckpointWriteCondition.notOlderThan(3));
        Feed feed = new Feed();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage, id -> OptionalLong.of(2));

        Throwable thrown = catchThrowable(() -> durable.subscribe(subscriptionId, __ -> {
        }));

        assertAll(
                () -> assertThat(thrown).as("thrown by subscribe").isInstanceOfSatisfying(CheckpointWriteConditionNotFulfilledException.class,
                        e -> assertThat(e.storedVersion).as("stored version reported").hasValue(3L)),
                () -> assertThat(storage.read(subscriptionId).asString()).as("stored position").isEqualTo("newer-nodes-position"),
                () -> assertThat(storage.writeVersion(subscriptionId)).as("stored version").hasValue(3L),
                () -> assertThat(feed.positionsStartedFrom).as("positions started from").isEmpty()
        );
    }

    @Test
    void the_very_position_this_node_read_stored_by_another_node_is_started_from_and_gets_this_nodes_version() {
        RaceHookStorage storage = new RaceHookStorage(redis);
        storage.beforeTheResolver = () -> storage.save(subscriptionId, new StringBasedCheckpoint(FIRST_POSITION), CheckpointWriteCondition.notOlderThan(1));
        Feed feed = new Feed();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage, id -> OptionalLong.of(2));

        Throwable thrown = catchThrowable(() -> durable.subscribe(subscriptionId, __ -> {
        }));

        assertAll(
                () -> assertThat(thrown).as("thrown by subscribe").isNull(),
                () -> assertThat(storage.read(subscriptionId).asString()).as("stored position").isEqualTo(FIRST_POSITION),
                () -> assertThat(storage.writeVersion(subscriptionId)).as("stored version").hasValue(2L),
                () -> assertThat(feed.positionsStartedFrom).as("positions started from").containsExactly(FIRST_POSITION)
        );
    }

    @Test
    void a_retry_after_a_first_position_write_whose_reply_was_lost_starts_from_the_position_that_write_stored() {
        AtomicInteger attempts = new AtomicInteger();
        StringRedisTemplate losingTheFirstReply = new StringRedisTemplate(lettuceConnectionFactory) {
            @Override
            public <T> T execute(RedisScript<T> script, RedisSerializer<?> argsSerializer, RedisSerializer<T> resultSerializer, List<String> keys, Object... args) {
                T reply = super.execute(script, argsSerializer, resultSerializer, keys, args);
                if (attempts.getAndIncrement() == 0) {
                    throw new RedisConnectionFailureException("Connection lost after the script ran");
                }
                return reply;
            }
        };
        losingTheFirstReply.afterPropertiesSet();
        SpringRedisCheckpointStorage storage = new SpringRedisCheckpointStorage(losingTheFirstReply, RetryStrategy.fixed(1L).maxAttempts(3));
        Feed feed = new Feed();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage, id -> OptionalLong.of(2));

        Throwable thrown = catchThrowable(() -> durable.subscribe(subscriptionId, __ -> {
        }));

        assertAll(
                () -> assertThat(thrown).as("thrown by subscribe").isNull(),
                () -> assertThat(attempts.get()).as("the attempt that wrote and the retry").isGreaterThanOrEqualTo(2),
                () -> assertThat(storage.read(subscriptionId).asString()).as("stored position").isEqualTo(FIRST_POSITION),
                () -> assertThat(storage.writeVersion(subscriptionId)).as("stored version").hasValue(2L),
                () -> assertThat(feed.positionsStartedFrom).as("positions started from").containsExactly(FIRST_POSITION)
        );
    }

    /**
     * Runs a hook, standing in for another node writing, right before the three-argument resolver does.
     */
    private static final class RaceHookStorage extends SpringRedisCheckpointStorage {
        @Nullable Runnable beforeTheResolver;

        RaceHookStorage(RedisOperations<String, String> redis) {
            super(redis);
        }

        @Override
        public Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate, long writeVersion) {
            if (beforeTheResolver != null) {
                beforeTheResolver.run();
            }
            return super.resolveFirstCheckpointRace(subscriptionId, candidate, writeVersion);
        }
    }

    /**
     * A feed whose model default is {@link #FIRST_POSITION}, and which evaluates a dynamic start position on subscribe,
     * recording the checkpoint it starts from.
     */
    private static final class Feed implements CheckpointAwareSubscriptionModel {
        final List<String> positionsStartedFrom = new ArrayList<>();

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            if (startAt.isDynamic()) {
                StartAt resolved = startAt.get(new SubscriptionModelContext(Feed.class));
                if (resolved instanceof StartAt.StartAtCheckpoint checkpoint) {
                    positionsStartedFrom.add(checkpoint.checkpoint.asString());
                }
            }
            return new Subscription() {
                @Override
                public String id() {
                    return subscriptionId;
                }

                @Override
                public boolean waitUntilStarted(Duration timeout) {
                    return true;
                }
            };
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return new StringBasedCheckpoint(FIRST_POSITION);
        }

        @Override
        public void stop() {
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return true;
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
        }
    }
}
