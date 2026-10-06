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

package org.occurrent.subscription.reactor.durable;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.connection.TransportSettings;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A subscribe made inside a callback of the reactive MongoDB driver runs on the driver's own event loop thread, which
 * Reactor does not count as a thread that may not block. The driver here has one such thread, so a subscribe that
 * waited there for MongoDB to answer would wait for an answer only that thread can deliver.
 */
@Timeout(60)
@Testcontainers
class ReactorDurableSubscriptionModelNettyEventLoopTest {
    private static final String DATABASE = "netty-event-loop";

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private EventLoopGroup eventLoop;
    private MongoClient mongoClient;
    private ReactorDurableSubscriptionModel model;

    @BeforeEach
    void create_a_mongodb_client_with_one_event_loop_thread() {
        eventLoop = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(mongoDBContainer.getReplicaSetUrl(DATABASE)))
                .transportSettings(TransportSettings.nettyBuilder().eventLoopGroup(eventLoop).build())
                .build();
        mongoClient = MongoClients.create(settings);
        ReactiveMongoTemplate template = new ReactiveMongoTemplate(mongoClient, DATABASE);
        ReactorMongoSubscriptionModel mongoModel = new ReactorMongoSubscriptionModel(template, "events", TimeRepresentation.RFC_3339_STRING);
        model = new ReactorDurableSubscriptionModel(feedOnly(mongoModel), new ReactorCheckpointStorage(template, "checkpoints"));
    }

    @AfterEach
    void shutdown() {
        // Also ends a subscribe still waiting on the event loop thread, so the client can close
        model.shutdown();
        mongoClient.close();
        eventLoop.shutdownGracefully(0, 0, TimeUnit.SECONDS).syncUninterruptibly();
    }

    static Stream<Arguments> startPositionsReadFromMongoDB() {
        return Stream.of(
                Arguments.of(Named.of("the model default", StartAt.subscriptionModelDefault())),
                Arguments.of(Named.of("now", StartAt.now())));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("startPositionsReadFromMongoDB")
    void a_subscribe_made_on_the_only_event_loop_thread_of_the_mongodb_driver_returns_and_the_subscription_starts(StartAt startAt) {
        // Given
        AtomicBoolean onTheEventLoop = new AtomicBoolean();

        // When
        CompletableFuture<Subscription> subscribed = Mono.from(mongoClient.getDatabase(DATABASE).runCommand(new Document("ping", 1)))
                .map(__ -> {
                    onTheEventLoop.set(eventLoop.next().inEventLoop());
                    return model.subscribe("subscription", null, startAt, cloudEvent -> Mono.empty());
                })
                .toFuture();

        // Then
        assertThat(subscribed).as("the subscribe").succeedsWithin(Duration.ofSeconds(10));
        assertThat(onTheEventLoop).as("subscribed on the event loop thread").isTrue();
        assertThat(subscribed.join().waitUntilStarted().toFuture()).as("the start").succeedsWithin(Duration.ofSeconds(10));
    }

    // The durable model reads the feed itself when the wrapped model has no named subscriptions of its own
    private static CheckpointAwareSubscriptionModel feedOnly(ReactorMongoSubscriptionModel wrapped) {
        return new CheckpointAwareSubscriptionModel() {
            @Override
            public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
                return wrapped.subscribe(filter, startAt);
            }

            @Override
            public Mono<Checkpoint> globalCheckpoint() {
                return wrapped.globalCheckpoint();
            }

            @Override
            public Mono<Checkpoint> globalCheckpointAsOfNow() {
                return wrapped.globalCheckpointAsOfNow();
            }
        };
    }
}
