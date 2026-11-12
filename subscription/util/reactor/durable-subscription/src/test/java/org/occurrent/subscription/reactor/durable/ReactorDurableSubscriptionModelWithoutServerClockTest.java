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

import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A subscription from a dynamic start position that answers {@code StartAt.now()}, or from the subscription-model
 * default, over a {@link ReactorMongoSubscriptionModel} whose reply to {@code hello} has no {@code localTime}. Needs no
 * MongoDB, since that reply is all the subscribe asks MongoDB for before the start fails.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorDurableSubscriptionModelWithoutServerClockTest {

    private static final Duration FAILS_WITHIN = Duration.ofSeconds(5);
    private static final String SUBSCRIPTION_ID = "someSubscription";

    private final ReactorMongoSubscriptionModel mongoModel = new ReactorMongoSubscriptionModel(answeringHelloWithoutAClock(), "events",
            TimeRepresentation.RFC_3339_STRING);

    @AfterEach
    void shutdown() {
        mongoModel.shutdown();
    }

    @Test
    void a_dynamic_now_fails_the_start_when_mongodb_gives_no_position() {
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(mongoModel, new InMemoryCheckpointStorage());

        try {
            assertThat(model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(StartAt::now), cloudEvent -> Mono.empty()).waitUntilStarted().toFuture())
                    .as("waiting for the start of the subscription")
                    .failsWithin(FAILS_WITHIN).withThrowableOfType(ExecutionException.class).havingCause()
                    .isInstanceOf(IllegalStateException.class)
                    .withMessageContaining("answered nothing when asked where its feed was when subscribe(..) was called");
        } finally {
            model.shutdown();
        }
    }

    @Test
    void a_dynamic_now_configured_to_start_anyway_fails_the_start_as_in_0_34_0_when_mongodb_gives_no_position() {
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(mongoModel, new InMemoryCheckpointStorage(),
                new ReactorDurableSubscriptionModelConfig(1).startWhenNoStartPositionCanBeRecorded(true));

        try {
            assertThat(model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(StartAt::now), cloudEvent -> Mono.empty()).waitUntilStarted().toFuture())
                    .as("waiting for the start of the subscription")
                    .failsWithin(FAILS_WITHIN).withThrowableOfType(ExecutionException.class).havingCause()
                    .withMessageContaining("has no localTime");
        } finally {
            model.shutdown();
        }
    }

    @ParameterizedTest(name = "startWhenNoStartPositionCanBeRecorded={0}")
    @ValueSource(booleans = {false, true})
    void a_subscription_from_the_model_default_with_nothing_stored_fails_the_start_when_mongodb_gives_no_position(boolean startAnyway) {
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(mongoModel, new InMemoryCheckpointStorage(),
                new ReactorDurableSubscriptionModelConfig(1).startWhenNoStartPositionCanBeRecorded(startAnyway));

        try {
            assertThat(model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), cloudEvent -> Mono.empty()).waitUntilStarted().toFuture())
                    .as("waiting for the start of the subscription")
                    .failsWithin(FAILS_WITHIN).withThrowableOfType(ExecutionException.class).havingCause()
                    .withMessageContaining(startAnyway ? "has no localTime" : "answered nothing when asked for the current position");
        } finally {
            model.shutdown();
        }
    }

    // Answers every command with a reply that has no localTime and refuses every other call, so a test fails when the
    // model asks MongoDB for more than its clock
    private static ReactiveMongoOperations answeringHelloWithoutAClock() {
        return (ReactiveMongoOperations) Proxy.newProxyInstance(ReactiveMongoOperations.class.getClassLoader(), new Class<?>[]{ReactiveMongoOperations.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("executeCommand") && method.getParameterCount() == 1 && args[0] instanceof Document) {
                        return Mono.just(new Document("ok", 1.0));
                    }
                    return switch (method.getName()) {
                        case "toString" -> "ReactiveMongoOperations answering hello without a clock";
                        case "hashCode" -> System.identityHashCode(proxy);
                        case "equals" -> proxy == args[0];
                        default -> throw new UnsupportedOperationException(method.toString());
                    };
                });
    }
}
