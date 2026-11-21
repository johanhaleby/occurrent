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

package org.occurrent.subscription.push.reactor;

import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.internal.SharedStorageMessages;
import org.slf4j.event.Level;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Under {@code startupMode = BACKGROUND} nobody waits for the replay, so the log is the only place a failed catch-up
 * reaches anyone, and it is where the recovery is told.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupThenPushSubscriptionModelFailureLogTest {

    @BeforeEach
    @AfterEach
    void forgetWhatWasLogged() {
        CapturingLoggerProvider.clear();
    }

    @Test
    void a_failed_catch_up_logs_how_to_change_what_is_stored_when_several_instances_run_it() {
        PushSubscriptionModel liveFeed = new PushSubscriptionModel();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(failingReader(), liveFeed, null);

        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), cloudEvent -> Mono.empty());

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(CapturingLoggerProvider.logged(CatchupThenPushSubscriptionModel.class, Level.ERROR)).singleElement().satisfies(logged ->
                        assertThat(logged.message())
                                .startsWith("Catch-up failed for subscription sub. Its registration on the live feed is kept")
                                .endsWith(SharedStorageMessages.MARKER_WHEN_SEVERAL_INSTANCES_RUN_IT)));
    }

    private static PositionOrderedReader failingReader() {
        return new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return Flux.error(new IllegalStateException("replay boom"));
            }

            @Override
            public Mono<Long> currentPosition() {
                return Mono.just(0L);
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
    }
}
