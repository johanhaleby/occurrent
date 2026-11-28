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

package org.occurrent.subscription.push.blocking;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.internal.SharedStorageMessages;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Under {@code startupMode = BACKGROUND} nobody waits for the replay, so the log is the only place a failed catch-up
 * reaches anyone, and it is where the recovery is told.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupThenPushSubscriptionModelFailureLogTest {

    private ListAppender<ILoggingEvent> appender;
    private Logger modelLog;
    private CatchupThenPushSubscriptionModel model;

    @BeforeEach
    void startCapturing() {
        appender = new ListAppender<>();
        appender.start();
        modelLog = (Logger) LoggerFactory.getLogger(CatchupThenPushSubscriptionModel.class);
        modelLog.addAppender(appender);
    }

    @AfterEach
    void stopCapturing() {
        if (model != null) {
            model.shutdown();
        }
        modelLog.detachAppender(appender);
        appender.stop();
    }

    @Test
    void a_failed_catch_up_logs_how_to_change_what_is_stored_when_several_instances_run_it() {
        PushSubscriptionModel liveFeed = new PushSubscriptionModel();
        model = new CatchupThenPushSubscriptionModel(failingReader(), liveFeed, null);

        var subscription = model.subscribe("sub", null, StartAt.subscriptionModelDefault(), cloudEvent -> {
        });
        Throwable replayFailure = catchThrowable(subscription::waitUntilStarted);

        assertThat(replayFailure).isInstanceOf(IllegalStateException.class).hasMessageContaining("replay boom");
        assertThat(appender.list).filteredOn(event -> event.getLevel() == Level.ERROR).singleElement().satisfies(event ->
                assertThat(event.getFormattedMessage())
                        .startsWith("Catch-up failed for subscription sub. Its registration on the live feed is kept")
                        .endsWith(SharedStorageMessages.MARKER_WHEN_SEVERAL_INSTANCES_RUN_IT));
    }

    private static PositionOrderedReader failingReader() {
        return new PositionOrderedReader() {
            @Override
            public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                throw new IllegalStateException("replay boom");
            }

            @Override
            public long currentPosition() {
                return 0;
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
    }
}
