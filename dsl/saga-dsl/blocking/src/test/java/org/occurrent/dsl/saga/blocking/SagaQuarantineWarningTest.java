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

package org.occurrent.dsl.saga.blocking;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.HistoryRetainingSubscriptions;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.api.blocking.Subscription;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * What the runner says when it switches quarantine off, which is the only place an operator learns that this saga has
 * no instance isolation. The message has to be actionable, so what it must not say matters as much as what it says. An
 * earlier version told every feed to move to a catch-up model over MongoDB, which named the configuration a catching-up
 * push saga was already running as its own remedy.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaQuarantineWarningTest {

    sealed interface OrderEvent permits OrderPlaced {
        String orderId();
    }

    record OrderPlaced(String orderId) implements OrderEvent {
    }

    record ShipOrder(String orderId) {
    }

    private ListAppender<ILoggingEvent> appender;
    private Logger runnerLog;
    private @Nullable Level originalLevel;
    private CloudEventConverter<OrderEvent> converter;

    @BeforeEach
    void startCapturing() {
        converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test")).build();
        appender = new ListAppender<>();
        appender.start();
        runnerLog = (Logger) LoggerFactory.getLogger(SagaRunner.class);
        runnerLog.addAppender(appender);
        // Restored in teardown. The level is process-global, so leaving it set would silence lower-level logging in
        // whichever test happens to run next.
        originalLevel = runnerLog.getLevel();
        runnerLog.setLevel(Level.WARN);
    }

    @AfterEach
    void stopCapturing() {
        runnerLog.setLevel(originalLevel);
        runnerLog.detachAppender(appender);
        appender.stop();
    }

    @Test
    void names_the_model_that_retains_nothing_and_says_isolation_is_gone() {
        run(new RetainsNothing());

        assertAll(
                () -> assertThat(warnings()).hasSize(1),
                () -> assertThat(warning()).contains("cannot say whether it still holds an event it delivered"),
                () -> assertThat(warning()).contains(RetainsNothing.class.getName()),
                () -> assertThat(warning()).contains("blocks every other instance")
        );
    }

    /**
     * The remedy an operator is given has to be one they do not already have. An earlier message sent every feed to a
     * catch-up model over MongoDB, which a push saga catching up from the event store was already running, and which
     * does not make its events retained anyway.
     */
    @Test
    void does_not_offer_a_catch_up_model_as_the_remedy_for_a_feed_that_cannot_have_one() {
        run(new RetainsNothing());

        assertAll(
                () -> assertThat(warning()).doesNotContain("Run the saga on one of the MongoDB subscription models"),
                () -> assertThat(warning()).contains("A push feed on its own does not"),
                () -> assertThat(warning()).contains("issues/918")
        );
    }

    @Test
    void says_nothing_at_all_when_the_model_holds_everything_it_delivers() {
        run(new Retains());

        assertThat(warnings()).isEmpty();
    }

    /**
     * A model that does not guarantee holding everything is worth a line before the incident, since that saga may be
     * quarantined for some events and not for others. The verdict per event is not knowable at startup, and neither is
     * whether this particular feed will ever produce a refusal, so the message says what the answer depends on rather
     * than diagnosing a source it cannot see.
     */
    @Test
    void says_that_quarantine_depends_on_the_event_when_the_model_holds_only_some_of_them() {
        run(new RetainsSomeEvents());

        assertAll(
                () -> assertThat(warnings()).hasSize(1),
                () -> assertThat(warning()).contains("cannot guarantee it holds every event it delivers"),
                () -> assertThat(warning()).contains("depends on the event it stopped on"),
                () -> assertThat(warning()).contains("that refusal is logged when it happens")
        );
    }

    /**
     * A saga that switched the budget off asked for the blocking behaviour, so it has given nothing up and is not
     * warned. Without this the message would fire for every saga that deliberately opted out.
     */
    @Test
    void says_nothing_when_quarantine_was_switched_off_deliberately() {
        SagaRunner.<OrderEvent, ShipOrder>agnostic(new RetainsNothing(), converter)
                .run("orders", saga(), SagaStateStore.inMemory(), c -> {
                }, null, SagaRunnerConfig.defaults().withQuarantineAfter(null))
                .close();

        assertThat(warnings()).isEmpty();
    }

    private void run(Subscribable model) {
        SagaRunner.<OrderEvent, ShipOrder>agnostic(model, converter)
                .run("orders", saga(), SagaStateStore.inMemory(), c -> {
                }, null, SagaRunnerConfig.defaults().withQuarantineAfter(Duration.ofMinutes(5)))
                .close();
    }

    private Saga<OrderEvent, String, ShipOrder> saga() {
        return Saga.<OrderEvent, String, ShipOrder>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> e.orderId())
                .build();
    }

    private List<String> warnings() {
        return new ArrayList<>(appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList());
    }

    private String warning() {
        return warnings().getFirst();
    }

    private static class RetainsNothing implements Subscribable {
        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return new NoopSubscription(subscriptionId);
        }
    }

    private static final class Retains extends RetainsNothing implements HistoryRetainingSubscriptions {
        @Override
        public boolean retains(CloudEvent event) {
            return true;
        }

        @Override
        public boolean retainsEveryEvent() {
            return true;
        }
    }

    // Can answer, and for some events the answer is no, which is what earns the startup line about it depending on
    // the event.
    private static final class RetainsSomeEvents extends RetainsNothing implements HistoryRetainingSubscriptions {
        @Override
        public boolean retains(CloudEvent event) {
            return false;
        }
    }

    private record NoopSubscription(String id) implements Subscription {
        @Override
        public void waitUntilStarted() {
        }

        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }
}
