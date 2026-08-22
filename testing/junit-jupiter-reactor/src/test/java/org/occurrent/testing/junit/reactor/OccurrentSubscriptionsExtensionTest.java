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

package org.occurrent.testing.junit.reactor;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.v1.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModelLifeCycle;
import org.occurrent.subscription.synchronous.reactor.SynchronousSubscriptionModel;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Container-free, like the blocking twin's own test. There is no reactive in-memory event store to drive a durable
 * model with, so {@code SynchronousSubscriptionModel} is the delivery vehicle: it needs no store, dispatch is a direct
 * method call, and it is life-cycle bearing and introspectable, which is everything these tests need.
 */
class OccurrentSubscriptionsExtensionTest {

    @Test
    void subscription_registered_before_the_test_does_not_receive_an_event_written_during_the_test() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        model.subscribe("orders", event -> {
            received.add(event);
            return Mono.empty();
        });

        OccurrentSubscriptionsExtension extension = OccurrentSubscriptionsExtension.stoppedByDefault(model);
        runBeforeEach(extension);

        model.dispatch(List.of(event())).block();

        assertThat(received).isEmpty();
        model.shutdown();
    }

    @Test
    void after_start_the_subscription_receives_events_dispatched_after() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        model.subscribe("orders", event -> {
            received.add(event);
            return Mono.empty();
        });

        OccurrentSubscriptionsExtension extension = OccurrentSubscriptionsExtension.stoppedByDefault(model);
        runBeforeEach(extension);

        extension.start("orders");
        model.dispatch(List.of(event())).block();

        assertThat(received).hasSize(1);
        model.shutdown();
    }

    @Test
    void start_on_an_unknown_id_names_the_subscriptions_the_model_actually_has() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        model.subscribe("orders", event -> Mono.empty());
        model.subscribe("shipments", event -> Mono.empty());

        OccurrentSubscriptionsExtension extension = OccurrentSubscriptionsExtension.stoppedByDefault(model);
        runBeforeEach(extension);

        assertThatThrownBy(() -> extension.start("odrers"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("odrers")
                .hasMessageContaining("orders")
                .hasMessageContaining("shipments");

        model.shutdown();
    }

    @Test
    void start_all_starts_every_subscription_on_the_model() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        CopyOnWriteArrayList<CloudEvent> orders = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<CloudEvent> shipments = new CopyOnWriteArrayList<>();
        model.subscribe("orders", event -> {
            orders.add(event);
            return Mono.empty();
        });
        model.subscribe("shipments", event -> {
            shipments.add(event);
            return Mono.empty();
        });

        OccurrentSubscriptionsExtension extension = OccurrentSubscriptionsExtension.stoppedByDefault(model);
        runBeforeEach(extension);

        assertThat(extension.startAll()).containsExactlyInAnyOrder("orders", "shipments");

        model.dispatch(List.of(event())).block();

        assertThat(orders).hasSize(1);
        assertThat(shipments).hasSize(1);
        model.shutdown();
    }

    @Test
    void start_all_is_fine_when_a_subscription_is_already_running() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        model.subscribe("orders", event -> {
            received.add(event);
            return Mono.empty();
        });

        OccurrentSubscriptionsExtension extension = OccurrentSubscriptionsExtension.stoppedByDefault(model);
        runBeforeEach(extension);
        extension.start("orders");

        assertThat(extension.startAll()).isEmpty();

        model.dispatch(List.of(event())).block();

        assertThat(received).hasSize(1);
        model.shutdown();
    }

    @Test
    void start_all_fails_loudly_when_a_model_cannot_list_its_subscriptions() {
        OccurrentSubscriptionsExtension extension = OccurrentSubscriptionsExtension.stoppedByDefault(new NotIntrospectableSubscriptions());

        assertThatThrownBy(extension::startAll)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cannot list them")
                .hasMessageContaining("start(String)");
    }

    @Test
    void always_start_resumes_automatically_in_before_each() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        model.subscribe("orders", event -> {
            received.add(event);
            return Mono.empty();
        });

        OccurrentSubscriptionsExtension extension = OccurrentSubscriptionsExtension.stoppedByDefault(model).alwaysStart("orders");
        runBeforeEach(extension);

        model.dispatch(List.of(event())).block();

        assertThat(received).hasSize(1);
        model.shutdown();
    }

    @Test
    void after_each_stops_every_subscription_so_it_does_not_leak_into_the_next_test() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        model.subscribe("orders", event -> {
            received.add(event);
            return Mono.empty();
        });

        OccurrentSubscriptionsExtension extension = OccurrentSubscriptionsExtension.stoppedByDefault(model);
        runBeforeEach(extension);
        extension.start("orders");

        extension.afterEach(unusedExtensionContext());

        model.dispatch(List.of(event())).block();

        assertThat(received).isEmpty();
        model.shutdown();
    }

    @Test
    void two_models_are_both_stopped_and_started() {
        SynchronousSubscriptionModel first = new SynchronousSubscriptionModel();
        SynchronousSubscriptionModel second = new SynchronousSubscriptionModel();
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        second.subscribe("order-projection", event -> {
            received.add(event);
            return Mono.empty();
        });

        OccurrentSubscriptionsExtension extension = OccurrentSubscriptionsExtension.stoppedByDefault(first, second);
        runBeforeEach(extension);

        second.dispatch(List.of(event())).block();
        assertThat(received)
                .as("stoppedByDefault must stop every model it is given, not only the first")
                .isEmpty();

        extension.start("order-projection");
        second.dispatch(List.of(event())).block();
        assertThat(received).hasSize(1);

        first.shutdown();
        second.shutdown();
    }

    @Test
    void start_finds_the_id_on_whichever_model_owns_it() {
        SynchronousSubscriptionModel first = new SynchronousSubscriptionModel();
        SynchronousSubscriptionModel second = new SynchronousSubscriptionModel();
        CopyOnWriteArrayList<CloudEvent> ordersReceived = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<CloudEvent> projectionReceived = new CopyOnWriteArrayList<>();
        first.subscribe("orders", event -> {
            ordersReceived.add(event);
            return Mono.empty();
        });
        second.subscribe("order-projection", event -> {
            projectionReceived.add(event);
            return Mono.empty();
        });

        OccurrentSubscriptionsExtension extension = OccurrentSubscriptionsExtension.stoppedByDefault(first, second);
        runBeforeEach(extension);

        extension.start("order-projection");
        extension.start("orders");

        first.dispatch(List.of(event())).block();
        second.dispatch(List.of(event())).block();

        assertThat(ordersReceived).hasSize(1);
        assertThat(projectionReceived).hasSize(1);

        first.shutdown();
        second.shutdown();
    }

    @Test
    void start_all_unions_the_ids_across_every_model() {
        SynchronousSubscriptionModel first = new SynchronousSubscriptionModel();
        SynchronousSubscriptionModel second = new SynchronousSubscriptionModel();
        first.subscribe("orders", event -> Mono.empty());
        second.subscribe("order-projection", event -> Mono.empty());

        OccurrentSubscriptionsExtension extension = OccurrentSubscriptionsExtension.stoppedByDefault(first, second);
        runBeforeEach(extension);

        assertThat(extension.startAll()).containsExactlyInAnyOrder("orders", "order-projection");

        first.shutdown();
        second.shutdown();
    }

    @Test
    void clear_state_runs_after_every_subscription_is_stopped_and_before_any_is_resumed() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        model.subscribe("orders", event -> Mono.empty());
        CopyOnWriteArrayList<String> order = new CopyOnWriteArrayList<>();
        SubscriptionModelLifeCycle recordingModel = recording(model, order);

        OccurrentSubscriptionsExtension extension = OccurrentSubscriptionsExtension.stoppedByDefault(recordingModel)
                .clearingStateWith(() -> order.add("clearState"))
                .alwaysStart("orders");
        runBeforeEach(extension);

        assertThat(order)
                .as("a flush that ran before the stop would be undone by it, and one that ran after the resume would "
                        + "delete what the resumed subscription is about to read")
                .containsExactly("stop", "clearState", "resume:orders");

        model.shutdown();
    }

    @Test
    void checkpoints_are_deleted_for_every_id_the_model_reports() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        model.subscribe("orders", event -> Mono.empty());
        model.subscribe("invoices", event -> Mono.empty());
        CopyOnWriteArrayList<String> deleted = new CopyOnWriteArrayList<>();

        OccurrentSubscriptionsExtension extension = OccurrentSubscriptionsExtension.stoppedByDefault(model)
                .clearingCheckpoints(new RecordingCheckpointStorage(deleted));
        runBeforeEach(extension);

        assertThat(deleted).containsExactlyInAnyOrder("orders", "invoices");
        model.shutdown();
    }

    @Test
    void checkpoints_are_deleted_before_a_subscription_is_resumed() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        model.subscribe("orders", event -> Mono.empty());
        CopyOnWriteArrayList<String> order = new CopyOnWriteArrayList<>();
        SubscriptionModelLifeCycle recordingModel = recording(model, order);

        OccurrentSubscriptionsExtension extension = OccurrentSubscriptionsExtension.stoppedByDefault(recordingModel)
                .clearingCheckpoints(new RecordingCheckpointStorage(order, "delete:"))
                .alwaysStart("orders");
        runBeforeEach(extension);

        assertThat(order)
                .as("a checkpoint deleted after the resume would be the one the subscription just stored")
                .containsExactly("stop", "delete:orders", "resume:orders");

        model.shutdown();
    }

    @Test
    void clearing_checkpoints_is_a_no_op_when_there_are_no_ids_to_clear_them_for() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        CopyOnWriteArrayList<String> deleted = new CopyOnWriteArrayList<>();
        OccurrentSubscriptionsExtension extension = OccurrentSubscriptionsExtension.stoppedByDefault(model)
                .clearingCheckpoints(new RecordingCheckpointStorage(deleted));

        // Deleting nothing is the correct outcome for an empty set of ids, not a reason to fail every test in the class.
        runBeforeEach(extension);

        assertThat(deleted).isEmpty();

        model.shutdown();
    }

    @Test
    void clearing_checkpoints_for_names_ids_without_starting_them() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        CopyOnWriteArrayList<String> deleted = new CopyOnWriteArrayList<>();
        OccurrentSubscriptionsExtension extension = OccurrentSubscriptionsExtension.stoppedByDefault(model)
                .clearingCheckpointsFor(new RecordingCheckpointStorage(deleted), "orders", "shipments");
        runBeforeEach(extension);

        assertThat(deleted).containsExactlyInAnyOrder("orders", "shipments");
        assertThat(model.isPaused("orders")).isFalse();
        assertThat(model.isRunning("orders")).isFalse();

        model.shutdown();
    }

    @Test
    void clearing_checkpoints_for_rejects_an_empty_id_list() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        OccurrentSubscriptionsExtension extension = OccurrentSubscriptionsExtension.stoppedByDefault(model);

        assertThatThrownBy(() -> extension.clearingCheckpointsFor(new RecordingCheckpointStorage(new CopyOnWriteArrayList<>())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be empty");

        model.shutdown();
    }

    @Test
    void a_subscription_that_does_not_start_within_the_timeout_fails_the_test_instead_of_hanging() {
        SubscriptionModelLifeCycle neverStarts = neverStartingModel();

        OccurrentSubscriptionsExtension extension = OccurrentSubscriptionsExtension.stoppedByDefault(neverStarts)
                .withStartTimeout(Duration.ofMillis(50));

        assertThatThrownBy(() -> extension.start("orders"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("orders")
                .hasMessageContaining("withStartTimeout");
    }

    @Test
    void with_start_timeout_rejects_a_non_positive_duration() {
        SynchronousSubscriptionModel model = new SynchronousSubscriptionModel();
        OccurrentSubscriptionsExtension extension = OccurrentSubscriptionsExtension.stoppedByDefault(model);

        assertThatThrownBy(() -> extension.withStartTimeout(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive");

        model.shutdown();
    }

    // A subscription whose Subscription never reports started, so resumeAndWait's bounded wait is what has to save
    // the test from hanging rather than the id lookup or the model's own resumeSubscription.
    private static SubscriptionModelLifeCycle neverStartingModel() {
        Subscription neverStartingSubscription = new Subscription() {
            @Override
            public String id() {
                return "orders";
            }

            @Override
            public Mono<Void> waitUntilStarted() {
                return Mono.never();
            }
        };
        return (SubscriptionModelLifeCycle) Proxy.newProxyInstance(
                OccurrentSubscriptionsExtensionTest.class.getClassLoader(),
                new Class<?>[]{SubscriptionModelLifeCycle.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "resumeSubscription" -> neverStartingSubscription;
                    case "isPaused" -> true;
                    case "isRunning" -> false;
                    case "stop", "start", "pauseSubscription", "cancelSubscription" -> null;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    // A lifecycle with no IntrospectableSubscriptions anywhere in it, so startAll has nothing to enumerate.
    private static final class NotIntrospectableSubscriptions implements SubscriptionModelLifeCycle {
        @Override
        public void stop() {
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        @Override
        public boolean isRunning() {
            return false;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return false;
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            throw new AssertionError("resumeSubscription must not be reached when the model cannot be enumerated");
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
        }
    }

    // Records the life-cycle calls in order while delegating to the real model, so the assertions above are about
    // ordering rather than about what a stub happens to allow.
    private static SubscriptionModelLifeCycle recording(SubscriptionModelLifeCycle delegate, List<String> order) {
        return (SubscriptionModelLifeCycle) Proxy.newProxyInstance(
                OccurrentSubscriptionsExtensionTest.class.getClassLoader(),
                new Class<?>[]{SubscriptionModelLifeCycle.class},
                (proxy, method, args) -> {
                    if ("stop".equals(method.getName()) && args == null) {
                        order.add("stop");
                    } else if ("resumeSubscription".equals(method.getName())) {
                        order.add("resume:" + args[0]);
                    }
                    return method.invoke(delegate, args);
                });
    }

    // CheckpointStorage has four methods, so this cannot be a lambda. Only delete is exercised here, and the rest
    // throw rather than returning a default, so a future change that starts calling them says so.
    private record RecordingCheckpointStorage(List<String> deleted, String prefix) implements CheckpointStorage {

        RecordingCheckpointStorage(List<String> deleted) {
            this(deleted, "");
        }

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            throw new UnsupportedOperationException("read");
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            throw new UnsupportedOperationException("save");
        }

        @Override
        public Mono<Long> writeVersion(String subscriptionId) {
            throw new UnsupportedOperationException("writeVersion");
        }

        @Override
        public Mono<Void> delete(String subscriptionId) {
            deleted.add(prefix + subscriptionId);
            return Mono.empty();
        }
    }

    private static void runBeforeEach(OccurrentSubscriptionsExtension extension) {
        extension.beforeEach(unusedExtensionContext());
    }

    // beforeEach/afterEach do not read the ExtensionContext today, but a null argument would silently hide it if
    // they started to. A proxy that throws on any access fails the test loudly instead.
    private static ExtensionContext unusedExtensionContext() {
        return (ExtensionContext) Proxy.newProxyInstance(
                OccurrentSubscriptionsExtensionTest.class.getClassLoader(),
                new Class<?>[]{ExtensionContext.class},
                (proxy, method, args) -> {
                    throw new UnsupportedOperationException("Did not expect ExtensionContext#" + method.getName() + " to be called in this test");
                });
    }

    private static CloudEvent event() {
        return new CloudEventBuilder()
                .withId(UUID.randomUUID().toString())
                .withSubject("subject")
                .withType("type1")
                .withSource(URI.create("urn:source"))
                .withTime(OffsetDateTime.now())
                .withData("test".getBytes(UTF_8))
                .build();
    }
}
