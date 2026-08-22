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

package org.occurrent.subscription.api.reactor;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.subscription.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbSubscriptionModelAdapterTest {

    @Test
    void delivers_only_dcb_events_matching_the_query_and_passes_the_start_position_through() {
        CloudEvent matching = dcbEvent("NameDefined", 1, "name:1");
        // A stream-written event: it carries a position (stream position is on by default) but no DCB tags, so it is
        // not a DCB event and must be dropped by the isDcbEvent floor even though DcbCriteria tag matching alone might
        // otherwise let it through.
        CloudEvent streamEvent = OccurrentCloudEventExtension.withPosition(event("NameDefined"), 3);
        CloudEvent otherBoundary = dcbEvent("OrderPlaced", 2, "order:1");

        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel(Flux.just(matching, streamEvent, otherBoundary));
        DcbSubscriptionModel adapter = DcbSubscriptionModel.from(delegate);

        StepVerifier.create(adapter.subscribe(DcbCriteria.tags(Tag.parse("name:1")), DcbStartAt.afterPosition(5)))
                .expectNext(matching)
                .verifyComplete();

        // The in-process floor drops the stream event (no DCB tags) and the DCB event whose tags do not match the
        // query, so the subscription stays scoped to its own query even if a backend ignores the server-side filter.
        assertThat(delegate.capturedFilter).isInstanceOf(DcbSubscriptionFilter.class);
        // The DcbStartAt is converted to a generic StartAt and passed straight to the delegate.
        assertThat(delegate.capturedStartAt).isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class,
                start -> assertThat(start.checkpoint).isEqualTo(GlobalCheckpoint.of(5)));
    }

    @Test
    void named_subscribe_scopes_delivery_to_the_query_and_cancel_delegates_to_the_lifecycle() {
        CloudEvent matching = dcbEvent("NameDefined", 1, "name:1");
        CloudEvent otherBoundary = dcbEvent("OrderPlaced", 2, "order:1");

        RecordingSubscribableSubscriptionModel delegate = new RecordingSubscribableSubscriptionModel();
        DcbSubscriptionModel adapter = DcbSubscriptionModel.from(delegate);

        List<CloudEvent> delivered = new ArrayList<>();
        Function<CloudEvent, Mono<Void>> action = cloudEvent -> {
            delivered.add(cloudEvent);
            return Mono.empty();
        };

        SubscriptionHandle subscription = adapter.subscribe("sub-1", DcbCriteria.tags(Tag.parse("name:1")), DcbStartAt.afterPosition(5), action);

        assertThat(subscription.id()).isEqualTo("sub-1");
        assertThat(delegate.capturedFilter).isInstanceOf(DcbSubscriptionFilter.class);
        assertThat(delegate.capturedStartAt).isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class,
                start -> assertThat(start.checkpoint).isEqualTo(GlobalCheckpoint.of(5)));

        // The in-process floor scopes delivery to the query, matching the boundary case for the plain Flux subscribe.
        delegate.capturedAction.apply(matching).block();
        delegate.capturedAction.apply(otherBoundary).block();
        assertThat(delivered).containsExactly(matching);

        adapter.cancelSubscription("sub-1");
        assertThat(delegate.cancelledSubscriptionIds).containsExactly("sub-1");
    }

    @Test
    void named_subscribe_throws_if_the_delegate_does_not_support_named_subscriptions() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel(Flux.empty());
        DcbSubscriptionModel adapter = DcbSubscriptionModel.from(delegate);

        assertThatThrownBy(() -> adapter.subscribe("sub-1", DcbCriteria.tags(Tag.parse("name:1")), DcbStartAt.afterPosition(5), cloudEvent -> Mono.empty()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(Subscribable.class.getSimpleName());
    }

    @Test
    void cancel_subscription_throws_if_the_delegate_does_not_support_lifecycle_management() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel(Flux.empty());
        DcbSubscriptionModel adapter = DcbSubscriptionModel.from(delegate);

        assertThatThrownBy(() -> adapter.cancelSubscription("sub-1"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(SubscriptionModelLifeCycle.class.getSimpleName());
    }

    @Test
    void cancel_subscription_throws_when_the_delegate_only_supports_the_narrower_cancellable_subscriptions_capability() {
        // A register-only model such as a push model now implements CancellableSubscriptions rather than the full
        // SubscriptionModelLifeCycle, so it must still fail this gate: named DCB cancellation needs start/stop/pause too.
        RecordingCancellableOnlySubscriptionModel delegate = new RecordingCancellableOnlySubscriptionModel();
        DcbSubscriptionModel adapter = DcbSubscriptionModel.from(delegate);

        assertThatThrownBy(() -> adapter.cancelSubscription("sub-1"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(SubscriptionModelLifeCycle.class.getSimpleName());
    }

    private static CloudEvent dcbEvent(String type, long position, String... tags) {
        return OccurrentCloudEventExtension.withPosition(DcbCloudEvents.withTags(event(type), java.util.Arrays.stream(tags).map(Tag::parse).toList()), position);
    }

    private static CloudEvent event(String type) {
        return CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(URI.create("urn:test"))
                .withType(type)
                .build();
    }

    private static final class RecordingSubscriptionModel implements FluxSubscriptionModel {
        private final Flux<CloudEvent> events;
        @Nullable
        private SubscriptionFilter capturedFilter;
        @Nullable
        private StartAt capturedStartAt;

        private RecordingSubscriptionModel(Flux<CloudEvent> events) {
            this.events = events;
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            this.capturedFilter = filter;
            this.capturedStartAt = startAt;
            return events;
        }
    }

    private static final class RecordingSubscribableSubscriptionModel implements FluxSubscriptionModel, Subscribable, SubscriptionModelLifeCycle {
        @Nullable
        private SubscriptionFilter capturedFilter;
        @Nullable
        private StartAt capturedStartAt;
        @Nullable
        private Function<CloudEvent, Mono<Void>> capturedAction;
        private final List<String> cancelledSubscriptionIds = new ArrayList<>();

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            throw new UnsupportedOperationException("Not used by this test");
        }

        @Override
        public SubscriptionHandle subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
            this.capturedFilter = filter;
            this.capturedStartAt = startAt;
            this.capturedAction = action;
            return new SubscriptionHandle() {
                @Override
                public String id() {
                    return subscriptionId;
                }

                @Override
                public Mono<Void> waitUntilStarted() {
                    return Mono.empty();
                }
            };
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
        public SubscriptionHandle resumeSubscription(String subscriptionId) {
            throw new UnsupportedOperationException("Not used by this test");
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            cancelledSubscriptionIds.add(subscriptionId);
        }
    }

    // A register-only model (like a push model): implements Subscribable and the narrower CancellableSubscriptions,
    // not the full SubscriptionModelLifeCycle, so it has nothing to start, stop, or pause.
    private static final class RecordingCancellableOnlySubscriptionModel implements FluxSubscriptionModel, Subscribable, CancellableSubscriptions {

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            throw new UnsupportedOperationException("Not used by this test");
        }

        @Override
        public SubscriptionHandle subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
            throw new UnsupportedOperationException("Not used by this test");
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            throw new UnsupportedOperationException("Not used by this test");
        }
    }
}
