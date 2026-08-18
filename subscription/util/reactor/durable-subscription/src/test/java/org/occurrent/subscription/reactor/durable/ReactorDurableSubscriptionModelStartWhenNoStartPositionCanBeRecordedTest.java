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

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code startWhenNoStartPositionCanBeRecorded} turns the refusal of a registration whose position source answers
 * nothing into a start from the caller's default, with nothing recorded. These tests hold the override to exactly
 * that: it starts what would have been refused, it records nothing then, and it changes nothing when the source can
 * answer.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorDurableSubscriptionModelStartWhenNoStartPositionCanBeRecordedTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(2);

    private static final String SUBSCRIPTION_ID = "someSubscription";

    @Test
    void the_override_starts_a_cold_registration_the_position_source_cannot_answer_for_from_the_callers_default() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("unused");
        delegate.globalCheckpoint = null;
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage, overrideOn());

        Subscription subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());
        subscription.waitUntilStarted().block(TIMEOUT);

        assertThat(delegate.startedAt).hasSize(1);
        assertThat(delegate.startedAt.getFirst().isDefault()).isTrue();
        assertThat(storage.read(SUBSCRIPTION_ID).blockOptional(TIMEOUT)).isEmpty();
    }

    @Test
    void the_override_hands_a_named_registration_the_callers_default_when_the_position_source_cannot_answer() {
        NamedRecordingSubscriptionModel delegate = new NamedRecordingSubscriptionModel("unused");
        delegate.feed.globalCheckpoint = null;
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage, overrideOn());

        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        assertThat(delegate.startedAt).hasSize(1);
        assertThat(delegate.startedAt.getFirst().isDefault()).isTrue();
        assertThat(storage.read(SUBSCRIPTION_ID).blockOptional(TIMEOUT)).isEmpty();
    }

    @Test
    void the_override_starts_a_registration_made_while_stopped_from_where_the_feed_is_when_it_starts() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("unused");
        delegate.globalCheckpoint = null;
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage, overrideOn());
        model.stop();

        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());
        model.start(true);

        assertThat(delegate.startedAt).hasSize(1);
        assertThat(delegate.startedAt.getFirst().isDefault()).isTrue();
        assertThat(storage.read(SUBSCRIPTION_ID).blockOptional(TIMEOUT)).isEmpty();
    }

    @Test
    void the_override_still_records_the_first_position_when_the_source_can_answer() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage, overrideOn());

        Subscription subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());
        subscription.waitUntilStarted().block(TIMEOUT);

        assertThat(storage.read(SUBSCRIPTION_ID).block(TIMEOUT).asString()).isEqualTo("at-registration");
        assertThat(delegate.startedAt).hasSize(1);
        assertThat(delegate.startedAt.getFirst()).isInstanceOf(StartAt.StartAtCheckpoint.class);
    }

    private static ReactorDurableSubscriptionModelConfig overrideOn() {
        return new ReactorDurableSubscriptionModelConfig(1).startWhenNoStartPositionCanBeRecorded(true);
    }
}
