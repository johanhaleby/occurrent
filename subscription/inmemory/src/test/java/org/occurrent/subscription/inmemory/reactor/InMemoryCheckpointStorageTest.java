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

package org.occurrent.subscription.inmemory.reactor;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StringBasedCheckpoint;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("in-memory reactive checkpoint storage")
class InMemoryCheckpointStorageTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(2);

    private final InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();

    @Nested
    @DisplayName("round trip")
    class RoundTrip {

        @Test
        void a_saved_checkpoint_is_read_back() {
            Checkpoint checkpoint = new StringBasedCheckpoint("42");

            storage.save("subscription", checkpoint).block(TIMEOUT);

            assertThat(storage.read("subscription").block(TIMEOUT)).isEqualTo(checkpoint);
        }

        @Test
        void reading_an_unknown_subscription_completes_empty() {
            assertThat(storage.read("nobody").blockOptional(TIMEOUT)).isEmpty();
        }

        @Test
        void save_emits_the_checkpoint_for_chaining() {
            Checkpoint checkpoint = new StringBasedCheckpoint("42");

            assertThat(storage.save("subscription", checkpoint).block(TIMEOUT)).isEqualTo(checkpoint);
        }

        @Test
        void delete_removes_the_checkpoint() {
            storage.save("subscription", new StringBasedCheckpoint("42")).block(TIMEOUT);

            storage.delete("subscription").block(TIMEOUT);

            assertThat(storage.read("subscription").blockOptional(TIMEOUT)).isEmpty();
        }
    }

    @Nested
    @DisplayName("the publishers are cold")
    class ThePublishersAreCold {

        @Test
        void an_unsubscribed_save_stores_nothing() {
            Mono<Checkpoint> assembledButNeverSubscribed = storage.save("subscription", new StringBasedCheckpoint("42"));

            assertThat(assembledButNeverSubscribed).isNotNull();
            assertThat(storage.read("subscription").blockOptional(TIMEOUT)).isEmpty();
        }

        @Test
        void an_unsubscribed_delete_removes_nothing() {
            storage.save("subscription", new StringBasedCheckpoint("42")).block(TIMEOUT);

            Mono<Void> assembledButNeverSubscribed = storage.delete("subscription");

            assertThat(assembledButNeverSubscribed).isNotNull();
            assertThat(storage.read("subscription").block(TIMEOUT)).isEqualTo(new StringBasedCheckpoint("42"));
        }

        @Test
        void read_reflects_the_state_at_subscription_not_at_assembly() {
            Mono<Checkpoint> assembledBeforeTheSave = storage.read("subscription");

            storage.save("subscription", new StringBasedCheckpoint("42")).block(TIMEOUT);

            assertThat(assembledBeforeTheSave.block(TIMEOUT)).isEqualTo(new StringBasedCheckpoint("42"));
        }
    }

    @Nested
    @DisplayName("arguments are validated eagerly")
    class ArgumentsAreValidatedEagerly {

        @Test
        void a_null_subscription_id_fails_the_caller_not_a_subscriber() {
            assertThatThrownBy(() -> storage.read(null)).isInstanceOf(NullPointerException.class).hasMessageContaining("subscriptionId");
            assertThatThrownBy(() -> storage.save(null, new StringBasedCheckpoint("42"))).isInstanceOf(NullPointerException.class).hasMessageContaining("subscriptionId");
            assertThatThrownBy(() -> storage.delete(null)).isInstanceOf(NullPointerException.class).hasMessageContaining("subscriptionId");
        }

        @Test
        void a_null_checkpoint_fails_the_caller_not_a_subscriber() {
            assertThatThrownBy(() -> storage.save("subscription", null)).isInstanceOf(NullPointerException.class).hasMessageContaining("Checkpoint");
        }
    }
}
