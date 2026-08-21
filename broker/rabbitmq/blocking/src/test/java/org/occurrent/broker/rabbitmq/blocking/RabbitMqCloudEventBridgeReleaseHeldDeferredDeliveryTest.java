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

package org.occurrent.broker.rabbitmq.blocking;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * {@link RabbitMqCloudEventBridge#releaseHeldDeferredDelivery(Deque, java.util.function.LongConsumer)}, in
 * isolation from a real {@code Channel} or broker: a Copilot review finding on this PR's pacing fix. A failed
 * release (a nack that itself throws, an {@code IOException} surfacing as {@link RabbitMqBridgeException}) must
 * never drop the tag it was for, and must not let a later, unrelated tag in the same pass jump ahead of the one
 * that just failed.
 */
class RabbitMqCloudEventBridgeReleaseHeldDeferredDeliveryTest {

    @Test
    void a_failed_release_puts_the_tag_back_at_the_front_and_stops_the_rest_of_the_pass() {
        Deque<Long> held = new ArrayDeque<>(List.of(1L, 2L));
        AtomicInteger attempts = new AtomicInteger();
        List<Long> released = new CopyOnWriteArrayList<>();

        Throwable thrown = catchThrowable(() -> RabbitMqCloudEventBridge.releaseHeldDeferredDelivery(held, tag -> {
            attempts.incrementAndGet();
            if (tag == 1L) {
                throw new RabbitMqBridgeException("Failed to negatively acknowledge delivery tag " + tag,
                        new IOException("channel hiccup"));
            }
            released.add(tag);
        }));

        assertThat(thrown).isInstanceOf(RabbitMqBridgeException.class).hasMessageContaining("delivery tag 1");
        // Only the one attempt that failed: the pass stops there rather than moving on to tag 2, which would
        // reorder a later tag ahead of one still waiting to be retried.
        assertThat(attempts.get()).isEqualTo(1);
        assertThat(released).isEmpty();
        // Tag 1 is back at the front, ahead of tag 2, which this pass never got to.
        assertThat(held).containsExactly(1L, 2L);
    }

    @Test
    void a_tag_that_failed_once_is_released_on_the_next_pass() {
        Deque<Long> held = new ArrayDeque<>(List.of(1L, 2L));
        AtomicInteger attempts = new AtomicInteger();
        List<Long> released = new CopyOnWriteArrayList<>();

        Throwable thrown = catchThrowable(() -> RabbitMqCloudEventBridge.releaseHeldDeferredDelivery(held, tag -> {
            if (attempts.getAndIncrement() == 0) {
                throw new RabbitMqBridgeException("Failed to negatively acknowledge delivery tag " + tag,
                        new IOException("channel hiccup"));
            }
            released.add(tag);
        }));
        assertThat(thrown).isInstanceOf(RabbitMqBridgeException.class);
        assertThat(held).containsExactly(1L, 2L);

        // The next poll's own call to this same method, against the channel that survived the earlier failure.
        RabbitMqCloudEventBridge.releaseHeldDeferredDelivery(held, tag -> {
            attempts.incrementAndGet();
            released.add(tag);
        });

        assertThat(released).containsExactly(1L, 2L);
        assertThat(held).isEmpty();
    }

    @Test
    void a_tag_added_while_the_pass_is_running_is_not_released_until_the_next_pass() {
        Deque<Long> held = new ArrayDeque<>(List.of(1L));
        List<Long> released = new CopyOnWriteArrayList<>();

        RabbitMqCloudEventBridge.releaseHeldDeferredDelivery(held, tag -> {
            released.add(tag);
            // A redelivery this release itself triggers, landing back on the same consumer and reporting
            // DEFERRED again before this pass has finished: the exact race the snapshot-count fix exists for.
            held.addLast(2L);
        });

        assertThat(released).containsExactly(1L);
        // The snapshot at entry was 1 tag, so the loop already ran its one iteration and stopped, leaving the
        // tag appended mid-pass for the next poll to pick up instead of nacking it in the same pass.
        assertThat(held).containsExactly(2L);
    }
}
