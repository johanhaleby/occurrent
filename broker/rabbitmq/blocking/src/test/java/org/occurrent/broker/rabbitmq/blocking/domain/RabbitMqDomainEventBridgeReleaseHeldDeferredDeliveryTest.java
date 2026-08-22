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

package org.occurrent.broker.rabbitmq.blocking.domain;

import org.junit.jupiter.api.Test;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqBridgeException;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * {@link RabbitMqDomainEventBridge#releaseHeldDeferredDelivery(Deque, java.util.function.LongConsumer)} in
 * isolation, with no real {@code Channel} or broker behind it. A failed release (a nack that throws, an
 * {@code IOException} surfacing as {@link RabbitMqBridgeException}) must never drop the tag it was for, and must
 * not let a later tag in the same pass jump ahead of the one that just failed.
 */
class RabbitMqDomainEventBridgeReleaseHeldDeferredDeliveryTest {

    @Test
    void a_failed_release_puts_the_tag_back_at_the_front_and_stops_the_rest_of_the_pass() {
        Deque<Long> held = new ArrayDeque<>(List.of(1L, 2L));
        AtomicInteger attempts = new AtomicInteger();
        List<Long> released = new CopyOnWriteArrayList<>();

        Throwable thrown = catchThrowable(() -> RabbitMqDomainEventBridge.releaseHeldDeferredDelivery(held, tag -> {
            attempts.incrementAndGet();
            if (tag == 1L) {
                throw new RabbitMqBridgeException("Failed to negatively acknowledge delivery tag " + tag,
                        new IOException("channel hiccup"));
            }
            released.add(tag);
        }));

        assertThat(thrown).isInstanceOf(RabbitMqBridgeException.class).hasMessageContaining("delivery tag 1");
        assertThat(attempts.get()).isEqualTo(1);
        assertThat(released).isEmpty();
        assertThat(held).containsExactly(1L, 2L);
    }

    @Test
    void a_tag_that_failed_once_is_released_on_the_next_pass() {
        Deque<Long> held = new ArrayDeque<>(List.of(1L, 2L));
        AtomicInteger attempts = new AtomicInteger();
        List<Long> released = new CopyOnWriteArrayList<>();

        Throwable thrown = catchThrowable(() -> RabbitMqDomainEventBridge.releaseHeldDeferredDelivery(held, tag -> {
            if (attempts.getAndIncrement() == 0) {
                throw new RabbitMqBridgeException("Failed to negatively acknowledge delivery tag " + tag,
                        new IOException("channel hiccup"));
            }
            released.add(tag);
        }));
        assertThat(thrown).isInstanceOf(RabbitMqBridgeException.class);
        assertThat(held).containsExactly(1L, 2L);

        RabbitMqDomainEventBridge.releaseHeldDeferredDelivery(held, tag -> {
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

        RabbitMqDomainEventBridge.releaseHeldDeferredDelivery(held, tag -> {
            released.add(tag);
            held.addLast(2L);
        });

        assertThat(released).containsExactly(1L);
        assertThat(held).containsExactly(2L);
    }
}
