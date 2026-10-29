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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * An order saga whose instance starts on {@code OrderPlaced}, with {@code ItemAdded} as an event that cannot start it.
 * The state is {@code null} until the start, so applying an item before it fails. Every event issues a command, which
 * makes an event that is skipped a missing command and an event applied twice a command issued twice.
 */
final class HeldOrderSaga {

    static final String ORDER = "order-1";
    static final String STREAM = "order-stream";
    static final String ITEM_ADDED_TYPE = ItemAdded.class.getName();

    sealed interface OrderEvent permits OrderPlaced, ItemAdded {
        String eventId();

        String orderId();
    }

    record OrderPlaced(String eventId, String orderId) implements OrderEvent {
    }

    record ItemAdded(String eventId, String orderId, String item) implements OrderEvent {
    }

    sealed interface OrderCommand permits OpenLedger, Reserve {
    }

    record OpenLedger(String orderId) implements OrderCommand {
    }

    record Reserve(String orderId, String item) implements OrderCommand {
    }

    record Order(List<String> items) {
        Order withItem(String item) {
            List<String> added = new ArrayList<>(items);
            added.add(item);
            return new Order(added);
        }
    }

    private static final CloudEventConverter<OrderEvent> CONVERTER =
            new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(OrderEvent::eventId).build();

    private HeldOrderSaga() {
    }

    static Saga<OrderEvent, Order, OrderCommand> saga() {
        return Saga.<OrderEvent, Order, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> new Order(List.of()))
                .react(OrderPlaced.class, (state, e) -> List.of(SagaEffect.issue(new OpenLedger(e.orderId()))))
                .evolve(ItemAdded.class, (state, e) -> state.withItem(e.item()))
                .react(ItemAdded.class, (state, e) -> List.of(SagaEffect.issue(new Reserve(e.orderId(), e.item()))))
                .build();
    }

    static CloudEventConverter<OrderEvent> converter() {
        return CONVERTER;
    }

    static CloudEvent placedAt(long streamVersion) {
        return streamKeyed(streamVersion, new OrderPlaced("placed-" + streamVersion, ORDER));
    }

    static CloudEvent itemAt(long streamVersion, String item) {
        return streamKeyed(streamVersion, new ItemAdded("item-" + item, ORDER, item));
    }

    static Reserve reserve(String item) {
        return new Reserve(ORDER, item);
    }

    // The stream key an event store writes, with the stream id and the version chosen by the test.
    private static CloudEvent streamKeyed(long streamVersion, OrderEvent event) {
        return CloudEventBuilder.v1(CONVERTER.toCloudEvent(event)).withExtension(OccurrentCloudEventExtension.occurrent(STREAM, streamVersion)).build();
    }

    /**
     * Records the commands it is given. A reservation of an item that is set to fail throws {@link #failure} before it
     * is recorded, and a hook set for an item runs once, just before that item's reservation is handled.
     */
    static final class RecordingDispatcher implements CommandDispatcher<OrderCommand> {
        final IllegalStateException failure = new IllegalStateException("cannot reserve");
        private final List<OrderCommand> issued = new CopyOnWriteArrayList<>();
        private final Set<String> failingItems = ConcurrentHashMap.newKeySet();
        private volatile @Nullable String hookItem;
        private volatile @Nullable Runnable hook;

        @Override
        public void dispatch(OrderCommand command) {
            if (command instanceof Reserve reserve) {
                Runnable before = hook;
                if (before != null && reserve.item().equals(hookItem)) {
                    hook = null;
                    before.run();
                }
                if (failingItems.contains(reserve.item())) {
                    throw failure;
                }
            }
            issued.add(command);
        }

        List<OrderCommand> issued() {
            return issued;
        }

        void failReservationOf(String item) {
            failingItems.add(item);
        }

        void stopFailingReservationOf(String item) {
            failingItems.remove(item);
        }

        void beforeReservationOf(String item, Runnable action) {
            hookItem = item;
            hook = action;
        }
    }
}
