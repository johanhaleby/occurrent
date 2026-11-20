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
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.flow.FlowState;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A FlowState that is not the flow executor's cannot be read back field by field, so the runner refuses a saga that starts from one when it is built, before any delivery.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaForeignFlowStateTest {

    private static final String ORDER = "order-1";

    sealed interface OrderEvent permits OrderPlaced {
        String eventId();

        String orderId();
    }

    record OrderPlaced(String eventId, String orderId) implements OrderEvent {
    }

    sealed interface OrderCommand permits ShipOrder {
    }

    record ShipOrder(String orderId) implements OrderCommand {
    }

    record HandRolledFlowState(String currentStep) implements FlowState<OrderEvent> {
        @Override
        public List<OrderEvent> received() {
            return List.of();
        }

        @Override
        public boolean completed() {
            return false;
        }
    }

    private final CloudEventConverter<OrderEvent> converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test"))
            .idMapper(OrderEvent::eventId).build();
    private final SagaStateStore<FlowState<OrderEvent>> store = SagaStateStore.inMemory();
    private final List<OrderCommand> dispatched = new ArrayList<>();

    @Test
    void a_saga_whose_initial_state_is_a_hand_rolled_FlowState_is_refused_before_any_delivery_and_dispatches_nothing() {
        Saga<OrderEvent, FlowState<OrderEvent>, OrderCommand> saga = Saga.<OrderEvent, FlowState<OrderEvent>, OrderCommand>builder(new HandRolledFlowState("start"))
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> new HandRolledFlowState("placed"))
                .react(OrderPlaced.class, (state, e) -> List.of(SagaEffect.issue(new ShipOrder(e.orderId()))))
                .build();

        Throwable thrown = catchThrowable(() -> {
            SagaExecution<OrderEvent, FlowState<OrderEvent>, OrderCommand> execution = new SagaExecution<>("orders", saga, store, dispatched::add, converter,
                    SagaRunnerConfig.defaults());
            execution.onCloudEvent(placed());
        });

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("FlowStateImpl");
        assertThat(store.find(ORDER)).isEmpty();
        assertThat(dispatched).isEmpty();
    }

    private CloudEvent placed() {
        return OccurrentCloudEventExtension.withPosition(
                CloudEventBuilder.v1(converter.toCloudEvent(new OrderPlaced("placed", ORDER))).withExtension(OccurrentCloudEventExtension.occurrent(ORDER, 1)).build(), 1);
    }
}
