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

package org.occurrent.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.application.service.blocking.generic.GenericApplicationService;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.dsl.decider.DeciderApplicationService;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.net.URI;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("CommandDispatchers")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class CommandDispatchersTest {

    private InMemoryEventStore eventStore;
    private CloudEventConverter<OrderEvent> cloudEventConverter;
    private DeciderApplicationService<OrderEvent> deciderApplicationService;
    private Decider<OrderCommand, Void, OrderEvent> shipmentDecider;

    @BeforeEach
    void create_instances() {
        eventStore = new InMemoryEventStore();
        cloudEventConverter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test")).build();
        ApplicationService<OrderEvent> applicationService = new GenericApplicationService<>(eventStore, cloudEventConverter);
        deciderApplicationService = new DeciderApplicationService<>(applicationService);
        shipmentDecider = Decider.create(
                null,
                (OrderCommand command, Void state) -> List.of(new OrderShipped(command.orderId())),
                (state, event) -> state
        );
    }

    @Test
    void dispatch_routes_the_command_to_the_stream_id_resolver_derives_and_writes_the_deciders_events() {
        // Given
        CommandDispatcher<OrderCommand> dispatcher = CommandDispatchers.decider(deciderApplicationService, shipmentDecider, OrderCommand::orderId);

        // When
        dispatcher.dispatch(new ShipOrder("order-1"));

        // Then
        List<OrderEvent> events = readEvents("order-1");
        assertThat(events).containsExactly(new OrderShipped("order-1"));
    }

    @Test
    void dispatching_the_same_command_twice_is_idempotent_because_the_decider_re_folds_the_authoritative_stream() {
        // Given
        Decider<OrderCommand, Boolean, OrderEvent> idempotentDecider = Decider.create(
                Boolean.FALSE,
                (OrderCommand command, Boolean alreadyShipped) -> alreadyShipped ? List.of() : List.of(new OrderShipped(command.orderId())),
                (state, event) -> Boolean.TRUE
        );
        CommandDispatcher<OrderCommand> dispatcher = CommandDispatchers.decider(deciderApplicationService, idempotentDecider, OrderCommand::orderId);

        // When
        ShipOrder command = new ShipOrder("order-2");
        dispatcher.dispatch(command);
        dispatcher.dispatch(command);

        // Then
        assertThat(readEvents("order-2")).containsExactly(new OrderShipped("order-2"));
    }

    private List<OrderEvent> readEvents(String streamId) {
        return eventStore.read(streamId).eventList().stream().map(cloudEventConverter::toDomainEvent).toList();
    }

    private sealed interface OrderCommand {
        String orderId();
    }

    private record ShipOrder(String orderId) implements OrderCommand {
    }

    private sealed interface OrderEvent {
    }

    private record OrderShipped(String orderId) implements OrderEvent {
    }
}
