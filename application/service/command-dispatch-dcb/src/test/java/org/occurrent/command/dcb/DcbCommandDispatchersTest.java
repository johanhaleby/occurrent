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

package org.occurrent.command.dcb;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.service.blocking.dcb.DcbApplicationService;
import org.occurrent.application.service.blocking.dcb.GenericDcbApplicationService;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.dsl.dcb.DcbDecider;
import org.occurrent.dsl.dcb.blocking.DcbDeciderApplicationService;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.net.URI;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("DcbCommandDispatchers")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class DcbCommandDispatchersTest {

    private InMemoryEventStore eventStore;
    private CloudEventConverter<OrderEvent> cloudEventConverter;
    private DcbDeciderApplicationService<OrderEvent> deciderApplicationService;

    @BeforeEach
    void create_instances() {
        eventStore = new InMemoryEventStore();
        cloudEventConverter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test")).build();
        DcbApplicationService<OrderEvent> applicationService = new GenericDcbApplicationService<>(
                eventStore,
                cloudEventConverter,
                event -> Set.of(tagFor(event)),
                GenericDcbApplicationService.defaultRetryStrategy()
        );
        deciderApplicationService = new DcbDeciderApplicationService<>(applicationService);
    }

    @Test
    void dispatch_executes_the_command_against_the_boundary_derived_from_the_deciders_criteria_and_tags() {
        // Given
        Decider<OrderCommand, Void, OrderEvent> shipmentDecider = Decider.create(
                null,
                (OrderCommand command, Void state) -> List.of(new OrderShipped(command.orderId())),
                (state, event) -> state
        );
        DcbDecider<OrderCommand, Void, OrderEvent> dcbDecider = DcbDecider.from(shipmentDecider, command -> orderQuery(command.orderId()), event -> Set.of(tagFor(event)));
        CommandDispatcher<OrderCommand> dispatcher = DcbCommandDispatchers.decider(deciderApplicationService, dcbDecider);

        // When
        dispatcher.dispatch(new ShipOrder("order-1"));

        // Then
        assertThat(readEvents("order-1")).containsExactly(new OrderShipped("order-1"));
    }

    @Test
    void dispatching_the_same_command_twice_is_idempotent_because_the_decider_re_reads_the_authoritative_boundary() {
        // Given
        Decider<OrderCommand, Boolean, OrderEvent> idempotentDecider = Decider.create(
                Boolean.FALSE,
                (OrderCommand command, Boolean alreadyShipped) -> alreadyShipped ? List.of() : List.of(new OrderShipped(command.orderId())),
                (state, event) -> Boolean.TRUE
        );
        DcbDecider<OrderCommand, Boolean, OrderEvent> dcbDecider = DcbDecider.from(idempotentDecider, command -> orderQuery(command.orderId()), event -> Set.of(tagFor(event)));
        CommandDispatcher<OrderCommand> dispatcher = DcbCommandDispatchers.decider(deciderApplicationService, dcbDecider);

        // When
        ShipOrder command = new ShipOrder("order-2");
        dispatcher.dispatch(command);
        dispatcher.dispatch(command);

        // Then
        assertThat(readEvents("order-2")).containsExactly(new OrderShipped("order-2"));
    }

    private List<OrderEvent> readEvents(String orderId) {
        return cloudEventConverter.toDomainEvents(eventStore.read(orderQuery(orderId)).stream()).toList();
    }

    private static DcbCriteria orderQuery(String orderId) {
        return DcbCriteria.tags(Tag.of("order", orderId));
    }

    private static Tag tagFor(OrderEvent event) {
        return Tag.of("order", event.orderId());
    }

    private sealed interface OrderCommand {
        String orderId();
    }

    private record ShipOrder(String orderId) implements OrderCommand {
    }

    private sealed interface OrderEvent {
        String orderId();
    }

    private record OrderShipped(String orderId) implements OrderEvent {
    }
}
