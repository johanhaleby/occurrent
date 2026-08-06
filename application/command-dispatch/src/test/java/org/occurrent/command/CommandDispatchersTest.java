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
import org.junit.jupiter.api.*;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.application.service.blocking.generic.GenericApplicationService;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.dsl.decider.DeciderApplicationService;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.net.URI;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

@DisplayName("CommandDispatchers")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class CommandDispatchersTest {

    private InMemoryEventStore eventStore;
    private CloudEventConverter<OrderEvent> cloudEventConverter;
    private ApplicationService<OrderEvent> applicationService;
    private DeciderApplicationService<OrderEvent> deciderApplicationService;
    private Decider<OrderCommand, Void, OrderEvent> shipmentDecider;

    @BeforeEach
    void create_instances() {
        eventStore = new InMemoryEventStore();
        cloudEventConverter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test")).build();
        applicationService = new GenericApplicationService<>(eventStore, cloudEventConverter);
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

    @Nested
    @DisplayName("decider dispatchAll")
    class DeciderBatching {

        /** Emits nothing for an order already shipped, so a second command in the same run can be seen to have folded. */
        private final Decider<OrderCommand, Boolean, OrderEvent> shipOnceDecider = Decider.create(
                Boolean.FALSE,
                (OrderCommand command, Boolean alreadyShipped) -> {
                    if (command instanceof ExplodeOrder) {
                        throw new IllegalStateException("boom");
                    }
                    return alreadyShipped ? List.of() : List.of(new OrderShipped(command.orderId()));
                },
                (state, event) -> Boolean.TRUE
        );

        private CommandDispatcher<OrderCommand> dispatcher() {
            return CommandDispatchers.decider(deciderApplicationService, shipOnceDecider, OrderCommand::orderId);
        }

        @Test
        void a_run_against_one_stream_becomes_a_single_atomic_append() {
            // Given a run of two commands to one stream whose second one the decider rejects
            List<OrderCommand> commands = List.of(new ShipOrder("order-10"), new ExplodeOrder("order-10"));

            // When
            assertThatThrownBy(() -> dispatcher().dispatchAll(commands)).isInstanceOf(IllegalStateException.class);

            // Then nothing at all was appended, so the two ran as one execute rather than one append each
            assertThat(readEvents("order-10")).isEmpty();
        }

        @Test
        void a_run_is_folded_so_each_command_decides_against_what_the_previous_one_decided() {
            // Given two ship commands for one order, where the decider ships an order at most once

            // When
            dispatcher().dispatchAll(List.of(new ShipOrder("order-11"), new ShipOrder("order-11")));

            // Then the second command saw the first one's event and decided nothing
            assertThat(readEvents("order-11")).containsExactly(new OrderShipped("order-11"));
        }

        @Test
        void commands_are_never_reordered_to_make_a_run_longer() {
            // Given order-12, order-13, then order-12 again, where the last one is rejected
            List<OrderCommand> commands = List.of(
                    new ShipOrder("order-12"), new ShipOrder("order-13"), new ExplodeOrder("order-12"));

            // When
            assertThatThrownBy(() -> dispatcher().dispatchAll(commands)).isInstanceOf(IllegalStateException.class);

            // Then the first two were appended, so the trailing order-12 was not folded back into the leading one
            assertThat(readEvents("order-12")).containsExactly(new OrderShipped("order-12"));
            assertThat(readEvents("order-13")).containsExactly(new OrderShipped("order-13"));
        }

        @Test
        void an_empty_batch_writes_nothing_which_is_what_a_reaction_arming_only_a_timer_produces() {
            assertThatCode(() -> dispatcher().dispatchAll(List.of())).doesNotThrowAnyException();
        }

        @Test
        void a_stream_id_that_cannot_be_resolved_fails_the_batch_before_anything_is_appended() {
            // Given a resolver that rejects the second command, as an annotation-driven one does for a missing id
            StreamIdResolver<OrderCommand> resolver = command -> {
                if (command instanceof ExplodeOrder) {
                    throw new IllegalArgumentException("no stream id");
                }
                return command.orderId();
            };
            CommandDispatcher<OrderCommand> dispatcher = CommandDispatchers.decider(deciderApplicationService, shipOnceDecider, resolver);

            // When
            assertThatThrownBy(() -> dispatcher.dispatchAll(List.of(new ShipOrder("order-14"), new ExplodeOrder("order-14"))))
                    .isInstanceOf(IllegalArgumentException.class);

            // Then the first command was not appended either, because ids are resolved before any run is written
            assertThat(readEvents("order-14")).isEmpty();
        }
    }

    @Nested
    @DisplayName("invocation")
    class Invocations {

        @Test
        void dispatch_runs_the_decision_against_the_stream_it_names() {
            // Given
            CommandDispatcher<Invocation<OrderEvent>> dispatcher = CommandDispatchers.invocation(applicationService);

            // When
            dispatcher.dispatch(Invocation.to("order-3", events -> ship(events, "order-3")));

            // Then
            assertThat(readEvents("order-3")).containsExactly(new OrderShipped("order-3"));
        }

        @Test
        void the_decision_sees_the_events_already_on_the_stream() {
            // Given
            CommandDispatcher<Invocation<OrderEvent>> dispatcher = CommandDispatchers.invocation(applicationService);
            dispatcher.dispatch(Invocation.to("order-4", events -> List.of(new OrderPlaced("order-4"))));

            // When
            dispatcher.dispatch(Invocation.to("order-4", events -> ship(events, "order-4")));

            // Then
            assertThat(readEvents("order-4")).containsExactly(new OrderPlaced("order-4"), new OrderShipped("order-4"));
        }

        @Test
        void dispatching_the_same_invocation_twice_is_idempotent_because_the_decision_re_folds_the_stream() {
            // Given
            CommandDispatcher<Invocation<OrderEvent>> dispatcher = CommandDispatchers.invocation(applicationService);
            Invocation<OrderEvent> shipOnce = Invocation.to("order-5", events -> ship(events, "order-5"));

            // When
            dispatcher.dispatch(shipOnce);
            dispatcher.dispatch(shipOnce);

            // Then
            assertThat(readEvents("order-5")).containsExactly(new OrderShipped("order-5"));
        }

        @Test
        void dispatch_all_folds_consecutive_invocations_to_the_same_stream_into_one_atomic_append() {
            // Given a run of two invocations to one stream whose second decision fails
            CommandDispatcher<Invocation<OrderEvent>> dispatcher = CommandDispatchers.invocation(applicationService);
            List<Invocation<OrderEvent>> invocations = List.of(
                    Invocation.to("order-6", events -> List.of(new OrderPlaced("order-6"))),
                    Invocation.to("order-6", events -> {
                        throw new IllegalStateException("boom");
                    }));

            // When
            assertThatThrownBy(() -> dispatcher.dispatchAll(invocations)).isInstanceOf(IllegalStateException.class);

            // Then nothing at all was appended, so the two ran as a single execute rather than one append each
            assertThat(readEvents("order-6")).isEmpty();
        }

        @Test
        void dispatch_all_composes_a_run_so_each_decision_sees_what_the_previous_one_decided() {
            // Given
            CommandDispatcher<Invocation<OrderEvent>> dispatcher = CommandDispatchers.invocation(applicationService);

            // When
            dispatcher.dispatchAll(List.of(
                    Invocation.to("order-7", events -> List.of(new OrderPlaced("order-7"))),
                    Invocation.to("order-7", events -> ship(events, "order-7"))));

            // Then the second decision saw the OrderPlaced the first one decided, so it shipped
            assertThat(readEvents("order-7")).containsExactly(new OrderPlaced("order-7"), new OrderShipped("order-7"));
        }

        @Test
        void dispatch_all_never_reorders_to_make_a_group_larger() {
            // Given order-8, order-9, then order-8 again, where the last one fails
            CommandDispatcher<Invocation<OrderEvent>> dispatcher = CommandDispatchers.invocation(applicationService);
            List<Invocation<OrderEvent>> invocations = List.of(
                    Invocation.to("order-8", events -> List.of(new OrderPlaced("order-8"))),
                    Invocation.to("order-9", events -> List.of(new OrderPlaced("order-9"))),
                    Invocation.to("order-8", events -> {
                        throw new IllegalStateException("boom");
                    }));

            // When
            assertThatThrownBy(() -> dispatcher.dispatchAll(invocations)).isInstanceOf(IllegalStateException.class);

            // Then the first two were appended, so the trailing order-8 was not folded back into the leading one
            assertThat(readEvents("order-8")).containsExactly(new OrderPlaced("order-8"));
            assertThat(readEvents("order-9")).containsExactly(new OrderPlaced("order-9"));
        }

        @Test
        void an_invocation_describes_itself_by_its_stream_id_because_a_lambda_has_no_readable_name() {
            assertThat(Invocation.to("order-10", events -> events)).hasToString("Invocation[streamId=order-10]");
        }

        @Test
        void a_blank_stream_id_is_rejected_when_the_invocation_is_built() {
            assertThatThrownBy(() -> Invocation.to(" ", events -> events))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("streamId cannot be blank");
        }
    }

    private static List<OrderEvent> ship(List<OrderEvent> events, String orderId) {
        boolean alreadyShipped = events.stream().anyMatch(OrderShipped.class::isInstance);
        return alreadyShipped ? List.of() : List.of(new OrderShipped(orderId));
    }

    private List<OrderEvent> readEvents(String streamId) {
        return eventStore.read(streamId).eventList().stream().map(cloudEventConverter::toDomainEvent).toList();
    }

    private sealed interface OrderCommand {
        String orderId();
    }

    private record ShipOrder(String orderId) implements OrderCommand {
    }

    /** Rejected by the deciders and resolvers below, so a batch can be made to fail at a chosen position. */
    private record ExplodeOrder(String orderId) implements OrderCommand {
    }

    private sealed interface OrderEvent {
    }

    private record OrderPlaced(String orderId) implements OrderEvent {
    }

    private record OrderShipped(String orderId) implements OrderEvent {
    }
}
