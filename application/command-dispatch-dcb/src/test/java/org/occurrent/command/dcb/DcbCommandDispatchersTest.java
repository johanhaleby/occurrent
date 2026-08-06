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
import org.junit.jupiter.api.*;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

@DisplayName("DcbCommandDispatchers")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class DcbCommandDispatchersTest {

    private InMemoryEventStore eventStore;
    private CloudEventConverter<OrderEvent> cloudEventConverter;
    private DcbApplicationService<OrderEvent> applicationService;
    private DcbDeciderApplicationService<OrderEvent> deciderApplicationService;

    @BeforeEach
    void create_instances() {
        eventStore = new InMemoryEventStore();
        cloudEventConverter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test")).build();
        applicationService = new GenericDcbApplicationService<>(
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

    @Nested
    @DisplayName("decider dispatchAll")
    class DeciderBatching {

        /** Ships an order at most once, so a folded run can be told apart from separate appends, and rejects an explode. */
        private CommandDispatcher<OrderCommand> dispatcher() {
            Decider<OrderCommand, Boolean, OrderEvent> shipOnce = Decider.create(
                    Boolean.FALSE,
                    (OrderCommand command, Boolean alreadyShipped) -> {
                        if (command instanceof ExplodeOrder) {
                            throw new IllegalStateException("boom");
                        }
                        return alreadyShipped ? List.of() : List.of(new OrderShipped(command.orderId()));
                    },
                    (state, event) -> Boolean.TRUE
            );
            DcbDecider<OrderCommand, Boolean, OrderEvent> dcbDecider =
                    DcbDecider.from(shipOnce, command -> orderQuery(command.orderId()), event -> Set.of(tagFor(event)));
            return DcbCommandDispatchers.decider(deciderApplicationService, dcbDecider);
        }

        @Test
        void each_commands_boundary_is_derived_exactly_once_per_batch() {
            // Given a criteria function that counts how often it is asked
            AtomicInteger derivations = new AtomicInteger();
            Decider<OrderCommand, Void, OrderEvent> shipmentDecider = Decider.create(
                    null,
                    (OrderCommand command, Void state) -> List.of(new OrderShipped(command.orderId())),
                    (state, event) -> state
            );
            DcbDecider<OrderCommand, Void, OrderEvent> dcbDecider = DcbDecider.from(
                    shipmentDecider,
                    command -> {
                        derivations.incrementAndGet();
                        return orderQuery(command.orderId());
                    },
                    event -> Set.of(tagFor(event)));
            CommandDispatcher<OrderCommand> dispatcher = DcbCommandDispatchers.decider(deciderApplicationService, dcbDecider);

            // When a run of three commands sharing one boundary is dispatched as a batch
            dispatcher.dispatchAll(List.of(new ShipOrder("order-15"), new ShipOrder("order-15"), new ShipOrder("order-15")));

            // Then grouping resolved each one and the execute reused what grouping already had
            assertThat(derivations).hasValue(3);
        }

        @Test
        void a_run_inside_one_boundary_becomes_a_single_atomic_append() {
            // Given a run of two commands in one boundary whose second one the decider rejects
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
            // The boundary is derived from the commands, so an empty batch has none to derive it from
            assertThatCode(() -> dispatcher().dispatchAll(List.of())).doesNotThrowAnyException();
        }

        @Test
        void a_command_the_decider_does_not_recognise_fails_the_batch_before_anything_is_appended() {
            // Given a decider that has no boundary for a foreign command, which is what adapt produces
            Decider<OrderCommand, Void, OrderEvent> shipmentDecider = Decider.create(
                    null,
                    (OrderCommand command, Void state) -> List.of(new OrderShipped(command.orderId())),
                    (state, event) -> state
            );
            DcbDecider<OrderCommand, Void, OrderEvent> dcbDecider = DcbDecider.from(
                    shipmentDecider,
                    command -> command instanceof ExplodeOrder ? null : orderQuery(command.orderId()),
                    event -> Set.of(tagFor(event)));
            CommandDispatcher<OrderCommand> dispatcher = DcbCommandDispatchers.decider(deciderApplicationService, dcbDecider);

            // When
            assertThatThrownBy(() -> dispatcher.dispatchAll(List.of(new ShipOrder("order-14"), new ExplodeOrder("order-14"))))
                    .isInstanceOf(IllegalArgumentException.class);

            // Then the first command was not appended either, because boundaries are derived before any run is written
            assertThat(readEvents("order-14")).isEmpty();
        }
    }

    @Nested
    @DisplayName("invocation")
    class Invocations {

        @Test
        void dispatch_runs_the_decision_inside_the_boundary_it_names() {
            // Given
            CommandDispatcher<DcbInvocation<OrderEvent>> dispatcher = DcbCommandDispatchers.invocation(applicationService);

            // When
            dispatcher.dispatch(DcbInvocation.to(orderQuery("order-3"), events -> ship(events, "order-3")));

            // Then
            assertThat(readEvents("order-3")).containsExactly(new OrderShipped("order-3"));
        }

        @Test
        void dispatching_the_same_invocation_twice_is_idempotent_because_the_decision_re_reads_the_boundary() {
            // Given
            CommandDispatcher<DcbInvocation<OrderEvent>> dispatcher = DcbCommandDispatchers.invocation(applicationService);
            DcbInvocation<OrderEvent> shipOnce = DcbInvocation.to(orderQuery("order-4"), events -> ship(events, "order-4"));

            // When
            dispatcher.dispatch(shipOnce);
            dispatcher.dispatch(shipOnce);

            // Then
            assertThat(readEvents("order-4")).containsExactly(new OrderShipped("order-4"));
        }

        @Test
        void an_invocation_can_carry_its_own_tag_generator_when_the_application_service_has_no_global_one() {
            // Given an application service with no global tag generator, so nothing else can tag the decided events
            DcbApplicationService<OrderEvent> untagged = new GenericDcbApplicationService<>(eventStore, cloudEventConverter);
            CommandDispatcher<DcbInvocation<OrderEvent>> dispatcher = DcbCommandDispatchers.invocation(untagged);

            // When
            dispatcher.dispatch(DcbInvocation.to(
                    orderQuery("order-5"),
                    event -> Set.of(tagFor(event)),
                    events -> ship(events, "order-5")));

            // Then the events are findable by the tag, so the invocation's own generator was applied
            assertThat(readEvents("order-5")).containsExactly(new OrderShipped("order-5"));
        }

        @Test
        void an_invocation_describes_itself_by_its_boundary_because_a_lambda_has_no_readable_name() {
            DcbCriteria criteria = orderQuery("order-6");

            assertThat(DcbInvocation.to(criteria, events -> events)).hasToString("DcbInvocation[criteria=" + criteria + "]");
        }
    }

    private static List<OrderEvent> ship(List<OrderEvent> events, String orderId) {
        boolean alreadyShipped = events.stream().anyMatch(OrderShipped.class::isInstance);
        return alreadyShipped ? List.of() : List.of(new OrderShipped(orderId));
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

    /** Rejected by the deciders below, so a batch can be made to fail at a chosen position. */
    private record ExplodeOrder(String orderId) implements OrderCommand {
    }

    private sealed interface OrderEvent {
        String orderId();
    }

    private record OrderShipped(String orderId) implements OrderEvent {
    }
}
