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

package org.occurrent.example.domain.appointmentscheduling.model;

import org.occurrent.dsl.decider.Decider;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.example.domain.appointmentscheduling.event.AppointmentBooked;
import org.occurrent.example.domain.appointmentscheduling.event.AppointmentCancelled;
import org.occurrent.example.domain.appointmentscheduling.event.ClinicianRegistered;
import org.occurrent.example.domain.appointmentscheduling.event.DomainEvent;
import org.occurrent.example.domain.appointmentscheduling.event.PatientRegistered;
import org.occurrent.example.domain.appointmentscheduling.event.SlotDefined;
import org.occurrent.example.domain.appointmentscheduling.model.Commands.AppointmentCommand;
import org.occurrent.example.domain.appointmentscheduling.model.Commands.BookAppointment;
import org.occurrent.example.domain.appointmentscheduling.model.Commands.CancelAppointment;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static java.time.ZoneOffset.UTC;

/**
 * The cross-boundary decision. Booking an appointment must hold one consistency boundary across three
 * entities at once: the clinician and patient must be registered, the slot must be defined and not already
 * booked, and the patient must be under their booking limit. The read boundary spans all three tags, so the
 * decision sees the clinician's registration, the patient's registration and every one of their
 * appointments, and the slot's definition and current booking. Cancelling reads only the slot.
 */
public final class Appointment {
    private Appointment() {
    }

    public static final Decider<AppointmentCommand, State, DomainEvent> DECIDER = Decider.create(
            State.initial(), Appointment::decide, Appointment::evolve);

    public static DcbCriteria criteria(AppointmentCommand command) {
        return switch (command) {
            case BookAppointment book -> DcbCriteria.tagsAnyOf(Tags.clinician(book.clinicianId()), Tags.patient(book.patientId()), Tags.slot(book.slotId()));
            case CancelAppointment cancel -> DcbCriteria.tags(Tags.slot(cancel.slotId()));
        };
    }

    private static List<DomainEvent> decide(AppointmentCommand command, State state) {
        return switch (command) {
            case BookAppointment book -> book(book, state);
            case CancelAppointment cancel -> cancel(cancel, state);
        };
    }

    private static List<DomainEvent> book(BookAppointment command, State state) {
        if (!state.registeredClinicians().contains(command.clinicianId())) {
            throw new IllegalArgumentException("Clinician " + command.clinicianId() + " is not registered");
        }
        Integer limit = state.patientLimits().get(command.patientId());
        if (limit == null) {
            throw new IllegalArgumentException("Patient " + command.patientId() + " is not registered");
        }
        if (!state.definedSlots().contains(command.slotId())) {
            throw new IllegalArgumentException("Slot " + command.slotId() + " is not defined");
        }
        if (state.activeBySlot().containsKey(command.slotId())) {
            throw new IllegalStateException("Slot " + command.slotId() + " is already booked");
        }
        int count = state.activeCountByPatient().getOrDefault(command.patientId(), 0);
        if (count >= limit) {
            throw new IllegalStateException("Patient " + command.patientId() + " has reached the booking limit of " + limit);
        }
        return List.of(new AppointmentBooked(UUID.randomUUID(), OffsetDateTime.now(UTC), command.clinicianId(), command.patientId(), command.slotId()));
    }

    private static List<DomainEvent> cancel(CancelAppointment command, State state) {
        State.Booking booking = state.activeBySlot().get(command.slotId());
        if (booking == null) {
            throw new IllegalStateException("Slot " + command.slotId() + " has no active appointment to cancel");
        }
        return List.of(new AppointmentCancelled(UUID.randomUUID(), OffsetDateTime.now(UTC), booking.clinicianId(), booking.patientId(), command.slotId()));
    }

    private static State evolve(State state, DomainEvent event) {
        return switch (event) {
            case ClinicianRegistered e -> state.withClinician(e.clinicianId());
            case PatientRegistered e -> state.withPatient(e.patientId(), e.maxAppointments());
            case SlotDefined e -> state.withSlot(e.slotId());
            case AppointmentBooked e -> state.withBooking(e.slotId(), e.clinicianId(), e.patientId());
            case AppointmentCancelled e -> state.withCancellation(e.slotId(), e.patientId());
            default -> state;
        };
    }

    /**
     * Keyed by entity id so a decision reads only the ids named by its command, which the DCB criteria
     * guarantees are complete in the folded events.
     */
    public record State(Set<UUID> registeredClinicians, Map<UUID, Integer> patientLimits, Set<UUID> definedSlots,
                        Map<UUID, Booking> activeBySlot, Map<UUID, Integer> activeCountByPatient) {

        public record Booking(UUID clinicianId, UUID patientId) {
        }

        static State initial() {
            return new State(Set.of(), Map.of(), Set.of(), Map.of(), Map.of());
        }

        State withClinician(UUID clinicianId) {
            return new State(plus(registeredClinicians, clinicianId), patientLimits, definedSlots, activeBySlot, activeCountByPatient);
        }

        State withPatient(UUID patientId, int maxAppointments) {
            return new State(registeredClinicians, put(patientLimits, patientId, maxAppointments), definedSlots, activeBySlot, activeCountByPatient);
        }

        State withSlot(UUID slotId) {
            return new State(registeredClinicians, patientLimits, plus(definedSlots, slotId), activeBySlot, activeCountByPatient);
        }

        State withBooking(UUID slotId, UUID clinicianId, UUID patientId) {
            return new State(registeredClinicians, patientLimits, definedSlots, put(activeBySlot, slotId, new Booking(clinicianId, patientId)), add(activeCountByPatient, patientId, 1));
        }

        State withCancellation(UUID slotId, UUID patientId) {
            return new State(registeredClinicians, patientLimits, definedSlots, remove(activeBySlot, slotId), add(activeCountByPatient, patientId, -1));
        }

        private static <T> Set<T> plus(Set<T> set, T element) {
            Set<T> copy = new HashSet<>(set);
            copy.add(element);
            return Set.copyOf(copy);
        }

        private static <K, V> Map<K, V> put(Map<K, V> map, K key, V value) {
            Map<K, V> copy = new HashMap<>(map);
            copy.put(key, value);
            return Map.copyOf(copy);
        }

        private static <K, V> Map<K, V> remove(Map<K, V> map, K key) {
            Map<K, V> copy = new HashMap<>(map);
            copy.remove(key);
            return Map.copyOf(copy);
        }

        private static <K> Map<K, Integer> add(Map<K, Integer> map, K key, int delta) {
            Map<K, Integer> copy = new HashMap<>(map);
            copy.merge(key, delta, Integer::sum);
            return Map.copyOf(copy);
        }
    }
}
