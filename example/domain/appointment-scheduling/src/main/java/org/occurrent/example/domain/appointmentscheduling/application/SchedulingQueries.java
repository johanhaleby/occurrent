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

package org.occurrent.example.domain.appointmentscheduling.application;

import org.jspecify.annotations.Nullable;
import org.occurrent.dsl.dcb.blocking.DcbDomainEventQueries;
import org.occurrent.example.domain.appointmentscheduling.event.*;
import org.occurrent.example.domain.appointmentscheduling.model.Tags;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Strongly consistent read side. It queries the DCB store through {@link DcbDomainEventQueries}, which hands
 * back domain events for a criterion, so this class never touches the raw store or the cloud event converter.
 * Criteria are built from event classes with the query DSL's criteria builder.
 */
public class SchedulingQueries {
    private final DcbDomainEventQueries<DomainEvent> queries;

    public SchedulingQueries(DcbDomainEventQueries<DomainEvent> queries) {
        this.queries = queries;
    }

    public Overview overview() {
        List<AppointmentBooked> active = activeFrom(queries.types(AppointmentBooked.class, AppointmentCancelled.class).toList());
        Map<UUID, Long> activeByPatient = active.stream().collect(Collectors.groupingBy(AppointmentBooked::patientId, Collectors.counting()));
        Set<UUID> bookedSlots = active.stream().map(AppointmentBooked::slotId).collect(Collectors.toSet());

        List<ClinicianView> clinicians = queries.types(ClinicianRegistered.class)
                .map(e -> new ClinicianView(e.clinicianId(), e.name()))
                .toList();
        List<PatientView> patients = queries.types(PatientRegistered.class)
                .map(e -> new PatientView(e.patientId(), e.name(), e.maxAppointments(), activeByPatient.getOrDefault(e.patientId(), 0L).intValue()))
                .toList();
        List<SlotView> slots = queries.types(SlotDefined.class)
                .map(e -> new SlotView(e.slotId(), e.startTime(), bookedSlots.contains(e.slotId())))
                .toList();
        List<AppointmentView> appointments = active.stream().map(SchedulingQueries::toAppointment).toList();
        return new Overview(clinicians, patients, slots, appointments);
    }

    public Optional<ClinicianDetail> clinician(UUID clinicianId) {
        List<DomainEvent> events = queries.tags(Tags.clinician(clinicianId)).toList();
        return events.stream().filter(ClinicianRegistered.class::isInstance).map(ClinicianRegistered.class::cast).findFirst()
                .map(registered -> new ClinicianDetail(registered.clinicianId(), registered.name(),
                        activeFrom(events).stream().map(SchedulingQueries::toAppointment).toList()));
    }

    public Optional<PatientDetail> patient(UUID patientId) {
        List<DomainEvent> events = queries.tags(Tags.patient(patientId)).toList();
        return events.stream().filter(PatientRegistered.class::isInstance).map(PatientRegistered.class::cast).findFirst()
                .map(registered -> new PatientDetail(registered.patientId(), registered.name(), registered.maxAppointments(),
                        activeFrom(events).stream().map(SchedulingQueries::toAppointment).toList()));
    }

    public Optional<SlotDetail> slot(UUID slotId) {
        List<DomainEvent> events = queries.tags(Tags.slot(slotId)).toList();
        return events.stream().filter(SlotDefined.class::isInstance).map(SlotDefined.class::cast).findFirst()
                .map(defined -> {
                    List<AppointmentBooked> active = activeFrom(events);
                    AppointmentView booking = active.isEmpty() ? null : toAppointment(active.get(0));
                    return new SlotDetail(defined.slotId(), defined.startTime(), booking);
                });
    }

    // Folds booked/cancelled in read (sequence) order into the appointments that are currently active.
    private static List<AppointmentBooked> activeFrom(List<DomainEvent> events) {
        Map<UUID, AppointmentBooked> bySlot = new LinkedHashMap<>();
        for (DomainEvent event : events) {
            if (event instanceof AppointmentBooked booked) {
                bySlot.put(booked.slotId(), booked);
            } else if (event instanceof AppointmentCancelled cancelled) {
                bySlot.remove(cancelled.slotId());
            }
        }
        return List.copyOf(bySlot.values());
    }

    private static AppointmentView toAppointment(AppointmentBooked booked) {
        return new AppointmentView(booked.clinicianId(), booked.patientId(), booked.slotId());
    }

    public record Overview(List<ClinicianView> clinicians, List<PatientView> patients, List<SlotView> slots, List<AppointmentView> appointments) {
    }

    public record ClinicianView(UUID id, String name) {
    }

    public record PatientView(UUID id, String name, int maxAppointments, int activeAppointments) {
        public int remaining() {
            return maxAppointments - activeAppointments;
        }
    }

    public record SlotView(UUID id, OffsetDateTime startTime, boolean booked) {
    }

    public record AppointmentView(UUID clinicianId, UUID patientId, UUID slotId) {
    }

    public record ClinicianDetail(UUID id, String name, List<AppointmentView> appointments) {
    }

    public record PatientDetail(UUID id, String name, int maxAppointments, List<AppointmentView> appointments) {
        public int remaining() {
            return maxAppointments - appointments.size();
        }
    }

    public record SlotDetail(UUID id, OffsetDateTime startTime, @Nullable AppointmentView booking) {
    }
}
