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
import org.occurrent.example.domain.appointmentscheduling.event.DomainEvent;
import org.occurrent.example.domain.appointmentscheduling.event.PatientRegistered;
import org.occurrent.example.domain.appointmentscheduling.model.Commands.RegisterPatient;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static java.time.ZoneOffset.UTC;

/**
 * Registers a patient once, with the maximum number of appointments they may hold at a time.
 */
public final class Patient {
    private Patient() {
    }

    public enum State {NOT_REGISTERED, REGISTERED}

    public static final Decider<RegisterPatient, State, DomainEvent> DECIDER = Decider.create(
            State.NOT_REGISTERED,
            (command, state) -> {
                if (state == State.REGISTERED) {
                    throw new IllegalStateException("Patient " + command.patientId() + " is already registered");
                }
                if (command.maxAppointments() < 1) {
                    throw new IllegalArgumentException("maxAppointments must be at least 1");
                }
                return List.of(new PatientRegistered(UUID.randomUUID(), OffsetDateTime.now(UTC), command.patientId(), command.name(), command.maxAppointments()));
            },
            (state, event) -> event instanceof PatientRegistered ? State.REGISTERED : state);

    public static DcbCriteria criteria(RegisterPatient command) {
        return DcbCriteria.type(PatientRegistered.class.getSimpleName()).tags(Tags.patient(command.patientId()));
    }
}
