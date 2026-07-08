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
import org.occurrent.example.domain.appointmentscheduling.event.ClinicianRegistered;
import org.occurrent.example.domain.appointmentscheduling.event.DomainEvent;
import org.occurrent.example.domain.appointmentscheduling.model.Commands.RegisterClinician;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static java.time.ZoneOffset.UTC;

/**
 * Registers a clinician once. The read boundary is the clinician's own registration events.
 */
public final class Clinician {
    private Clinician() {
    }

    public enum State {NOT_REGISTERED, REGISTERED}

    public static final Decider<RegisterClinician, State, DomainEvent> DECIDER = Decider.create(
            State.NOT_REGISTERED,
            (command, state) -> {
                if (state == State.REGISTERED) {
                    throw new IllegalStateException("Clinician " + command.clinicianId() + " is already registered");
                }
                return List.of(new ClinicianRegistered(UUID.randomUUID(), OffsetDateTime.now(UTC), command.clinicianId(), command.name()));
            },
            (state, event) -> event instanceof ClinicianRegistered ? State.REGISTERED : state);

    public static DcbCriteria criteria(RegisterClinician command) {
        return DcbCriteria.type(ClinicianRegistered.class.getSimpleName()).tags(Tags.clinician(command.clinicianId()));
    }
}
