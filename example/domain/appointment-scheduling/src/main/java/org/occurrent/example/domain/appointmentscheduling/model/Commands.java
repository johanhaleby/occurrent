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

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * The commands accepted by the four deciders.
 */
public final class Commands {
    private Commands() {
    }

    public record RegisterClinician(UUID clinicianId, String name) {
    }

    public record RegisterPatient(UUID patientId, String name, int maxAppointments) {
    }

    public record DefineSlot(UUID slotId, OffsetDateTime startTime) {
    }

    public sealed interface AppointmentCommand {
        UUID slotId();
    }

    public record BookAppointment(UUID clinicianId, UUID patientId, UUID slotId) implements AppointmentCommand {
    }

    public record CancelAppointment(UUID slotId) implements AppointmentCommand {
    }
}
