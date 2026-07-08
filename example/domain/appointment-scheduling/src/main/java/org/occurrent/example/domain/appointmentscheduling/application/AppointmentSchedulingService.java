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

import org.occurrent.application.service.blocking.dcb.DcbApplicationService;
import org.occurrent.example.domain.appointmentscheduling.event.DomainEvent;
import org.occurrent.example.domain.appointmentscheduling.model.Appointment;
import org.occurrent.example.domain.appointmentscheduling.model.Clinician;
import org.occurrent.example.domain.appointmentscheduling.model.Commands.BookAppointment;
import org.occurrent.example.domain.appointmentscheduling.model.Commands.CancelAppointment;
import org.occurrent.example.domain.appointmentscheduling.model.Commands.DefineSlot;
import org.occurrent.example.domain.appointmentscheduling.model.Commands.RegisterClinician;
import org.occurrent.example.domain.appointmentscheduling.model.Commands.RegisterPatient;
import org.occurrent.example.domain.appointmentscheduling.model.Patient;
import org.occurrent.example.domain.appointmentscheduling.model.Slot;

/**
 * Runs each command by reading the events its decider's criteria selects, deciding, and appending. The plain
 * decider does the folding, the criteria defines the DCB read and append boundary, and the application
 * service supplies the optimistic append condition and tags the new events.
 */
public class AppointmentSchedulingService {
    private final DcbApplicationService<DomainEvent> applicationService;

    public AppointmentSchedulingService(DcbApplicationService<DomainEvent> applicationService) {
        this.applicationService = applicationService;
    }

    public void registerClinician(RegisterClinician command) {
        applicationService.execute(Clinician.criteria(command),
                events -> Clinician.DECIDER.decideOnEventsAndReturnEvents(events, command));
    }

    public void registerPatient(RegisterPatient command) {
        applicationService.execute(Patient.criteria(command),
                events -> Patient.DECIDER.decideOnEventsAndReturnEvents(events, command));
    }

    public void defineSlot(DefineSlot command) {
        applicationService.execute(Slot.criteria(command),
                events -> Slot.DECIDER.decideOnEventsAndReturnEvents(events, command));
    }

    public void bookAppointment(BookAppointment command) {
        applicationService.execute(Appointment.criteria(command),
                events -> Appointment.DECIDER.decideOnEventsAndReturnEvents(events, command));
    }

    public void cancelAppointment(CancelAppointment command) {
        applicationService.execute(Appointment.criteria(command),
                events -> Appointment.DECIDER.decideOnEventsAndReturnEvents(events, command));
    }
}
