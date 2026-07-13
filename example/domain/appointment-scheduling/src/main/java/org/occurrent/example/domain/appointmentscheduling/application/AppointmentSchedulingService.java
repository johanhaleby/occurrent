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
import org.occurrent.application.service.dcb.TagGenerator;
import org.occurrent.application.service.dcb.annotation.AnnotationTagGenerator;
import org.occurrent.dsl.dcb.DcbDecider;
import org.occurrent.dsl.dcb.blocking.DcbDeciderApplicationService;
import org.occurrent.example.domain.appointmentscheduling.event.DomainEvent;
import org.occurrent.example.domain.appointmentscheduling.model.Appointment;
import org.occurrent.example.domain.appointmentscheduling.model.Clinician;
import org.occurrent.example.domain.appointmentscheduling.model.Commands.AppointmentCommand;
import org.occurrent.example.domain.appointmentscheduling.model.Commands.BookAppointment;
import org.occurrent.example.domain.appointmentscheduling.model.Commands.CancelAppointment;
import org.occurrent.example.domain.appointmentscheduling.model.Commands.DefineSlot;
import org.occurrent.example.domain.appointmentscheduling.model.Commands.RegisterClinician;
import org.occurrent.example.domain.appointmentscheduling.model.Commands.RegisterPatient;
import org.occurrent.example.domain.appointmentscheduling.model.Patient;
import org.occurrent.example.domain.appointmentscheduling.model.Slot;

/**
 * Runs each command through a {@link DcbDecider} that bundles the feature's decider, its DCB read boundary, and the
 * tags for the events it writes. {@link DcbDeciderApplicationService#execute} reads the events the boundary selects,
 * decides, tags the new events, and appends them under the DCB optimistic condition.
 */
public class AppointmentSchedulingService {

    // The tags are derived from the events themselves via their annotations, so one stateless generator serves every
    // decider.
    private static final TagGenerator<DomainEvent> TAGS = new AnnotationTagGenerator<>();

    private static final DcbDecider<RegisterClinician, ?, DomainEvent> CLINICIAN = DcbDecider.from(Clinician.DECIDER, Clinician::criteria, TAGS);
    private static final DcbDecider<RegisterPatient, ?, DomainEvent> PATIENT = DcbDecider.from(Patient.DECIDER, Patient::criteria, TAGS);
    private static final DcbDecider<DefineSlot, ?, DomainEvent> SLOT = DcbDecider.from(Slot.DECIDER, Slot::criteria, TAGS);
    private static final DcbDecider<AppointmentCommand, ?, DomainEvent> APPOINTMENT = DcbDecider.from(Appointment.DECIDER, Appointment::criteria, TAGS);

    private final DcbDeciderApplicationService<DomainEvent> applicationService;

    public AppointmentSchedulingService(DcbApplicationService<DomainEvent> applicationService) {
        this.applicationService = new DcbDeciderApplicationService<>(applicationService);
    }

    public void registerClinician(RegisterClinician command) {
        applicationService.execute(command, CLINICIAN);
    }

    public void registerPatient(RegisterPatient command) {
        applicationService.execute(command, PATIENT);
    }

    public void defineSlot(DefineSlot command) {
        applicationService.execute(command, SLOT);
    }

    public void bookAppointment(BookAppointment command) {
        applicationService.execute(command, APPOINTMENT);
    }

    public void cancelAppointment(CancelAppointment command) {
        applicationService.execute(command, APPOINTMENT);
    }
}
