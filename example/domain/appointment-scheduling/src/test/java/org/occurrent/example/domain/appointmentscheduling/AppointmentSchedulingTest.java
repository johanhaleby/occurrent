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

package org.occurrent.example.domain.appointmentscheduling;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.blocking.dcb.DcbApplicationService;
import org.occurrent.application.service.blocking.dcb.GenericDcbApplicationService;
import org.occurrent.application.service.dcb.annotation.AnnotationTagGenerator;
import org.occurrent.dsl.dcb.blocking.DcbDomainEventQueries;
import org.occurrent.dsl.query.blocking.DomainEventQueries;
import org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.example.domain.appointmentscheduling.application.AppointmentSchedulingService;
import org.occurrent.example.domain.appointmentscheduling.application.SchedulingQueries;
import org.occurrent.example.domain.appointmentscheduling.event.DomainEvent;
import org.occurrent.example.domain.appointmentscheduling.model.Commands.BookAppointment;
import org.occurrent.example.domain.appointmentscheduling.model.Commands.CancelAppointment;
import org.occurrent.example.domain.appointmentscheduling.model.Commands.DefineSlot;
import org.occurrent.example.domain.appointmentscheduling.model.Commands.RegisterClinician;
import org.occurrent.example.domain.appointmentscheduling.model.Commands.RegisterPatient;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class AppointmentSchedulingTest {

    private static final URI SOURCE = URI.create("urn:occurrent:domain:appointmentscheduling");

    @Container
    private static final MongoDBContainer mongoDBContainer;

    static {
        mongoDBContainer = new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version", "8.0")).withReplicaSet();
    }

    private MongoClient mongoClient;
    private AppointmentSchedulingService service;
    private SchedulingQueries queries;

    @BeforeEach
    void wire_the_example_against_a_clean_database() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".appointments");
        mongoClient = MongoClients.create(connectionString);
        mongoClient.getDatabase(requireNonNull(connectionString.getDatabase())).drop();

        EventStoreConfig config = new EventStoreConfig.Builder()
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        MongoEventStore eventStore = new MongoEventStore(mongoClient, connectionString.getDatabase(), "events", config);
        CloudEventConverter<DomainEvent> converter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), SOURCE)
                .typeMapper(ReflectionCloudEventTypeMapper.simple(DomainEvent.class))
                .idMapper(event -> event.eventId().toString())
                .timeMapper(DomainEvent::occurredAt)
                .build();
        DcbApplicationService<DomainEvent> applicationService = new GenericDcbApplicationService<>(eventStore, converter, new AnnotationTagGenerator<>());
        service = new AppointmentSchedulingService(applicationService);
        queries = new SchedulingQueries(new DcbDomainEventQueries<>(new DomainEventQueries<>(eventStore, converter)));
    }

    @AfterEach
    void close_mongo_client() {
        mongoClient.close();
    }

    @Test
    void books_an_appointment_when_the_clinician_patient_and_slot_are_ready() {
        UUID clinician = registerClinician();
        UUID patient = registerPatient(2);
        UUID slot = defineSlot();

        service.bookAppointment(new BookAppointment(clinician, patient, slot));

        assertThat(queries.overview().appointments()).hasSize(1);
        assertThat(queries.slot(slot).orElseThrow().booking()).isNotNull();
    }

    @Test
    void rejects_booking_a_slot_that_is_already_booked() {
        UUID clinician = registerClinician();
        UUID firstPatient = registerPatient(2);
        UUID secondPatient = registerPatient(2);
        UUID slot = defineSlot();
        service.bookAppointment(new BookAppointment(clinician, firstPatient, slot));

        assertThatThrownBy(() -> service.bookAppointment(new BookAppointment(clinician, secondPatient, slot)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already booked");
    }

    @Test
    void rejects_booking_when_the_clinician_is_not_registered() {
        UUID patient = registerPatient(2);
        UUID slot = defineSlot();

        assertThatThrownBy(() -> service.bookAppointment(new BookAppointment(UUID.randomUUID(), patient, slot)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Clinician");
    }

    @Test
    void rejects_booking_when_the_patient_is_not_registered() {
        UUID clinician = registerClinician();
        UUID slot = defineSlot();

        assertThatThrownBy(() -> service.bookAppointment(new BookAppointment(clinician, UUID.randomUUID(), slot)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Patient");
    }

    @Test
    void enforces_the_patient_booking_limit() {
        UUID clinician = registerClinician();
        UUID patient = registerPatient(2);
        UUID slot1 = defineSlot();
        UUID slot2 = defineSlot();
        UUID slot3 = defineSlot();
        service.bookAppointment(new BookAppointment(clinician, patient, slot1));
        service.bookAppointment(new BookAppointment(clinician, patient, slot2));

        assertThatThrownBy(() -> service.bookAppointment(new BookAppointment(clinician, patient, slot3)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("booking limit");
    }

    @Test
    void cancelling_frees_the_slot_and_returns_the_booking_to_the_patient() {
        UUID clinician = registerClinician();
        UUID patient = registerPatient(1);
        UUID slot = defineSlot();
        service.bookAppointment(new BookAppointment(clinician, patient, slot));

        service.cancelAppointment(new CancelAppointment(slot));

        assertThat(queries.slot(slot).orElseThrow().booking()).isNull();
        assertThat(queries.patient(patient).orElseThrow().remaining()).isEqualTo(1);
        // The freed limit lets the patient book again.
        service.bookAppointment(new BookAppointment(clinician, patient, slot));
        assertThat(queries.overview().appointments()).hasSize(1);
    }

    @Test
    void two_concurrent_bookings_of_the_same_slot_let_exactly_one_win() throws Exception {
        UUID clinician = registerClinician();
        UUID firstPatient = registerPatient(1);
        UUID secondPatient = registerPatient(1);
        UUID slot = defineSlot();

        CyclicBarrier startTogether = new CyclicBarrier(2);
        AtomicInteger succeeded = new AtomicInteger();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            List<Future<?>> futures = List.of(
                    executor.submit(booking(startTogether, succeeded, clinician, firstPatient, slot)),
                    executor.submit(booking(startTogether, succeeded, clinician, secondPatient, slot)));
            for (Future<?> future : futures) {
                future.get();
            }
        } finally {
            executor.shutdownNow();
        }

        assertThat(succeeded).hasValue(1);
        assertThat(queries.overview().appointments()).hasSize(1);
    }

    private Runnable booking(CyclicBarrier startTogether, AtomicInteger succeeded, UUID clinician, UUID patient, UUID slot) {
        return () -> {
            try {
                startTogether.await();
                service.bookAppointment(new BookAppointment(clinician, patient, slot));
                succeeded.incrementAndGet();
            } catch (IllegalStateException | DcbAppendConditionNotFulfilledException e) {
                // The losing booking either sees the slot already booked after the winner committed, or, if the
                // optimistic retries are exhausted first, fails the DCB append condition. Either way it did not book.
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private UUID registerClinician() {
        UUID clinicianId = UUID.randomUUID();
        service.registerClinician(new RegisterClinician(clinicianId, "Dr " + shortId(clinicianId)));
        return clinicianId;
    }

    private UUID registerPatient(int maxAppointments) {
        UUID patientId = UUID.randomUUID();
        service.registerPatient(new RegisterPatient(patientId, "Patient " + shortId(patientId), maxAppointments));
        return patientId;
    }

    private UUID defineSlot() {
        UUID slotId = UUID.randomUUID();
        service.defineSlot(new DefineSlot(slotId, OffsetDateTime.now(UTC)));
        return slotId;
    }

    private static String shortId(UUID id) {
        return id.toString().substring(0, 8);
    }
}
