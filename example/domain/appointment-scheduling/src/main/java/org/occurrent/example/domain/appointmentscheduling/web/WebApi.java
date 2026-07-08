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

package org.occurrent.example.domain.appointmentscheduling.web;

import io.javalin.Javalin;
import io.javalin.http.Context;
import j2html.tags.ContainerTag;
import org.occurrent.example.domain.appointmentscheduling.application.AppointmentSchedulingService;
import org.occurrent.example.domain.appointmentscheduling.application.SchedulingQueries;
import org.occurrent.example.domain.appointmentscheduling.application.SchedulingQueries.AppointmentView;
import org.occurrent.example.domain.appointmentscheduling.application.SchedulingQueries.Overview;
import org.occurrent.example.domain.appointmentscheduling.application.SchedulingQueries.SlotView;
import org.occurrent.example.domain.appointmentscheduling.model.Commands.BookAppointment;
import org.occurrent.example.domain.appointmentscheduling.model.Commands.CancelAppointment;
import org.occurrent.example.domain.appointmentscheduling.model.Commands.DefineSlot;
import org.occurrent.example.domain.appointmentscheduling.model.Commands.RegisterClinician;
import org.occurrent.example.domain.appointmentscheduling.model.Commands.RegisterPatient;

import java.net.URLEncoder;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static j2html.TagCreator.a;
import static j2html.TagCreator.body;
import static j2html.TagCreator.button;
import static j2html.TagCreator.div;
import static j2html.TagCreator.each;
import static j2html.TagCreator.form;
import static j2html.TagCreator.h1;
import static j2html.TagCreator.h2;
import static j2html.TagCreator.input;
import static j2html.TagCreator.label;
import static j2html.TagCreator.li;
import static j2html.TagCreator.option;
import static j2html.TagCreator.p;
import static j2html.TagCreator.select;
import static j2html.TagCreator.span;
import static j2html.TagCreator.ul;

public final class WebApi {
    private WebApi() {
    }

    public static void configureRoutes(Javalin javalin, AppointmentSchedulingService service, SchedulingQueries queries) {
        javalin.get("/", ctx -> ctx.html(overviewPage(queries.overview(), ctx.queryParam("error"))));

        javalin.post("/clinicians", ctx -> submit(ctx, () ->
                service.registerClinician(new RegisterClinician(UUID.randomUUID(), required(ctx, "name")))));

        javalin.post("/patients", ctx -> submit(ctx, () ->
                service.registerPatient(new RegisterPatient(UUID.randomUUID(), required(ctx, "name"), Integer.parseInt(required(ctx, "maxAppointments"))))));

        javalin.post("/slots", ctx -> submit(ctx, () ->
                service.defineSlot(new DefineSlot(UUID.randomUUID(), LocalDateTime.parse(required(ctx, "startTime")).atOffset(UTC)))));

        javalin.post("/appointments", ctx -> submit(ctx, () ->
                service.bookAppointment(new BookAppointment(formUuid(ctx, "clinicianId"), formUuid(ctx, "patientId"), formUuid(ctx, "slotId")))));

        javalin.post("/appointments/cancel", ctx -> submit(ctx, () ->
                service.cancelAppointment(new CancelAppointment(formUuid(ctx, "slotId")))));

        javalin.get("/clinician/:id", ctx -> ctx.html(queries.clinician(pathUuid(ctx, "id"))
                .map(WebApi::clinicianPage).orElseGet(() -> notFoundPage("Clinician"))));

        javalin.get("/patient/:id", ctx -> ctx.html(queries.patient(pathUuid(ctx, "id"))
                .map(WebApi::patientPage).orElseGet(() -> notFoundPage("Patient"))));

        javalin.get("/slot/:id", ctx -> ctx.html(queries.slot(pathUuid(ctx, "id"))
                .map(WebApi::slotPage).orElseGet(() -> notFoundPage("Slot"))));
    }

    private static void submit(Context ctx, Runnable action) {
        try {
            action.run();
            ctx.redirect("/");
        } catch (Exception e) {
            String message = e.getMessage() == null ? e.toString() : e.getMessage();
            ctx.redirect("/?error=" + URLEncoder.encode(message, UTF_8));
        }
    }

    private static String overviewPage(Overview overview, String error) {
        return page(() -> body(
                h1("Appointment scheduling"),
                error == null ? span() : div(error).withStyle("color:#b00;font-weight:bold;margin:1em 0;"),
                cliniciansSection(overview),
                patientsSection(overview),
                slotsSection(overview),
                bookingSection(overview),
                appointmentsSection(overview)
        ));
    }

    private static ContainerTag cliniciansSection(Overview overview) {
        return div(
                h2("Clinicians"),
                form().withMethod("post").withAction("/clinicians").with(
                        label("Name ").with(input().withName("name").isRequired()),
                        button("Register clinician").withType("submit")),
                ul(each(overview.clinicians(), c -> li(a(c.name()).withHref("/clinician/" + c.id()))))
        );
    }

    private static ContainerTag patientsSection(Overview overview) {
        return div(
                h2("Patients"),
                form().withMethod("post").withAction("/patients").with(
                        label("Name ").with(input().withName("name").isRequired()),
                        label(" Max appointments ").with(input().withName("maxAppointments").withType("number").attr("min", 1).withValue("2").isRequired()),
                        button("Register patient").withType("submit")),
                ul(each(overview.patients(), p -> li(
                        a(p.name()).withHref("/patient/" + p.id()),
                        span(" (" + p.activeAppointments() + "/" + p.maxAppointments() + " booked, " + p.remaining() + " remaining)"))))
        );
    }

    private static ContainerTag slotsSection(Overview overview) {
        return div(
                h2("Slots"),
                form().withMethod("post").withAction("/slots").with(
                        label("Start time ").with(input().withName("startTime").withType("datetime-local").isRequired()),
                        button("Define slot").withType("submit")),
                ul(each(overview.slots(), s -> li(
                        a(s.startTime().toString()).withHref("/slot/" + s.id()),
                        span(s.booked() ? " (booked)" : " (free)"))))
        );
    }

    private static ContainerTag bookingSection(Overview overview) {
        List<SlotView> freeSlots = overview.slots().stream().filter(s -> !s.booked()).toList();
        return div(
                h2("Book appointment"),
                form().withMethod("post").withAction("/appointments").with(
                        label("Clinician ").with(select().withName("clinicianId").with(
                                each(overview.clinicians(), c -> option(c.name()).withValue(c.id().toString())))),
                        label(" Patient ").with(select().withName("patientId").with(
                                each(overview.patients(), p -> option(p.name()).withValue(p.id().toString())))),
                        label(" Slot ").with(select().withName("slotId").with(
                                each(freeSlots, s -> option(s.startTime().toString()).withValue(s.id().toString())))),
                        button("Book").withType("submit"))
        );
    }

    private static ContainerTag appointmentsSection(Overview overview) {
        return div(
                h2("Appointments"),
                ul(each(overview.appointments(), a -> li(
                        span("clinician " + shortId(a.clinicianId()) + ", patient " + shortId(a.patientId()) + ", slot " + shortId(a.slotId()) + " "),
                        form().withMethod("post").withAction("/appointments/cancel").withStyle("display:inline;").with(
                                input().withName("slotId").withType("hidden").withValue(a.slotId().toString()),
                                button("Cancel").withType("submit")))))
        );
    }

    private static String clinicianPage(SchedulingQueries.ClinicianDetail detail) {
        return page(() -> body(
                h1("Clinician " + detail.name()),
                appointmentsList(detail.appointments()),
                backLink()));
    }

    private static String patientPage(SchedulingQueries.PatientDetail detail) {
        return page(() -> body(
                h1("Patient " + detail.name()),
                p(detail.appointments().size() + " of " + detail.maxAppointments() + " booked, " + detail.remaining() + " remaining"),
                appointmentsList(detail.appointments()),
                backLink()));
    }

    private static String slotPage(SchedulingQueries.SlotDetail detail) {
        AppointmentView booking = detail.booking();
        return page(() -> body(
                h1("Slot " + detail.startTime()),
                booking == null
                        ? p("Free")
                        : p("Booked by clinician " + shortId(booking.clinicianId()) + " for patient " + shortId(booking.patientId())),
                backLink()));
    }

    private static ContainerTag appointmentsList(List<AppointmentView> appointments) {
        return appointments.isEmpty()
                ? p("No appointments")
                : ul(each(appointments, a -> li("clinician " + shortId(a.clinicianId()) + ", patient " + shortId(a.patientId()) + ", slot " + shortId(a.slotId()))));
    }

    private static ContainerTag backLink() {
        return p(a("Back").withHref("/"));
    }

    private static String notFoundPage(String what) {
        return page(() -> body(h1(what + " not found"), backLink()));
    }

    private static String page(Supplier<ContainerTag> body) {
        return body.get().withStyle("font-family:sans-serif;max-width:760px;margin:2em auto;").render();
    }

    private static String required(Context ctx, String name) {
        return requireNonNull(ctx.formParam(name), name + " is required");
    }

    private static UUID formUuid(Context ctx, String name) {
        return UUID.fromString(required(ctx, name));
    }

    private static UUID pathUuid(Context ctx, String name) {
        return UUID.fromString(ctx.pathParam(name));
    }

    private static String shortId(UUID id) {
        return id.toString().substring(0, 8);
    }
}
