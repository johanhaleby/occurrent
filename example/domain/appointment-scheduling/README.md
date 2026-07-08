# Appointment scheduling (DCB, no Spring)

A small clinic scheduler that books appointments across three entities at once: a clinician, a patient, and
a time slot. Booking has to hold one consistency boundary over all three, which is exactly what Occurrent's
Dynamic Consistency Boundary (DCB) support is for.

This is the third DCB example in the repository, and it is built deliberately unlike the other two. Where
`hotel-booking` and `course-enrollment` are Kotlin on Spring Boot and use the `dcbDecider` DSL, this one is:

- **Plain Java, no Spring.** A `main` in [`Bootstrap`](src/main/java/org/occurrent/example/domain/appointmentscheduling/Bootstrap.java)
  wires everything by hand, and the website is served with Javalin and j2html.
- **Native MongoDB.** It uses the native-driver `MongoEventStore` with the DCB capability turned on
  (`eventStoreCapabilities(STREAM, DCB)`), constructed from a plain `MongoClient`.
- **Plain deciders.** The domain is written as ordinary `Decider`s. There is no `DcbDecider`. The read
  boundary for each command is a `DcbCriteria` built at the call site in
  [`AppointmentSchedulingService`](src/main/java/org/occurrent/example/domain/appointmentscheduling/application/AppointmentSchedulingService.java).
- **Annotation-based tags.** Events carry `@DcbTag` on their id fields, and an `AnnotationTagGenerator`
  turns each event into its tags, so there is no hand-written event-to-tag function. The small `Tags`
  helper still exists, but only to build the tags a read criterion matches on, not to tag events.

## The invariants

Booking an appointment enforces, atomically:

- the clinician is registered
- the patient is registered
- the slot is defined and not already booked (a slot is booked at most once)
- the patient is under their appointment limit

The last two are the interesting ones. Slot uniqueness is a fact about the slot, the booking limit is a fact
about the patient, and a single booking has to respect both at the same time. The command reads
`DcbCriteria.tagsAnyOf(clinician, patient, slot)`, so the decision sees the clinician's registration, the
patient's registration and every one of their appointments, and the slot's current state. The append is
rejected if any matching event was committed since the read, so two people racing for the same slot cannot
both win, and a patient cannot exceed their limit by booking concurrently.

## Running it

DCB appends use a multi-document transaction, so MongoDB must run as a replica set.

The easy path needs no local MongoDB. Run
[`LocalLauncher`](src/test/java/org/occurrent/example/domain/appointmentscheduling/LocalLauncher.java), which
starts a throwaway replica set with Testcontainers and boots the app, then open http://localhost:7000.

To run against your own MongoDB instead, start a replica set on `mongodb://localhost:27017` and run
`Bootstrap`.

## Tests

[`AppointmentSchedulingTest`](src/test/java/org/occurrent/example/domain/appointmentscheduling/AppointmentSchedulingTest.java)
drives the use cases against a real native store on a Testcontainers replica set and covers slot uniqueness,
registration checks, the booking limit, cancellation, and two threads racing for the same slot.

```
mvn -Pexamples-module -pl example/domain/appointment-scheduling test
```
