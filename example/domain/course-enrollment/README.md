# Course enrollment (DCB example)

A small, from-scratch showcase of Occurrent's Dynamic Consistency Boundary (DCB) support, written in Kotlin with Spring Boot and the decider pattern. It is deliberately a skeleton: the wiring and the events are done, and the domain logic is left for you behind TODOs.

## Why this example

Enrolling a student in a course has to hold two rules at once, and they live on two different entities:

- a course has a seat capacity (a course rule), and
- a student may take at most a fixed number of courses (a student rule).

A classic aggregate owns a single entity, so holding both rules at once usually means a saga or a read-then-write race. DCB lets one conditional append span both the course and the student, so the decision stays atomic and consistent without either of those.

## How it maps to Occurrent

- Events carry DCB tags. The enroll and unenroll events are tagged with BOTH the course and the student, which is what makes the boundary cross both entities.
- A command reads a DCB query (the decision boundary), a decider folds that into state and decides, and the application service appends the result conditionally and retries on a conflict.
- The Spring Boot starter auto-configures the `DcbApplicationService` and the DCB DSL from the beans in `Bootstrap.kt`.

## What is already done for you

- The domain events in `CourseEnrollmentEvent.kt` and the policy constant.
- The Spring Boot wiring in `Bootstrap.kt`, the CloudEvent converter, the type mapper, and `application.yml`.
- The DCB tag names in `CourseEnrollmentDcbTags.kt`.
- The decider commands and the use cases that drive the decider through the DCB DSL.

## Where to start

Work through the TODOs in this order. Each one is marked in the code.

1. `CourseEnrollmentEventTagGenerator`: map each event to its tags. The enroll and unenroll events get both boundaries, this is the key modeling step.
2. `CourseEnrollmentDcbQueries`: build the queries, above all the cross-entity enrollment boundary.
3. `CourseEnrollmentDecider`: design the state, then implement `evolve` and `decide`. The enrollment invariants are the whole point of the example.
4. Write a test that fills a course to capacity and asserts the next enrollment is rejected, and that a student cannot exceed the course limit. A skeleton test is provided under `src/test`.

## Running

DCB uses MongoDB transactions, so it needs a replica set (a single-node one is fine). The test uses Testcontainers and needs no setup. To run the app yourself, point `spring.data.mongodb.uri` at a replica set.
