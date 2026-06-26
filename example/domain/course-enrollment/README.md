# Course enrollment (DCB example)

A small, from-scratch showcase of Occurrent's Dynamic Consistency Boundary (DCB) support, written in Kotlin with Spring Boot and the decider pattern..

## Why this example

Enrolling a student in a course has to hold two rules at once, and they live on two different entities:

- a course has a seat capacity (a course rule), and
- a student may take at most a fixed number of courses (a student rule).

A classic aggregate owns a single entity, so holding both rules at once usually means a saga or a read-then-write race. DCB lets one conditional append span both the course and the student, so the decision stays atomic and consistent without either of those.

## How it maps to Occurrent

- Events carry DCB tags. The enroll and unenroll events are tagged with BOTH the course and the student, which is what makes the boundary cross both entities.
- A command reads a DCB query (the decision boundary), a decider folds that into state and decides, and the application service appends the result conditionally and retries on a conflict.
- The Spring Boot starter auto-configures the `DcbApplicationService` and the DCB DSL from the beans in `Bootstrap.kt`.


## Running

DCB uses MongoDB transactions, so it needs a replica set (a single-node one is fine). The test uses Testcontainers and needs no setup. To run the app yourself, point `spring.data.mongodb.uri` at a replica set.
