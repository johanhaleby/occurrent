# 26. Rename @Subscription to @StreamSubscription and deprecate the old name

Date: 2026-06-27

## Status

Accepted

## Context

There is one declarative subscription annotation, `@Subscription`, and it drives a stream subscription: it derives a `Filter.type(...)` from the method's event type and starts at a time. A declarative DCB subscription annotation is coming next (ADR 0027). With both in place, a bare `@Subscription` would be ambiguous about which kind it means, and the pair would be lopsided: `@Subscription` and `@DcbSubscription` instead of `@StreamSubscription` and `@DcbSubscription`.

`@Subscription` is released, so it cannot be hard-renamed without breaking existing code. Java annotations also cannot alias or meta-annotate each other, so the two names have to be two annotation types, and anything that reads them has to know both.

## Decision

Make `@StreamSubscription` the canonical name and keep `@Subscription` as a deprecated alias.

- Add `@StreamSubscription` with the same attributes and the same nested `StartPosition`, `ResumeBehavior`, and `StartupMode` enums as `@Subscription`. It is the name new code should use.
- Mark `@Subscription` `@Deprecated(forRemoval = true)` with Javadoc pointing to `@StreamSubscription`. Keep its attributes and nested enums frozen, so existing code and existing stored references keep compiling and behaving exactly as before.
- `OccurrentAnnotationBeanPostProcessor` scans each method for both annotations, rejects a method that carries both, and normalizes whichever it finds into one internal definition that the existing stream subscription logic consumes. The deprecated annotation's enum values map to the canonical ones by name, since the constants are identical.
- Migrate the example applications to `@StreamSubscription`, so the examples show the canonical name.

The enums are duplicated for the length of the deprecation, once on each annotation. That is the cost of keeping the old annotation a frozen, self-contained, backward-compatible type rather than having the canonical annotation reference types nested in the deprecated one. When `@Subscription` is eventually removed, its enums go with it and `@StreamSubscription` keeps its own.

Do not extract the backend-agnostic processor helpers (parameter binding, start-position storage checks, startup-mode decision) yet. They are extracted in the `@DcbSubscription` work, where a second consumer actually exists, so the seams are drawn against a real second caller rather than guessed.

## Consequences

- The annotation pair is symmetric and unambiguous once `@DcbSubscription` lands: `@StreamSubscription` for stream subscriptions, `@DcbSubscription` for DCB ones.
- Existing `@Subscription` code keeps working, now with a deprecation warning that points at the new name.
- The processor carries a small normalization for the two annotation types and the duplicated enums until `@Subscription` is removed.
