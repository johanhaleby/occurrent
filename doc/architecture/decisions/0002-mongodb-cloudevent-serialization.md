# 2. MongoDB CloudEvent serialization

Date: 2020-07-12

## Status

Pending

## Context

Currently, Occurrent is doing "unnecessary" work when converting from a `CloudEvent` to `Document` and vice versa
See [issue 196](https://github.com/cloudevents/sdk-java/issues/196) in the cloud event java sdk project.     

## Decision

None yet

## Consequences

Keep it as it is, which means slower serialization/de-serialization.

One idea would be to create a custom serializer and check the content-type provided by the user when creating the `CloudEvent`. 
If JSON then convert the data to a `Document` using `Document.parse(..)`? Not optimal though...