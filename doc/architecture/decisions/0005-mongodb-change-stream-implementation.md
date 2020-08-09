# 5. MongoDB change stream implementation

Date: 2020-08-09

## Status

Accepted

## Context

A ResumeToken for MongoDB change streams is only available once the first document has been written to MongoDB. This is a huge drawback
in the following scenario:

1. The application is started with a change streamer that listens to events.
1. This change streamer contains a bug which prevents it from being executed correctly (i.e. it throws an exception).
1. Since the ResumeToken will not be persisted in this case it's lost! When the application is restarted the change streamer will start at the current position and thus losing the event.

## Decision

To avoid this the change streamer must be a bit more complex. When a new change streamer is started it reads the operation time from MongoDB and persists it 
as the "stream position document" for the change streamer. This means that if the scenario described above where to happen the stream will be resumed from the 
operation time and _not_ a resume token. Once the first resume token is persisted the stream will use the resume token position instead.

Also see [stackoverflow](https://stackoverflow.com/questions/63323190/get-resume-token-with-mongodb-java-driver-before-first-document-received-in-chan).

## Consequences

This should have any noticeable effects for the end user but the implementation will be a bit more complex.  