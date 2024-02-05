package org.occurrent.eventstore.jpa;

public interface CloudEventDaoTraits {
  String streamId();

  long streamRevision();
}
