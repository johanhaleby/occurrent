package org.occurrent.eventstore.jpa;

import io.cloudevents.CloudEvent;

public interface CloudEventConverter<T> {
  T toDao(long streamVersion, String streamId, CloudEvent e);

  CloudEvent toCloudEvent(T t);
}
