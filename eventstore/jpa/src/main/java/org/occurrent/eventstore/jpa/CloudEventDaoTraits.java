package org.occurrent.eventstore.jpa;

public interface CloudEventDaoTraits<TKey> {
  TKey key();
  void setKey(TKey key);
  String streamId();

  long streamRevision();
}
