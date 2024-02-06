package org.occurrent.eventstore.jpa;

import static org.assertj.core.api.Assertions.assertThat;

import io.cloudevents.CloudEvent;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.blocking.EventStream;
import org.occurrent.eventstore.jpa.utils.TestHelper;

abstract class JpaBlockingEventStoreTest_Base {
  protected abstract TestHelper testHelper();

  @Test
  void does_not_change_event_store_content_when_writing_an_empty_stream_of_events() {
    // When
    testHelper().persist("name", Stream.empty());

    // Then
    EventStream<CloudEvent> eventStream = testHelper().eventStore().read("name");

    assertThat(eventStream.isEmpty()).isTrue();
  }
}
