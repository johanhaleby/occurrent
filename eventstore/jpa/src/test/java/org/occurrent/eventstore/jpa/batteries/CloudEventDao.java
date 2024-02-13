package org.occurrent.eventstore.jpa.batteries;

import io.cloudevents.SpecVersion;
import io.cloudevents.lang.Nullable;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Table;
import java.net.URI;
import java.time.Instant;
import java.util.Map;
import lombok.*;
import lombok.experimental.Accessors;
import org.occurrent.eventstore.jpa.CloudEventDaoTraits;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

@Entity
@Table(name = "cloud_events")
@EntityListeners(AuditingEntityListener.class)
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Accessors(fluent = true)
public class CloudEventDao implements CloudEventDaoTraits<Long> {
  //  @org.springframework.data.annotation.Id
  @jakarta.persistence.Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private long id;

  @Column(name = "stream_revision")
  private long streamRevision;

  @Column(name = "stream_id")
  private String streamId;

  @Column(name = "event_id")
  private String eventId;

  // TODO - remove all @Transient tags and make sure the tests pass with however we store the data
  // attribute
  @Convert(converter = URIConverter.class)
  private URI source;

  private String type;

  private Instant timestamp;

  @Nullable private String subject;

  @Column(name = "data_content_type")
  @Nullable
  private String dataContentType = "";

  //  @Type(JsonType.class)
  @Convert(converter = CloudEventDaoDataConverter.class)
  @Column(columnDefinition = "jsonb")
  private Map<String, Object> data;

  @Column(name = "data_schema")
  @Nullable
  @Convert(converter = URIConverter.class)
  private URI dataSchema;

  @Column(name = "spec_version")
  private SpecVersion specVersion = SpecVersion.V03;

  @Override
  public Long key() {
    return id;
  }

  @Override
  public void setKey(Long key) {
    this.id = key;
  }
}
