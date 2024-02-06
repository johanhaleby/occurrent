package org.occurrent.eventstore.jpa.batteries;

import io.cloudevents.SpecVersion;
import io.cloudevents.lang.Nullable;
import jakarta.persistence.*;
import java.net.URI;
import java.time.Instant;
import java.util.UUID;
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
public class CloudEventDao implements CloudEventDaoTraits {
  //  @org.springframework.data.annotation.Id
  @jakarta.persistence.Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private long id;

  @Column(name = "stream_revision")
  private long streamRevision;

  @Column(name = "stream_id")
  private String streamId;

  @Column(name = "event_uuid")
  private UUID eventUuid;

  // TODO - remove all @Transient tags and make the design actually good.
  @Transient private URI source;
  @Transient private String type;

  @Transient @Nullable private Instant timestamp;
  @Transient @Nullable private String subject;

  @Column(name = "data_content_type")
  @Transient
  @Nullable
  private String dataContentType;

  @Transient private String data;

  @Transient
  @Column(name = "data_schema")
  @Nullable
  private URI dataSchema;

  @Transient
  @Column(name = "spec_version")
  private SpecVersion specVersion;
}
