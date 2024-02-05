package org.occurrent.eventstore.jpa.batteries;

import io.cloudevents.SpecVersion;
import io.cloudevents.lang.Nullable;
import jakarta.persistence.*;
import java.net.URI;
import java.time.Instant;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.occurrent.eventstore.jpa.CloudEventDaoTraits;
import org.springframework.data.annotation.Id;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

@Entity
@Table(name = "cloud_events")
@EntityListeners(AuditingEntityListener.class)
@Getter
@Setter
@AllArgsConstructor
@Builder
@Accessors(fluent = true)
public class CloudEventDao implements CloudEventDaoTraits {
//  @org.springframework.data.annotation.Id
  @jakarta.persistence.Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private long id;

  private long streamRevision;
  private String streamId;
  private UUID eventUuid;
  private URI source;
  private String type;
  @Nullable private Instant timestamp;
  @Nullable private String subject;
  @Nullable private String dataContentType;
  private byte[] data;
  @Nullable private URI dataSchema;
  private SpecVersion specVersion;
}
