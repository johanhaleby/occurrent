package org.occurrent.eventstore.jpa.springdata;

import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.SpecVersion;
import io.cloudevents.core.data.BytesCloudEventData;
import jakarta.persistence.*;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_ID;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_VERSION;

@Entity
@Table(name = "cloud_event",
        indexes = {
                @Index(name = "IDX_cloud_event_stream", columnList = "stream_id"),
                @Index(name = "IDX_cloud_event_stream_position", columnList = "stream_position"),
                @Index(name = "U_cloud_event_event_id_source", columnList = "event_id, source", unique = true),
                @Index(name = "U_cloud_event_stream_position", columnList = "stream_id, stream_position", unique = true),
        })
class CloudEventEntity implements CloudEvent {

    @Id
    @GeneratedValue
    @Column(name = "id", nullable = false)
    private UUID id;

    @Column(name = "spec_version", nullable = false, updatable = false)
    @Enumerated(EnumType.STRING)
    private SpecVersion specVersion;

    @ManyToOne
    @JoinColumn(name = "stream_id", nullable = false, updatable = false)
    private StreamEntity stream;

    @Column(name = "stream_position", nullable = false, updatable = false)
    private Long streamPosition;

    @Column(name = "event_id", nullable = false, updatable = false)
    private String eventId;

    @Column(name = "source", nullable = false)
    private URI source;

    @Column(name = "subject")
    private String subject;

    @Column(name = "type", nullable = false)
    private String type;

    @Column(name = "time")
    private OffsetDateTime time;

    @Column(name = "data_content_type")
    private String dataContentType;

    @Column(name = "data_schema")
    private URI dataSchema;

    @Column(name = "data")
    private byte[] data;

    @OneToMany(mappedBy = "cloudEvent")
    @MapKey(name = "attributeName")
    private Map<String, CloudEventAttributeEntity> attributes;

    public UUID getPK() {
        return id;
    }

    public void setPK(UUID aId) {
        id = aId;
    }

    @Override
    public CloudEventData getData() {
        return BytesCloudEventData.wrap(data);
    }

    public void setData(byte[] aData) {
        data = aData;
    }

    @Override
    public SpecVersion getSpecVersion() {
        return specVersion;
    }

    public void setSpecVersion(SpecVersion aSpecVersion) {
        specVersion = aSpecVersion;
    }

    public StreamEntity getStream() {
        return stream;
    }

    public void setStream(StreamEntity stream) {
        this.stream = stream;
    }

    public Long getStreamPosition() {
        return streamPosition;
    }

    public void setStreamPosition(Long streamPosition) {
        this.streamPosition = streamPosition;
    }

    @Override
    public String getType() {
        return type;
    }

    public void setType(String aType) {
        type = aType;
    }

    @Override
    public String getId() {
        return eventId;
    }

    public void setEventId(String aEventId) {
        eventId = aEventId;
    }

    @Override
    public URI getSource() {
        return source;
    }

    public void setSource(URI aSource) {
        source = aSource;
    }

    @Override
    public String getDataContentType() {
        return dataContentType;
    }

    public void setDataContentType(String aDataContentType) {
        dataContentType = aDataContentType;
    }

    @Override
    public URI getDataSchema() {
        return dataSchema;
    }

    public void setDataSchema(URI aDataSchema) {
        dataSchema = aDataSchema;
    }

    @Override
    public String getSubject() {
        return subject;
    }

    public void setSubject(String aSubject) {
        subject = aSubject;
    }

    @Override
    public OffsetDateTime getTime() {
        return time;
    }

    public void setTime(OffsetDateTime aTime) {
        time = aTime;
    }

    @Override
    public Object getAttribute(String s) throws IllegalArgumentException {
        return null;
    }

    @Override
    public Object getExtension(String aName) {
        return switch (aName) {
            case STREAM_VERSION -> getStreamPosition();
            case STREAM_ID -> getStream().getName();
            default -> null;
        };
    }

    @Override
    public Set<String> getExtensionNames() {
        return Set.of(STREAM_ID, STREAM_VERSION);
    }
}
