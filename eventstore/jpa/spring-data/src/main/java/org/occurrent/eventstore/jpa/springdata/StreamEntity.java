package org.occurrent.eventstore.jpa.springdata;

import jakarta.persistence.*;

import java.util.UUID;


@Entity
@Table(name = "event_stream",
        indexes = {
                @Index(name = "idx_event_stream_name", columnList = "name", unique = true)
        })
public class StreamEntity {

    @Id
    @GeneratedValue
    @Column(name = "id", nullable = false)
    private UUID id;

    @Column(name = "name", nullable = false, updatable = false)
    private String name;

    @Column(name = "version", nullable = false)
    private Long version;

    public UUID getId() {
        return id;
    }

    public void setId(UUID aId) {
        id = aId;
    }

    public String getName() {
        return name;
    }

    public void setName(String aName) {
        name = aName;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long aVersion) {
        version = aVersion;
    }
}
