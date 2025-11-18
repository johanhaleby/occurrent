package org.occurrent.eventstore.jpa.springdata;

import jakarta.persistence.*;

import java.util.UUID;

@Entity
class CloudEventAttributeEntity {

    @Id
    @Column(name = "id", nullable = false)
    private UUID id;

    @ManyToOne
    @JoinColumn(name = "cloud_event_id", nullable = false, updatable = false)
    private CloudEventEntity cloudEvent;

    @Column(name = "attribute_name", nullable = false, updatable = false)
    private String attributeName;

    @Column(name = "attribute_value", nullable = false, updatable = false)
    private String attributeValue;

}
