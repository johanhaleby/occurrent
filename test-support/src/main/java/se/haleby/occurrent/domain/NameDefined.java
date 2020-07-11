package se.haleby.occurrent.domain;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.Objects;

import static se.haleby.occurrent.time.TimeConversion.toDate;

public class NameDefined implements DomainEvent {

    private String eventId;
    private Date timestamp;
    private String name;

    @SuppressWarnings("unused")
    NameDefined() {
    }

    public NameDefined(String eventId, Date timestamp, String name) {
        this.eventId = eventId;
        this.timestamp = timestamp;
        this.name = name;
    }

    public NameDefined(String eventId, LocalDateTime timestamp, String name) {
        this(eventId, toDate(timestamp), name);
    }

    @Override
    public String getEventId() {
        return eventId;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Date getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NameDefined)) return false;
        NameDefined that = (NameDefined) o;
        return Objects.equals(eventId, that.eventId) &&
                Objects.equals(timestamp, that.timestamp) &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, timestamp, name);
    }

    @Override
    public String toString() {
        return "NameDefined{" +
                "id='" + eventId + '\'' +
                ", timestamp=" + timestamp +
                ", name='" + name + '\'' +
                '}';
    }
}