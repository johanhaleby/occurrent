package se.haleby.occurrent.domain;

import java.time.LocalDateTime;
import java.util.Objects;

public class NameDefined implements DomainEvent {

    private final LocalDateTime time;
    private final String name;

    public NameDefined(LocalDateTime time, String name) {
        this.time = time;
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public LocalDateTime getTime() {
        return time;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NameDefined)) return false;
        NameDefined that = (NameDefined) o;
        return Objects.equals(time, that.time) &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(time, name);
    }

    @Override
    public String toString() {
        return "NameDefined{" +
                "time=" + time +
                ", name='" + name + '\'' +
                '}';
    }
}