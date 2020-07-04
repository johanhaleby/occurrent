package se.haleby.occurrent.domain;

import java.time.LocalDateTime;
import java.util.Objects;

public class NameWasChanged implements DomainEvent {

    private final LocalDateTime time;
    private final String name;

    public NameWasChanged(LocalDateTime time, String nameChangedTo) {
        this.time = time;
        this.name = nameChangedTo;
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
        if (!(o instanceof NameWasChanged)) return false;
        NameWasChanged that = (NameWasChanged) o;
        return Objects.equals(time, that.time) &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(time, name);
    }

    @Override
    public String toString() {
        return "NameWasChanged{" +
                "time=" + time +
                ", name='" + name + '\'' +
                '}';
    }
}