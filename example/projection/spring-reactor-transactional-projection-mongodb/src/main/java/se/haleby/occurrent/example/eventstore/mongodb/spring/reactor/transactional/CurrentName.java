package se.haleby.occurrent.example.eventstore.mongodb.spring.reactor.transactional;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Objects;

@Document(collection = "current-name-projection")
@TypeAlias("CurrentName")
public class CurrentName {
    @Id
    private String id;
    private String name;

    @SuppressWarnings("unused")
    CurrentName() {
    }

    CurrentName(String id) {
        this(id, null);
    }

    public CurrentName(String id, String name) {
        this.id = id;
        this.name = name;
    }

    public String getId() {
        return id;
    }

    void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    void setName(String name) {
        this.name = name;
    }

    CurrentName changeName(String name) {
        setName(name);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CurrentName)) return false;
        CurrentName that = (CurrentName) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name);
    }

    @Override
    public String toString() {
        return "CurrentName{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}