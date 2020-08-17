package se.haleby.occurrent.example.eventstore.mongodb.spring.subscriptionprojections;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.springframework.stereotype.Component;
import se.haleby.occurrent.domain.DomainEvent;

import static se.haleby.occurrent.functional.CheckedFunction.unchecked;

@Component
public class DeserializeCloudEventToDomainEvent {

    private final ObjectMapper objectMapper;

    public DeserializeCloudEventToDomainEvent(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public DomainEvent deserialize(CloudEvent cloudEvent) {
        return unchecked((CloudEvent e) -> (DomainEvent) objectMapper.readValue(e.getData(), Class.forName(e.getType()))).apply(cloudEvent);
    }

}
