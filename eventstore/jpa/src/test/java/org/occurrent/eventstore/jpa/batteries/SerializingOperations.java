package org.occurrent.eventstore.jpa.batteries;

import static org.occurrent.eventstore.jpa.utils.Sneaky.sneaky;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import lombok.val;
import org.occurrent.domain.DomainEvent;

public record SerializingOperations(ObjectMapper mapper) {
  public static final SerializingOperations defaultInstance =
      new SerializingOperations(new ObjectMapper());

  private static final TypeReference<HashMap<String, Object>> typeRef = new TypeReference<>() {};

  private HashMap<String, Object> toMap(Object o) {
    return sneaky(() -> mapper.readValue(mapper.writeValueAsString(o), typeRef));
  }

  public byte[] domainEventToBytes(DomainEvent d) {
    val map = toMap(d);
    map.put("type", d.getClass().getSimpleName());
    return sneaky(() -> mapper.writeValueAsBytes(map));
  }

  public HashMap<String, Object> bytesToJson(byte[] bytes) {
    return sneaky(() -> mapper.readValue(bytes, typeRef));
  }

  public byte[] jsonToBytes(Map<String, Object> o) {
    return sneaky(() -> mapper.writeValueAsBytes(o));
  }
}
