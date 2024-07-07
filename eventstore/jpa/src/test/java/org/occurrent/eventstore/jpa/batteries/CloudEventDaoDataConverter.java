package org.occurrent.eventstore.jpa.batteries;

import static org.occurrent.eventstore.jpa.utils.Sneaky.sneaky;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import java.util.Map;

@Converter(autoApply = true)
public class CloudEventDaoDataConverter implements AttributeConverter<Map<String, Object>, String> {
  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public String convertToDatabaseColumn(Map<String, Object> o) {
    return sneaky(() -> mapper.writeValueAsString(o));
  }

  @Override
  public Map<String, Object> convertToEntityAttribute(String s) {
    return sneaky(() -> mapper.readValue(s, new TypeReference<>() {}));
  }
}
