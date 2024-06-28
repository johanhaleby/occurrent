package org.occurrent.eventstore.jpa.batteries;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import java.net.URI;

@Converter(autoApply = true)
public class URIConverter implements AttributeConverter<URI, String> {

  @Override
  public String convertToDatabaseColumn(URI uri) {
    if (uri == null) {
      return null;
    }
    return uri.toString();
  }

  @Override
  public URI convertToEntityAttribute(String uri) {
    if (uri == null) {
      return null;
    }
    return URI.create(uri);
  }
}
