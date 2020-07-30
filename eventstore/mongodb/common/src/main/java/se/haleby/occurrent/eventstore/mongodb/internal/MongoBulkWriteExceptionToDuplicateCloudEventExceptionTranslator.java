package se.haleby.occurrent.eventstore.mongodb.internal;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.WriteError;
import se.haleby.occurrent.eventstore.api.DuplicateCloudEventException;

import java.net.URI;

public class MongoBulkWriteExceptionToDuplicateCloudEventExceptionTranslator {

    public static DuplicateCloudEventException translateToDuplicateCloudEventException(MongoBulkWriteException e) {
        return e.getWriteErrors().stream()
                .filter(error -> error.getCode() == 11000)
                .map(WriteError::getMessage)
                .filter(errorMessage -> errorMessage.contains("{ id: \"") && errorMessage.contains(", source: \""))
                .map(errorMessage -> {
                    int idKeyStartIndex = errorMessage.indexOf("{ id: \"");
                    int idValueStartIndex = idKeyStartIndex + "{ id: \"".length();
                    int idValueEndIndex = errorMessage.indexOf("\"", idValueStartIndex);
                    String id = errorMessage.substring(idValueStartIndex, idValueEndIndex);

                    int sourceKeyStartIndex = errorMessage.indexOf(", source: \"", idValueEndIndex);
                    int sourceValueStartIndex = sourceKeyStartIndex + ", source: \"".length();
                    int sourceValueEndIndex = errorMessage.indexOf("\" }", sourceValueStartIndex);
                    String source = errorMessage.substring(sourceValueStartIndex, sourceValueEndIndex);

                    return new DuplicateCloudEventException(id, URI.create(source), e);
                })
                .findFirst()
                .orElse(new DuplicateCloudEventException(null, null, e));
    }
}