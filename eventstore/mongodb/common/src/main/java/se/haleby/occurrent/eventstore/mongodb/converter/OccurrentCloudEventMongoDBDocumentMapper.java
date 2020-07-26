package se.haleby.occurrent.eventstore.mongodb.converter;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import org.bson.Document;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Date;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import static se.haleby.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_ID;

public class OccurrentCloudEventMongoDBDocumentMapper {

    static DateTimeFormatter RFC_3339_DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .append(ISO_LOCAL_DATE_TIME)
            .optionalStart()
            .appendOffset("+HH:MM", "Z")
            .optionalEnd()
            .toFormatter();

    public static Document convertToDocument(EventFormat eventFormat, String streamId, CloudEvent cloudEvent) {
        byte[] bytes = eventFormat.serialize(cloudEvent);
        String serializedAsString = new String(bytes, UTF_8);
        Document cloudEventDocument = Document.parse(serializedAsString);

        // Convert date string to a date in order to be able to perform date/time queries
        // on the "time" property name
        ZonedDateTime time = cloudEvent.getTime();
        if (time != null) {
            Date date = Date.from(time.toInstant());
            cloudEventDocument.put("time", date);
        }

        cloudEventDocument.put(STREAM_ID, streamId);
        return cloudEventDocument;
    }

    public static CloudEvent convertToCloudEvent(EventFormat eventFormat, Document cloudEventDocument) {
        Document document = new Document(cloudEventDocument);
        document.remove("_id");
        Date time = document.getDate("time");

        if (time != null) {
            ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(time.toInstant(), UTC);
            String format = RFC_3339_DATE_TIME_FORMATTER.format(zonedDateTime);
            document.put("time", format);
        }

        String eventJsonString = document.toJson();
        byte[] eventJsonBytes = eventJsonString.getBytes(UTF_8);
        return eventFormat.deserialize(eventJsonBytes);
    }
}