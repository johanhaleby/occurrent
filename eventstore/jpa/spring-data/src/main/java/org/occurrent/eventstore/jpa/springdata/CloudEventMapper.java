package org.occurrent.eventstore.jpa.springdata;

import io.cloudevents.CloudEvent;
import org.springframework.stereotype.Component;

@Component
public class CloudEventMapper {

    public CloudEventEntity toEntity(CloudEvent aEvent) {
        if (aEvent == null) {
            return null;
        }
        CloudEventEntity res = new CloudEventEntity();
        res.setEventId(aEvent.getId());
        res.setSpecVersion(aEvent.getSpecVersion());
        res.setType(aEvent.getType());
        res.setSource(aEvent.getSource());
        res.setDataContentType(aEvent.getDataContentType());
        res.setDataSchema(aEvent.getDataSchema());
        res.setSubject(aEvent.getSubject());
        res.setTime(aEvent.getTime());
        if (aEvent.getData() != null) {
            res.setData(aEvent.getData().toBytes());
        }
        return res;
    }

}
