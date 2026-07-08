/*
 * Copyright 2026 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.example.domain.appointmentscheduling;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.javalin.Javalin;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.blocking.dcb.DcbApplicationService;
import org.occurrent.application.service.blocking.dcb.GenericDcbApplicationService;
import org.occurrent.application.service.dcb.TagGenerator;
import org.occurrent.application.service.dcb.annotation.AnnotationTagGenerator;
import org.occurrent.dsl.dcb.blocking.DcbDomainEventQueries;
import org.occurrent.dsl.query.blocking.DomainEventQueries;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.example.domain.appointmentscheduling.application.AppointmentSchedulingService;
import org.occurrent.example.domain.appointmentscheduling.application.SchedulingQueries;
import org.occurrent.example.domain.appointmentscheduling.event.DomainEvent;
import org.occurrent.example.domain.appointmentscheduling.web.WebApi;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;

import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * Wires the example without Spring: a native-driver MongoDB event store with the DCB capability, a Jackson 3
 * cloud event converter, an annotation-based tag generator, the DCB application service, and a Javalin server.
 * <p>
 * DCB appends use a multi-document transaction, so the MongoDB connection must be a replica set.
 */
public final class Bootstrap {
    private static final String DATABASE_NAME = "appointment-scheduling";
    private static final String EVENTS_COLLECTION_NAME = "events";
    private static final URI SOURCE = URI.create("urn:occurrent:domain:appointmentscheduling");

    private final Javalin javalin;
    private final MongoClient mongoClient;

    private Bootstrap(Javalin javalin, MongoClient mongoClient) {
        this.javalin = javalin;
        this.mongoClient = mongoClient;
    }

    public static void main(String[] args) {
        Bootstrap application = bootstrap("mongodb://localhost:27017", 7000);
        Runtime.getRuntime().addShutdownHook(new Thread(application::shutdown));
    }

    public static Bootstrap bootstrap(String connectionString, int port) {
        MongoClient mongoClient = MongoClients.create(connectionString);

        EventStoreConfig config = new EventStoreConfig.Builder()
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        MongoEventStore eventStore = new MongoEventStore(mongoClient, DATABASE_NAME, EVENTS_COLLECTION_NAME, config);

        CloudEventConverter<DomainEvent> converter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), SOURCE)
                .typeMapper(ReflectionCloudEventTypeMapper.simple(DomainEvent.class))
                .idMapper(event -> event.eventId().toString())
                .timeMapper(DomainEvent::occurredAt)
                .build();
        TagGenerator<DomainEvent> tagGenerator = new AnnotationTagGenerator<>();
        DcbApplicationService<DomainEvent> applicationService = new GenericDcbApplicationService<>(eventStore, converter, tagGenerator);

        AppointmentSchedulingService service = new AppointmentSchedulingService(applicationService);
        DcbDomainEventQueries<DomainEvent> dcbQueries = new DcbDomainEventQueries<>(new DomainEventQueries<>(eventStore, converter));
        SchedulingQueries queries = new SchedulingQueries(dcbQueries);

        Javalin javalin = Javalin.create(cfg -> cfg.showJavalinBanner = false).start(port);
        WebApi.configureRoutes(javalin, service, queries);
        return new Bootstrap(javalin, mongoClient);
    }

    public void shutdown() {
        javalin.stop();
        mongoClient.close();
    }
}
