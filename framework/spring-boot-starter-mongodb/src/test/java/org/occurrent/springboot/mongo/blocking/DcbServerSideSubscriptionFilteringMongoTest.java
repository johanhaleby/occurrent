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

package org.occurrent.springboot.mongo.blocking;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.subscription.DcbSubscriptionFilter;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves that server-side change stream filtering via {@link DcbSubscriptionFilter} works end-to-end
 * in DCB-only mode. Each test subscribes the wired {@link SubscriptionModel} DIRECTLY with a
 * {@link DcbSubscriptionFilter} -- not via {@link org.occurrent.dsl.dcb.blocking.DcbSubscriptions},
 * whose in-process correctness check would mask a broken server-side match. A non-matching event
 * delivered here means the change stream $match stage is wrong.
 */
@DisplayName("DCB server-side subscription filtering (DCB-only mode)")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = DcbServerSideSubscriptionFilteringMongoTest.DcbOnlyApplication.class,
        properties = {
                "occurrent.event-store.capabilities=dcb",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:dcb-filter-test"
        }
)
@Import(DcbServerSideSubscriptionFilteringMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class DcbServerSideSubscriptionFilteringMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:dcb-filter-test");

    @Autowired
    private DcbEventStore dcbEventStore;

    @Autowired
    private SubscriptionModel subscriptionModel;

    @Autowired
    private CloudEventConverter<TestEvent> cloudEventConverter;

    @Test
    void type_filter_delivers_only_the_subscribed_type() {
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        String subscriptionId = "type-filter-" + UUID.randomUUID();

        subscriptionModel.subscribe(subscriptionId, DcbSubscriptionFilter.filter(DcbQuery.type("OrderPlaced")), StartAt.now(), received::add)
                .waitUntilStarted();

        CloudEvent shouldArrive = dcbEvent("OrderPlaced", List.of("order:1"));
        CloudEvent shouldNotArrive = dcbEvent("OrderCancelled", List.of("order:1"));
        appendRaw(shouldArrive, shouldNotArrive);

        await().atMost(ofSeconds(20)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(received).extracting(CloudEvent::getId).containsExactly(shouldArrive.getId()));
    }

    @Test
    void tags_all_of_filter_delivers_only_events_carrying_all_required_tags() {
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        String subscriptionId = "tags-filter-" + UUID.randomUUID();

        subscriptionModel.subscribe(subscriptionId, DcbSubscriptionFilter.filter(DcbQuery.tags("customer:42", "tenant:1")), StartAt.now(), received::add)
                .waitUntilStarted();

        CloudEvent bothTags = dcbEvent("OrderPlaced", List.of("customer:42", "tenant:1"));
        // A superset of the required tags must still match. This guards against the $all condition under-delivering,
        // which the in-process correctness floor could not recover since it only removes events, never adds them.
        CloudEvent supersetTags = dcbEvent("OrderPlaced", List.of("customer:42", "tenant:1", "region:eu"));
        CloudEvent onlyOneTag = dcbEvent("OrderPlaced", List.of("customer:42"));
        CloudEvent noMatchingTag = dcbEvent("OrderPlaced", List.of("tenant:1", "region:eu"));
        appendRaw(bothTags, supersetTags, onlyOneTag, noMatchingTag);

        await().atMost(ofSeconds(20)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(received).extracting(CloudEvent::getId).containsExactlyInAnyOrder(bothTags.getId(), supersetTags.getId()));
    }

    @Test
    void excluded_types_filter_excludes_the_specified_type() {
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        String subscriptionId = "excluded-types-filter-" + UUID.randomUUID();

        DcbQuery query = DcbQuery.tags(List.of("order:99")).excludingTypes(List.of("OrderDeleted"));
        subscriptionModel.subscribe(subscriptionId, DcbSubscriptionFilter.filter(query), StartAt.now(), received::add)
                .waitUntilStarted();

        CloudEvent included = dcbEvent("OrderPlaced", List.of("order:99"));
        CloudEvent excluded = dcbEvent("OrderDeleted", List.of("order:99"));
        appendRaw(included, excluded);

        await().atMost(ofSeconds(20)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(received).extracting(CloudEvent::getId).containsExactly(included.getId()));
    }

    @Test
    void or_filter_delivers_union_of_both_items() {
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        String subscriptionId = "or-filter-" + UUID.randomUUID();

        DcbQuery query = DcbQuery.anyOf(
                org.occurrent.eventstore.api.dcb.DcbQuery.type("OrderPlaced"),
                org.occurrent.eventstore.api.dcb.DcbQuery.tags(List.of("vip:true"))
        );
        subscriptionModel.subscribe(subscriptionId, DcbSubscriptionFilter.filter(query), StartAt.now(), received::add)
                .waitUntilStarted();

        CloudEvent byType = dcbEvent("OrderPlaced", List.of("order:1"));
        CloudEvent byTag = dcbEvent("CustomerUpgraded", List.of("vip:true"));
        CloudEvent noMatch = dcbEvent("OrderCancelled", List.of("order:2"));
        appendRaw(byType, byTag, noMatch);

        await().atMost(ofSeconds(20)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(received).extracting(CloudEvent::getId).containsExactlyInAnyOrder(byType.getId(), byTag.getId()));
    }

    // --- helpers ---

    // The type mapper derives the CloudEvent type from the TestEvent class, so override it explicitly here to give each
    // event the CloudEvent type the DCB query filters on. The subscription receives raw CloudEvents, so nothing reads
    // the event back through the converter.
    private CloudEvent dcbEvent(String type, List<String> tags) {
        TestEvent event = new TestEvent(UUID.randomUUID().toString(), new Date(), "user", type);
        CloudEvent ce = cloudEventConverter.toCloudEvents(Stream.of(event)).findFirst().orElseThrow();
        CloudEvent withType = CloudEventBuilder.v1(ce).withType(type).build();
        return DcbCloudEvents.withTags(withType, tags);
    }

    private void appendRaw(CloudEvent... events) {
        dcbEventStore.append(List.of(events));
    }

    // --- inner application and configuration classes ---

    @TestConfiguration(proxyBeanMethods = false)
    static class MongoDbContainerConfiguration {

        @Bean
        @ServiceConnection
        MongoDBContainer mongoDbContainer() {
            return new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReplicaSet();
        }
    }

    @SpringBootApplication
    @EnableOccurrent
    static class DcbOnlyApplication {

        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), SOURCE)
                    .typeMapper(typeMapper)
                    .idMapper(TestEvent::eventId)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }
    }

    record TestEvent(String eventId, Date timestamp, String userId, String name) {
    }
}
