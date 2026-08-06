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

package org.occurrent.example.projection.dsl.dcbjava;

import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.dsl.dcb.blocking.DcbDomainEventQueries;
import org.occurrent.dsl.projection.DcbProjection;
import org.occurrent.dsl.projection.blocking.DcbProjectionRunner;
import org.occurrent.dsl.query.blocking.DomainEventQueries;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.example.projection.dsl.dcbjava.CouponRedemption.isCouponRedeemedProjection;

@DisplayNameGeneration(ReplaceUnderscores.class)
class CouponRedemptionProjectionTest {

    private InMemorySubscriptionModel subscriptionModel;
    private InMemoryEventStore eventStore;
    private CloudEventConverter<CouponEvent> converter;

    @BeforeEach
    void setup() {
        subscriptionModel = new InMemorySubscriptionModel();
        eventStore = new InMemoryEventStore(subscriptionModel);
        converter = new JacksonCloudEventConverter.Builder<CouponEvent>(new ObjectMapper(), URI.create("urn:occurrent:example:projection-dsl"))
                .typeMapper(ReflectionCloudEventTypeMapper.simple(CouponEvent.class))
                .idMapper(event -> java.util.UUID.randomUUID().toString())
                .build();
    }

    @AfterEach
    void shutdown() {
        subscriptionModel.shutdown();
    }

    @Test
    void dcb_push_projection_materializes_the_tag_scoped_flag() {
        ConcurrentHashMap<String, Boolean> store = new ConcurrentHashMap<>();
        ViewStateRepository<Boolean, String> repository = ViewStateRepository.create(store::get, store::put);

        DcbProjectionRunner.create(subscriptionModel, converter)
                .project("coupon-redeemed", isCouponRedeemedProjection("SAVE10"), repository);

        append("coupon:SAVE10", new CouponIssued("SAVE10"));
        append("coupon:SAVE10", new CouponRedeemed("SAVE10", "order-1"));

        // A single-instance projection keys its one slot by the subscription id, not the tagged coupon code.
        subscriptionModel.waitUntilAllEventsProcessed();
        assertThat(store.get("coupon-redeemed")).isTrue();
    }

    @Test
    void dcb_pull_projection_folds_the_tag_scoped_events_on_demand() {
        append("coupon:SAVE10", new CouponIssued("SAVE10"));
        append("coupon:SAVE10", new CouponRedeemed("SAVE10", "order-1"));

        DcbProjection<Boolean, CouponEvent, String> projection = isCouponRedeemedProjection("SAVE10");
        DcbDomainEventQueries<CouponEvent> queries = new DcbDomainEventQueries<>(new DomainEventQueries<>(eventStore, converter));

        // The pull equivalent of the runner: fold the events matching the projection's criteria into its view.
        boolean redeemed = projection.projection().view().evolve(queries.query(projection.criteria()));

        assertThat(redeemed).isTrue();
    }

    @Test
    void dcb_pull_projection_is_the_initial_state_for_an_unredeemed_coupon() {
        append("coupon:SAVE10", new CouponIssued("SAVE10"));

        DcbProjection<Boolean, CouponEvent, String> projection = isCouponRedeemedProjection("SAVE10");
        DcbDomainEventQueries<CouponEvent> queries = new DcbDomainEventQueries<>(new DomainEventQueries<>(eventStore, converter));

        boolean redeemed = projection.projection().view().evolve(queries.query(projection.criteria()));

        assertThat(redeemed).isFalse();
    }

    private void append(String tag, CouponEvent... events) {
        List<Tag> tags = List.of(Tag.parse(tag));
        List<CloudEvent> cloudEvents = converter.toCloudEvents(List.of(events)).stream()
                .map(cloudEvent -> DcbCloudEvents.withTags(cloudEvent, tags))
                .toList();
        eventStore.append(cloudEvents);
    }
}
