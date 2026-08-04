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

package org.occurrent.subscription.mongodb.spring.blocking;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.occurrent.tck.subscription.blocking.SubscriptionModelConformance;
import org.occurrent.tck.subscription.blocking.SubscriptionModelFixture;
import org.springframework.data.mongodb.core.MongoTemplate;

/**
 * Not the in-process suite: {@link SpringMongoSubscriptionModel} watches a MongoDB change stream, which is
 * asynchronous by nature, so only the store-backed conformance suite applies here.
 */
class SpringMongoSubscriptionModelConformanceTest extends SubscriptionModelConformance {

    private static final String DATABASE = "springsubscriptionconformance";


    // One client and one template for the class, since standing a client up means server discovery. What has to be
    // fresh per test is the event collection, which the fixture takes care of.
    private static MongoClient mongoClient;
    private static MongoTemplate mongoTemplate;

    @BeforeAll
    static void connect() {
        mongoClient = MongoClients.create(SharedMongoDBContainer.replicaSetUrl(DATABASE));
        mongoTemplate = new MongoTemplate(mongoClient, DATABASE);
    }

    @AfterAll
    static void disconnect() {
        mongoClient.close();
    }

    @Override
    protected SubscriptionModelFixture createFixture() {
        return new SpringMongoSubscriptionModelFixture(mongoClient, mongoTemplate, DATABASE);
    }
}
