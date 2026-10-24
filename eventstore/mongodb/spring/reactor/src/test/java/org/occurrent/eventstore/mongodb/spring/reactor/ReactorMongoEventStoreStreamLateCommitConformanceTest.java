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

package org.occurrent.eventstore.mongodb.spring.reactor;

import com.mongodb.ConnectionString;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.tck.eventstore.blocking.StreamLateCommitConformance;
import org.occurrent.tck.eventstore.blocking.EventStoreFixture;
import org.occurrent.tck.internal.scaffolding.ExpectedGuaranteeViolation;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

/**
 * The store reserves a position before its append commits, so these tests are expected to violate the guarantee until
 * https://github.com/johanhaleby/occurrent/issues/1297 is fixed.
 */
@Testcontainers
@ExpectedGuaranteeViolation(
        tests = {
                "a_reader_that_resumes_from_the_current_position_is_delivered_a_write_that_was_still_in_flight"
        },
        issue = "https://github.com/johanhaleby/occurrent/issues/1297")
class ReactorMongoEventStoreStreamLateCommitConformanceTest extends StreamLateCommitConformance {

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    /**
     * Empties the database before each test, which is how the fixture can promise the suite a store with no events in
     * it. An extension callback runs before the {@code @BeforeEach} that creates the fixture, so the order is right.
     */
    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    @Override
    protected EventStoreFixture createFixture() {
        return new ReactorMongoEventStoreConformanceFixture(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events"));
    }
}
