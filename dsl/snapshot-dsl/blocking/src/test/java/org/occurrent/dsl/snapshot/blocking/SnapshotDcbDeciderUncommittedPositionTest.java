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

package org.occurrent.dsl.snapshot.blocking;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.service.blocking.dcb.GenericDcbApplicationService;
import org.occurrent.dsl.dcb.DcbDecider;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.dsl.snapshot.SnapshotOptions;
import org.occurrent.dsl.snapshot.SnapshotPolicy;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * Reproduction for issue 1230 through the snapshot decider service.
 * <p>
 * The deposit is appended without a condition and holds the position the store reserved for it, before its transaction
 * begins. A withdrawal runs through {@link SnapshotDcbDeciderApplicationService}, takes the next position, commits, and
 * saves a snapshot at that position. The deposit then commits at the lower position. A snapshot at a position the
 * deposit committed below hides the deposit from every later execute that resumes from it, so the balance is too low.
 */
@Testcontainers
@Timeout(120)
@DisplayName("SnapshotDcbDeciderUncommittedPosition")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SnapshotDcbDeciderUncommittedPositionTest {

    private static final URI SOURCE = URI.create("urn:test:snapshot-uncommitted-position");
    private static final String ACCOUNT_ID = "a1";
    private static final Tag ACCOUNT_TAG = Tag.of("account", ACCOUNT_ID);

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient lateAppenderClient;
    private MongoClient serviceClient;

    @AfterEach
    void close_clients() {
        if (lateAppenderClient != null) {
            lateAppenderClient.close();
        }
        if (serviceClient != null) {
            serviceClient.close();
        }
    }

    @Test
    void the_next_decision_after_a_snapshot_saved_past_a_late_committing_deposit_sees_the_deposit() throws Exception {
        // Given
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl());
        String databaseName = requireNonNull(connectionString.getDatabase());

        lateAppenderClient = MongoClients.create(connectionString);
        HoldingTransactionTemplate holdingTransaction = new HoldingTransactionTemplate(
                new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(lateAppenderClient, databaseName)));
        SpringMongoEventStore lateAppender = newEventStore(new MongoTemplate(lateAppenderClient, databaseName), holdingTransaction);

        serviceClient = MongoClients.create(connectionString);
        SpringMongoEventStore serviceEventStore = newEventStore(new MongoTemplate(serviceClient, databaseName),
                new TransactionTemplate(new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(serviceClient, databaseName))));
        var service = new SnapshotDcbDeciderApplicationService<>(new GenericDcbApplicationService<>(serviceEventStore, new AccountEventConverter()));
        var account = SnapshotDcbDecider.from(accountDecider(), SnapshotStore.<Integer>inMemory(), SnapshotOptions.<Integer, AccountEvent>of(1, SnapshotPolicy.always()));

        service.execute(new Deposit(10), account);

        // When
        CompletableFuture<Void> lateDeposit = CompletableFuture.runAsync(() ->
                lateAppender.append(List.of(DcbCloudEvents.withTags(new AccountEventConverter().toCloudEvent(new Deposited(5)), List.of(ACCOUNT_TAG)))));
        try {
            assertThat(holdingTransaction.held.await(30, TimeUnit.SECONDS))
                    .as("the late deposit did not reach its held transaction")
                    .isTrue();
            assertThat(serviceEventStore.currentPosition())
                    .as("the late deposit must have reserved its position before it was held")
                    .isEqualTo(2);

            var withdrawal = service.execute(new Withdraw(3), account);
            assertThat(withdrawal.orElseThrow().firstSequencePosition())
                    .as("the withdrawal must have reserved its position after the held late deposit")
                    .isEqualTo(3);

            holdingTransaction.release.countDown();
            lateDeposit.get(30, TimeUnit.SECONDS);
        } finally {
            holdingTransaction.release.countDown();
        }
        int balance = service.executeAndReturnState(new CheckBalance(), account);

        // Then
        assertThat(balance)
                .as("The balance is 10 deposited, 5 deposited and 3 withdrawn. The 5 deposit committed after the withdrawal saved its snapshot, so the snapshot hides it.")
                .isEqualTo(12);
    }

    private static SpringMongoEventStore newEventStore(MongoTemplate template, TransactionTemplate transactionTemplate) {
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(transactionTemplate)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        return new SpringMongoEventStore(template, config);
    }

    private static DcbDecider<AccountCommand, Integer, AccountEvent> accountDecider() {
        Decider<AccountCommand, Integer, AccountEvent> decider = new Decider<>() {
            @Override
            public Integer initialState() {
                return 0;
            }

            @NonNull
            @Override
            public List<AccountEvent> decide(@NonNull AccountCommand command, Integer state) {
                return switch (command) {
                    case Deposit deposit -> List.of(new Deposited(deposit.amount()));
                    case Withdraw withdraw -> List.of(new Withdrawn(withdraw.amount()));
                    case CheckBalance __ -> List.of();
                };
            }

            @Override
            public Integer evolve(Integer state, @NonNull AccountEvent event) {
                return switch (event) {
                    case Deposited deposited -> state + deposited.amount();
                    case Withdrawn withdrawn -> state - withdrawn.amount();
                };
            }
        };
        DcbCriteria boundary = DcbCriteria.types("Deposited", "Withdrawn").tags(ACCOUNT_TAG);
        return DcbDecider.from(decider, command -> boundary, event -> Set.of(ACCOUNT_TAG));
    }

    private sealed interface AccountCommand {
    }

    private record Deposit(int amount) implements AccountCommand {
    }

    private record Withdraw(int amount) implements AccountCommand {
    }

    private record CheckBalance() implements AccountCommand {
    }

    private sealed interface AccountEvent {
        int amount();
    }

    private record Deposited(int amount) implements AccountEvent {
    }

    private record Withdrawn(int amount) implements AccountEvent {
    }

    private static final class AccountEventConverter implements CloudEventConverter<AccountEvent> {
        @Override
        public CloudEvent toCloudEvent(AccountEvent domainEvent) {
            return CloudEventBuilder.v1()
                    .withId(UUID.randomUUID().toString())
                    .withSource(SOURCE)
                    .withType(getCloudEventType(domainEvent))
                    .withTime(OffsetDateTime.now())
                    .withData(Integer.toString(domainEvent.amount()).getBytes(UTF_8))
                    .build();
        }

        @Override
        public AccountEvent toDomainEvent(CloudEvent cloudEvent) {
            int amount = Integer.parseInt(new String(requireNonNull(cloudEvent.getData()).toBytes(), UTF_8));
            return switch (cloudEvent.getType()) {
                case "Deposited" -> new Deposited(amount);
                case "Withdrawn" -> new Withdrawn(amount);
                default -> throw new IllegalArgumentException("Unknown type " + cloudEvent.getType());
            };
        }

        @Override
        public String getCloudEventType(Class<? extends AccountEvent> type) {
            return type.getSimpleName();
        }
    }

    /**
     * A {@link TransactionTemplate} that holds its first {@code execute} before the transaction begins, which is after the
     * store has reserved the position block, and runs every later one normally.
     */
    private static final class HoldingTransactionTemplate extends TransactionTemplate {
        private final transient CountDownLatch held = new CountDownLatch(1);
        private final transient CountDownLatch release = new CountDownLatch(1);
        private final transient AtomicBoolean holdNext = new AtomicBoolean(true);

        HoldingTransactionTemplate(MongoTransactionManager transactionManager) {
            super(transactionManager);
        }

        @Override
        public <T> T execute(TransactionCallback<T> action) {
            if (holdNext.compareAndSet(true, false)) {
                held.countDown();
                try {
                    if (!release.await(30, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Timed out waiting to be released");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
            return super.execute(action);
        }
    }
}
