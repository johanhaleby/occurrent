package org.occurrent.subscription.blocking.competingconsumers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoLeaseCompetingConsumerStrategy;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

@Testcontainers
class CompetingConsumerTwoNodeLeaseRaceTest {
    @Container
    private static final MongoDBContainer mongo = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private CompetingConsumerSubscriptionModel nodeA, nodeB;
    private MongoClient client;
    private SpringMongoEventStore eventStore;
    private final AtomicBoolean writing = new AtomicBoolean(true);

    @AfterEach
    void shutdown() {
        writing.set(false);
        try { if (nodeA != null) nodeA.shutdown(); } catch (Exception ignored) { }
        try { if (nodeB != null) nodeB.shutdown(); } catch (Exception ignored) { }
        if (client != null) client.close();
    }

    record Delivery(String node, String eventId, long nanos, String lockOwner, boolean nodeThinksItHolds) { }

    @Test
    void two_nodes_that_stop_start_pause_and_shut_down_lose_no_event_and_never_deliver_at_once_or_without_the_lease() throws Exception {
        ConnectionString cs = new ConnectionString(mongo.getReplicaSetUrl() + ".events");
        client = MongoClients.create(cs);
        MongoTemplate template = new MongoTemplate(client, requireNonNull(cs.getDatabase()));
        template.getDb().getCollection("events").drop();
        eventStore = new SpringMongoEventStore(template, new EventStoreConfig.Builder().eventStoreCollectionName("events")
                .transactionConfig(new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(client, requireNonNull(cs.getDatabase()))))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING).build());
        String locks = "locks-" + UUID.randomUUID();
        String checkpoints = "checkpoints-" + UUID.randomUUID();
        Duration lease = Duration.ofMillis(1000);
        SpringMongoLeaseCompetingConsumerStrategy strategyA = new SpringMongoLeaseCompetingConsumerStrategy.Builder(template).leaseTime(lease).collectionName(locks).build();
        SpringMongoLeaseCompetingConsumerStrategy strategyB = new SpringMongoLeaseCompetingConsumerStrategy.Builder(template).leaseTime(lease).collectionName(locks).build();
        nodeA = new CompetingConsumerSubscriptionModel(new DurableSubscriptionModel(spring(template), new SpringMongoCheckpointStorage(template, checkpoints), strategyA::fencingToken), strategyA);
        nodeB = new CompetingConsumerSubscriptionModel(new DurableSubscriptionModel(spring(template), new SpringMongoCheckpointStorage(template, checkpoints), strategyB::fencingToken), strategyB);

        CopyOnWriteArrayList<Delivery> deliveries = new CopyOnWriteArrayList<>();
        AtomicInteger inHandler = new AtomicInteger();
        AtomicBoolean forcedExpiryWindow = new AtomicBoolean(false);
        CopyOnWriteArrayList<String> overlapsOutsideForcedWindow = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<Delivery> unleasedOutsideForcedWindow = new CopyOnWriteArrayList<>();

        java.util.function.BiFunction<String, SpringMongoLeaseCompetingConsumerStrategy, java.util.function.Consumer<CloudEvent>> handler = (node, strategy) -> e -> {
            int now = inHandler.incrementAndGet();
            try {
                Document lock = template.getDb().getCollection(locks).find(Filters.eq("_id", "X")).first();
                String owner = lock == null ? "none" : lock.getString("subscriberId");
                Delivery d = new Delivery(node, e.getId(), System.nanoTime(), owner, strategy.hasLock("X", node));
                deliveries.add(d);
                if (now > 1 && !forcedExpiryWindow.get()) overlapsOutsideForcedWindow.add(node + ":" + e.getId());
                if (!node.equals(owner) && !forcedExpiryWindow.get()) unleasedOutsideForcedWindow.add(d);
                Thread.sleep(3);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            } finally {
                inHandler.decrementAndGet();
            }
        };

        nodeA.subscribe("A", "X", null, StartAt.subscriptionModelDefault(), handler.apply("A", strategyA)).waitUntilStarted();
        nodeB.subscribe("B", "X", null, StartAt.subscriptionModelDefault(), handler.apply("B", strategyB));

        List<String> written = new CopyOnWriteArrayList<>();
        Thread writer = new Thread(() -> {
            while (writing.get()) {
                written.add(write());
                try { Thread.sleep(10); } catch (InterruptedException ex) { return; }
            }
        });
        writer.start();
        List<String> log = new ArrayList<>();

        // The holder stops, gives up the lease and the other node takes over, then the stopped node starts again
        Thread.sleep(700);
        log.add("1: stop A");
        nodeA.stop();
        Thread.sleep(2500);
        log.add("1: start(true) A");
        nodeA.start(true);
        Thread.sleep(1000);
        log.add("1: stop B");
        nodeB.stop();
        Thread.sleep(2500);
        log.add("1: start(false) B");
        nodeB.start(false);
        Thread.sleep(1000);

        // The holder's lease expires in MongoDB, as it does when the holder stalls, and the user pauses the holder while the other node takes the lease.
        // A holder whose lease expired keeps delivering until it notices, so deliveries inside these windows are not checked
        log.add("2: start(true) B -> " + tryIt(() -> nodeB.start(true)));
        Thread.sleep(1000);
        Random random = new Random();
        for (int i = 0; i < 6; i++) {
            String holder = strategyA.hasLock("X", "A") ? "A" : (strategyB.hasLock("X", "B") ? "B" : "none");
            CompetingConsumerSubscriptionModel h = holder.equals("B") ? nodeB : nodeA;
            forcedExpiryWindow.set(true);
            template.getDb().getCollection(locks).updateOne(Filters.eq("_id", "X"), Updates.set("expiresAt", new Date(0)));
            Thread.sleep(random.nextInt(700));
            log.add("2." + i + ": holder=" + holder + " pause -> " + tryIt(() -> h.pauseSubscription("X")));
            Thread.sleep(1200);
            forcedExpiryWindow.set(false);
            log.add("2." + i + ": resume -> " + tryIt(() -> h.resumeSubscription("X")));
            Thread.sleep(800);
        }

        // Both nodes stop, then start at the same moment
        log.add("3: stop both");
        nodeA.stop();
        nodeB.stop();
        Thread.sleep(500);
        CyclicBarrier barrier = new CyclicBarrier(2);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        Future<?> fa = pool.submit(() -> { barrier.await(); nodeA.start(true); return null; });
        Future<?> fb = pool.submit(() -> { barrier.await(); nodeB.start(true); return null; });
        fa.get(10, SECONDS);
        fb.get(10, SECONDS);
        log.add("3: both started");
        Thread.sleep(2000);

        // The holder shuts down while events are written
        String holder = strategyA.hasLock("X", "A") ? "A" : (strategyB.hasLock("X", "B") ? "B" : "none");
        log.add("4: holder=" + holder + ", shutting it down");
        if (holder.equals("A")) {
            nodeA.shutdown();
            nodeA = null;
        } else {
            nodeB.shutdown();
            nodeB = null;
        }
        Thread.sleep(2000);
        writing.set(false);
        writer.join();
        log.add("written=" + written.size());

        await().atMost(20, SECONDS).untilAsserted(() -> {
            Set<String> delivered = new HashSet<>();
            deliveries.forEach(d -> delivered.add(d.eventId()));
            assertThat(written).as("events written and never delivered, " + log).allMatch(delivered::contains);
        });
        assertThat(overlapsOutsideForcedWindow).as("events delivered on both nodes at once, outside the windows where the test expires the lease itself, " + log).isEmpty();
        assertThat(unleasedOutsideForcedWindow).as("events delivered by a node without the lease, outside the windows where the test expires the lease itself, " + log).isEmpty();
    }

    private static String tryIt(Runnable r) {
        try { r.run(); return "ok"; } catch (Exception e) { return e.getClass().getSimpleName() + ": " + e.getMessage(); }
    }

    private SpringMongoSubscriptionModel spring(MongoTemplate template) {
        return new SpringMongoSubscriptionModel(template, "events", TimeRepresentation.RFC_3339_STRING);
    }

    private String write() {
        NameDefined event = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.of(2026, 1, 1, 0, 0), "name", "value");
        eventStore.write(UUID.randomUUID().toString(), List.of(CloudEventBuilder.v1().withId(event.eventId()).withSource(URI.create("http://name"))
                .withType(NameDefined.class.getName()).withTime(toLocalDateTime(event.timestamp()).atOffset(UTC)).withSubject(event.name())
                .withDataContentType("application/json").withData(unchecked(new ObjectMapper()::writeValueAsBytes).apply(event)).build()));
        return event.eventId();
    }
}
