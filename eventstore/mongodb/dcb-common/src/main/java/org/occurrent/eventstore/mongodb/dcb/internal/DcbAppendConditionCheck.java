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

package org.occurrent.eventstore.mongodb.dcb.internal;

import com.mongodb.MongoCommandException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadConcernLevel;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import org.bson.Document;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken.MarkerObservation;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbCriterion;
import org.occurrent.eventstore.api.dcb.internal.DcbMarkerVersionsSupport;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * The check of a conditional append with a consistency token on the MongoDB stores (ADR 149). It compares what the read
 * saw of each marker with the markers inside the append's transaction, and says whether that settles the condition or
 * the store has to look at the events.
 */
@NullMarked
public final class DcbAppendConditionCheck {

    private static final int COMMAND_NOT_FOUND = 59;

    private DcbAppendConditionCheck() {
    }

    /**
     * What a store's deployment answers to {@code hello}.
     */
    public enum Deployment {
        /**
         * A replica set.
         */
        REPLICA_SET,
        /**
         * A mongos, or a load balancer in front of one.
         */
        MONGOS,
        /**
         * Anything else, such as a standalone server.
         */
        OTHER
    }

    /**
     * What the check may assume about the append's transaction and the token's read.
     */
    public enum Scope {
        /**
         * The transaction reads the markers and the events from one snapshot, and a read whose token holds a marker
         * from after an event holds the event (ADR 149).
         */
        REPLICA_SET,
        /**
         * The transaction reads every shard at one point in time, but a token's markers can come from different points
         * in time (ADR 175).
         */
        SNAPSHOT_THROUGH_MONGOS,
        /**
         * Neither holds, so every marker that moved refuses the append.
         */
        MARKERS_ONLY;

        /**
         * The scope of an append transaction with {@code options} on {@code deployment}.
         *
         * @param deployment the store's deployment
         * @param options    the options the append's transaction started with, {@code null} when they couldn't be read
         */
        public static Scope of(Deployment deployment, @Nullable TransactionOptions options) {
            return switch (deployment) {
                case REPLICA_SET -> REPLICA_SET;
                case MONGOS -> readsEveryShardAtOnePointInTime(options) ? SNAPSHOT_THROUGH_MONGOS : MARKERS_ONLY;
                case OTHER -> MARKERS_ONLY;
            };
        }

        /**
         * The scope of the append's transaction on {@code deployment}, reading its options only through a mongos, where
         * they decide the scope. A failure to read them gives {@link #MARKERS_ONLY} there.
         *
         * @param deployment the store's deployment
         * @param options    reads the options the append's transaction started with, {@code null} when there are none
         */
        public static Scope reading(Deployment deployment, Supplier<@Nullable TransactionOptions> options) {
            if (deployment != Deployment.MONGOS) {
                return of(deployment, null);
            }
            TransactionOptions read;
            try {
                read = options.get();
            } catch (RuntimeException | LinkageError e) {
                read = null;
            }
            return of(deployment, read);
        }
    }

    /**
     * The settings a conditional append's own transaction runs with through a mongos, {@code options} with read concern
     * snapshot and write concern majority. MongoDB promises one point in time across the shards only for a transaction
     * that commits at majority. The write concern keeps the {@code wtimeout} and the journal flag of the one in
     * {@code options}, or of {@code clientWriteConcern} when {@code options} has none, since that's what the transaction
     * would otherwise commit with.
     *
     * @param options            the store's transaction options
     * @param clientWriteConcern the write concern of the client the store starts its sessions on
     */
    public static TransactionOptions snapshotThroughMongos(TransactionOptions options, WriteConcern clientWriteConcern) {
        WriteConcern configured = options.getWriteConcern() == null ? requireNonNull(clientWriteConcern, "clientWriteConcern cannot be null") : options.getWriteConcern();
        return TransactionOptions.merge(TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT).writeConcern(configured.withW("majority")).build(), options);
    }

    /**
     * The options the Spring stores start their own conditional append's transaction again with through a mongos, so it
     * reads every shard at one point in time. That's {@code running} with read concern snapshot and write concern
     * majority, keeping the {@code wtimeout} and the journal flag of its write concern. Empty when {@code running}
     * already has that pair.
     *
     * @param running the options the transaction runs with, as its session reports them
     */
    public static Optional<TransactionOptions> restartAtOnePointInTime(TransactionOptions running) {
        if (readsEveryShardAtOnePointInTime(running)) {
            return Optional.empty();
        }
        // A session reports the write concern it fills in from the client, so the fallback isn't used
        return Optional.of(snapshotThroughMongos(running, WriteConcern.ACKNOWLEDGED));
    }

    private static boolean readsEveryShardAtOnePointInTime(@Nullable TransactionOptions options) {
        if (options == null || options.getReadConcern() == null || options.getWriteConcern() == null) {
            return false;
        }
        return options.getReadConcern().getLevel() == ReadConcernLevel.SNAPSHOT && "majority".equals(options.getWriteConcern().getWObject());
    }

    /**
     * What the markers say about a condition.
     */
    public sealed interface Outcome {
    }

    /**
     * The markers show no event matching the criteria committed after the read.
     */
    public record Pass() implements Outcome {
    }

    /**
     * The markers can't rule out a matching event committed after the read, and the events can't settle it.
     */
    public record Refuse() implements Outcome {
    }

    /**
     * The store has to look for a matching event above these versions.
     *
     * @param items the criteria items the markers didn't settle
     */
    public record QueryEvents(List<ItemQuery> items) implements Outcome {
    }

    /**
     * The events of {@code item} to look for.
     *
     * @param item       the criteria item
     * @param selections the marker keys that select them, each with the version the read saw
     */
    public record ItemQuery(DcbCriterion item, List<KeySelection> selections) {
    }

    /**
     * An event is selected by an {@code mv} element with this key, the epoch of the marker and a version above this one.
     */
    public record KeySelection(String key, long version) {
    }

    /**
     * Compares {@code current}, the markers read inside the append's transaction, with what {@code token} saw.
     *
     * @param token            the token of the condition
     * @param criteria         the criteria of the condition
     * @param current          the marker observation of every marker key of {@code criteria}
     * @param scope            what the check may assume, see {@link Scope#of(Deployment, TransactionOptions)}
     * @throws IllegalArgumentException if the token doesn't hold exactly the marker keys of {@code criteria}, which means
     *                                  this store's read didn't create it
     */
    public static Outcome check(DcbConsistencyToken token, DcbCriteria criteria, Map<String, MarkerObservation> current, Scope scope) {
        Set<String> keys = DcbMarkerVersionsSupport.queryMarkerKeys(criteria);
        Map<String, MarkerObservation> read = token.markers();
        if (!read.keySet().equals(keys)) {
            throw new IllegalArgumentException("The consistency token holds the marker keys " + read.keySet() + " but the criteria have the marker keys " + keys + ", so this event store's read didn't create it");
        }
        boolean moved = false;
        for (String key : keys) {
            MarkerObservation atRead = read.get(key);
            MarkerObservation now = Objects.requireNonNull(current.get(key), () -> "No observation of marker key " + key);
            if (saw(atRead) && (now.version() < atRead.version() || !Objects.equals(atRead.epoch(), now.epoch()))) {
                return new Refuse();
            }
            moved |= moved(atRead, now);
        }
        if (!moved) {
            return new Pass();
        }
        if (scope == Scope.MARKERS_ONLY || criteria instanceof DcbCriteria.MatchAll) {
            return new Refuse();
        }
        List<ItemQuery> items = new ArrayList<>();
        for (DcbCriterion item : DcbMarkerVersionsSupport.items(criteria)) {
            List<String> movedKeys = DcbMarkerVersionsSupport.queryMarkerKeys(item).stream().filter(key -> moved(read.get(key), current.get(key))).toList();
            if (ruledOutByTheMarkers(item, movedKeys, scope)) {
                continue;
            }
            for (String key : movedKeys) {
                if (!eventsSelectableAbove(read.get(key), current.get(key))) {
                    return new Refuse();
                }
            }
            List<String> selectionKeys = selectsOnEveryMovedKey(item, scope) ? movedKeys : DcbMarkerVersionsSupport.selectionKeys(item);
            items.add(new ItemQuery(item, selectionKeys.stream().map(key -> new KeySelection(key, read.get(key).version())).toList()));
        }
        return items.isEmpty() ? new Pass() : new QueryEvents(items);
    }

    /**
     * The deployment the store runs on, asked once when the store is created. A mongos answers {@code msg} isdbgrid, also
     * through a load balancer, and a replica set member answers a {@code setName}. Every failure except a server that
     * doesn't know {@code hello} propagates.
     *
     * @param runCommand runs a command against the store's database
     */
    public static Deployment deployment(Function<Document, Document> runCommand) {
        Document answer = Objects.requireNonNull(helloOrIsMaster(runCommand), "The server sent no answer to hello or isMaster");
        if ("isdbgrid".equals(answer.get("msg"))) {
            return Deployment.MONGOS;
        }
        return answer.get("setName") != null ? Deployment.REPLICA_SET : Deployment.OTHER;
    }

    // An old server, such as MongoDB 4.2.8, doesn't know hello. The Spring templates wrap the driver's exception, so the
    // causes are walked.
    private static Document helloOrIsMaster(Function<Document, Document> runCommand) {
        try {
            return runCommand.apply(new Document("hello", 1));
        } catch (RuntimeException e) {
            Throwable cause = e;
            for (int hops = 0; cause != null && hops < 64; cause = cause.getCause(), hops++) {
                if (cause instanceof MongoCommandException commandException && commandException.getErrorCode() == COMMAND_NOT_FOUND) {
                    return runCommand.apply(new Document("isMaster", 1));
                }
            }
            throw e;
        }
    }

    private static boolean saw(MarkerObservation atRead) {
        return atRead.version() > 0;
    }

    private static boolean moved(MarkerObservation atRead, MarkerObservation now) {
        return atRead.version() != now.version();
    }

    // A matching event increments every tag marker of an item with tags, so one marker that didn't move rules it out. It
    // increments one of the type markers of an item without tags, so that item needs one that moved. The check doesn't
    // read the events for this, so it also holds for a writer that writes no mv. Through a mongos the token's unmoved
    // marker can be from after an event the read missed, so only an item none of whose markers moved is ruled out.
    private static boolean ruledOutByTheMarkers(DcbCriterion item, List<String> movedKeys, Scope scope) {
        if (selectsOnEveryMovedKey(item, scope)) {
            return movedKeys.isEmpty();
        }
        return movedKeys.size() < item.tags().size();
    }

    // Every tag marker of an item with tags moved on a replica set, so its first tag key finds a matching event. Through a
    // mongos a matching event the read missed sits above the token's version only on the keys whose marker moved.
    private static boolean selectsOnEveryMovedKey(DcbCriterion item, Scope scope) {
        return item.tags().isEmpty() || scope == Scope.SNAPSHOT_THROUGH_MONGOS;
    }

    // A marker without an epoch was last written by 0.34, so its events may have no mv. The epoch hasn't changed since the
    // read when the read saw the marker. A u that moved means a writer that writes no mv incremented the marker after the
    // read.
    private static boolean eventsSelectableAbove(MarkerObservation atRead, MarkerObservation now) {
        return now.epoch() != null && atRead.unstamped() == now.unstamped();
    }
}
