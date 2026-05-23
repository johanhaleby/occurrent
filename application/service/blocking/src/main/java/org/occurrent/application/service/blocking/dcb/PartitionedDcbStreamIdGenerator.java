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

package org.occurrent.application.service.blocking.dcb;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.CRC32;

import static java.util.Objects.requireNonNull;

/**
 * Places DCB-written events into a fixed number of Occurrent storage streams.
 * <p>
 * The generator hashes a canonical form of the boundary tags and uses jump consistent
 * hashing to keep stream placement stable when callers use the same partition count
 * and prefix.
 */
public class PartitionedDcbStreamIdGenerator implements DcbStreamIdGenerator {
    private final int partitions;
    private final String prefix;

    /**
     * Creates a generator with 64 partitions and the {@code dcb:partition:} stream prefix.
     */
    public PartitionedDcbStreamIdGenerator() {
        this(64, "dcb:partition:");
    }

    /**
     * Creates a generator with an explicit partition count and stream prefix.
     */
    public PartitionedDcbStreamIdGenerator(int partitions, String prefix) {
        if (partitions <= 0) {
            throw new IllegalArgumentException("Partitions must be greater than zero");
        }
        this.partitions = partitions;
        this.prefix = requireNonNull(prefix, "Prefix cannot be null");
    }

    /**
     * Generates the storage stream id for the supplied boundary tags.
     */
    @Override
    public String generateStreamId(Set<String> boundaryTags) {
        requireNonNull(boundaryTags, "Boundary tags cannot be null");
        String canonicalTags = String.join("|", new TreeSet<>(boundaryTags));
        return prefix + jumpConsistentHash(hash(canonicalTags), partitions);
    }

    private static long hash(String value) {
        CRC32 crc32 = new CRC32();
        crc32.update(value.getBytes(StandardCharsets.UTF_8));
        long crc = crc32.getValue();
        return (crc << 32) ^ (crc * 0x9E3779B97F4A7C15L);
    }

    static int jumpConsistentHash(long key, int buckets) {
        long b = -1;
        long j = 0;
        while (j < buckets) {
            b = j;
            key = key * 2862933555777941757L + 1;
            j = (long) ((b + 1) * (1L << 31) / (double) ((key >>> 33) + 1));
        }
        return (int) b;
    }
}
