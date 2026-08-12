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

package org.occurrent.subscription.redis.spring.blocking;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Redis Cluster refuses a script whose keys hash to different slots, which is what
 * {@link SpringRedisCheckpointStorage#versionKey(String)}'s hash tag exists to prevent.
 * <p>
 * This is a slot-computation test, not a Cluster integration test, because no test container in this module runs
 * Cluster mode. Every Redis test here, {@code SpringRedisCheckpointStorageTest}, its conformance twin, and
 * {@code FlushRedisExtension}, wires a single-node {@code GenericContainer<>("redis:5.0.3-alpine")}, and standing up
 * a multi-node Cluster (several nodes exchanging gossip before they answer a single request) needs its own
 * multi-container topology, unlike anything else in this repository's Redis coverage, so it is not something a
 * single Testcontainers container gives you cheaply. Verifying the invariant against an independent implementation
 * of Cluster's own slot algorithm is the proportionate substitute. It catches exactly the defect a real Cluster
 * would, the checkpoint key and the version key landing in different slots, without that infrastructure.
 */
@DisplayNameGeneration(DisplayNameGenerator.Simple.class)
class SpringRedisCheckpointStorageClusterSlotTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "orders",
            "550e8400-e29b-41d4-a716-446655440000",
            "tenant-42:orders-projection",
            "no-braces-at-all-1234567890",
            "{tenant-42}-orders",
            "orders-{tenant-42}",
            "a{b{c}d}e"
    })
    void checkpoint_key_and_version_key_hash_to_the_same_cluster_slot(String subscriptionId) {
        String versionKey = SpringRedisCheckpointStorage.versionKey(subscriptionId);

        assertThat(clusterSlot(versionKey)).isEqualTo(clusterSlot(subscriptionId));
    }

    /**
     * Documents, rather than works around, the one shape the class javadoc names, a subscription id whose only
     * closing brace has no opening brace before it anywhere in the string. Cluster hashes such an id on its whole,
     * untagged text, and no hash tag built around that text can reproduce the same slot without introducing a
     * closing brace of its own that Cluster would find first, so the mismatch below is Cluster's own hash-tag rule,
     * not a gap in {@code versionKey}. Confirmed against an independent Python implementation of the same CRC16 and
     * tag-extraction rules before this test was written, so the recorded expectation is not a guess.
     */
    @ParameterizedTest
    @ValueSource(strings = {"orders}v2", "a}b{c"})
    void a_stray_closing_brace_with_no_opening_brace_before_it_is_the_one_shape_the_hash_tag_cannot_reproduce(String subscriptionId) {
        String versionKey = SpringRedisCheckpointStorage.versionKey(subscriptionId);

        assertThat(clusterSlot(versionKey)).isNotEqualTo(clusterSlot(subscriptionId));
    }

    // Redis Cluster's own slot algorithm (Cluster specification, "Keys hash tags"): hash the substring between the
    // first '{' and the first '}' after it, or the whole key when either brace is missing or none of the key sits
    // between them. Written independently of SpringRedisCheckpointStorage's own extraction so this test is not
    // tautological with the production code it is checking.
    private static int clusterSlot(String key) {
        return crc16(hashTag(key)) & 0x3FFF;
    }

    private static String hashTag(String key) {
        int openBrace = key.indexOf('{');
        if (openBrace < 0) {
            return key;
        }
        int closeBrace = key.indexOf('}', openBrace + 1);
        if (closeBrace < 0 || closeBrace == openBrace + 1) {
            return key;
        }
        return key.substring(openBrace + 1, closeBrace);
    }

    // CRC16/XMODEM (polynomial 0x1021, initial value 0x0000, no reflection), the variant the Cluster specification's
    // reference implementation uses. Cross-checked against the standard test vector, crc16("123456789") == 0x31C3.
    private static int crc16(String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        int crc = 0x0000;
        for (byte b : bytes) {
            crc ^= (b & 0xFF) << 8;
            for (int i = 0; i < 8; i++) {
                crc = (crc & 0x8000) != 0 ? (crc << 1) ^ 0x1021 : crc << 1;
                crc &= 0xFFFF;
            }
        }
        return crc;
    }
}
