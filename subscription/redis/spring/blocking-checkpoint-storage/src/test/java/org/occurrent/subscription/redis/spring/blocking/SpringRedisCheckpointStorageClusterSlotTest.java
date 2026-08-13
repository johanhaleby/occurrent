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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * single Testcontainers container gives you cheaply. Verifying the invariant against a second implementation of
 * Cluster's own slot algorithm is the proportionate substitute. It catches exactly the defect a real Cluster
 * would, the checkpoint key and the version key landing in different slots, without that infrastructure. The CRC16
 * half of that second implementation is genuinely independent, production has no CRC16 of its own to share a bug
 * with, and is pinned against the standard test vector below. The hash-tag half restates the same Cluster
 * specification production's {@code clusterHashTag} also implements, using a regex match rather than index
 * scanning so a bug specific to one style of scanning is not shared by both sides of a comparison.
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
     * Documents, rather than works around, the one shape the class javadoc names, a subscription id where Cluster
     * itself falls back to hashing the whole id (no brace pair, an unmatched brace, or an empty pair like
     * {@code {}}) and that whole id is either empty or contains a closing brace somewhere in it. Two of these
     * values have no opening brace before their closing one at all ({@code "orders}v2"}, {@code "a}b{c"}), and four
     * have an opening brace immediately before it, forming an empty tag ({@code "{}orders"}, {@code "{}v2"},
     * {@code "orders{}v2"}, {@code "{}"}). Cluster hashes every one of them on its whole, untagged text, and no
     * hash tag built around that text can reproduce the same slot without introducing a closing brace of its own
     * that Cluster would find first, so the mismatch below is Cluster's own hash-tag rule, not a gap in
     * {@code versionKey}. Confirmed against an independent Python implementation of the same CRC16 and
     * tag-extraction rules before this test was written, so the recorded expectation is not a guess.
     */
    @ParameterizedTest
    @ValueSource(strings = {"orders}v2", "a}b{c", "{}orders", "{}v2", "orders{}v2", "{}"})
    void a_cluster_hash_tag_that_comes_out_empty_or_still_contains_a_closing_brace_cannot_be_reproduced(String subscriptionId) {
        String versionKey = SpringRedisCheckpointStorage.versionKey(subscriptionId);

        assertThat(clusterSlot(versionKey)).isNotEqualTo(clusterSlot(subscriptionId));
    }

    /**
     * Asserts the exact wrapped string, not just the slot it hashes to. An id landing in the whole-key fallback for
     * one reason (an unmatched brace) and one landing there for another (an empty pair like {@code {}}) can both
     * still fail the slot-equality tests above for reasons unrelated to that specific fallback, so those tests
     * alone cannot tell a correct fallback from a broken one. Each fallback branch must wrap the id unchanged and
     * whole, hashed as a SHA-256 digest rather than kept as a substring, and this checks that directly.
     */
    @Test
    void version_key_wraps_a_sha256_digest_of_the_whole_subscription_id_whenever_cluster_would_fall_back_to_hashing_it_whole() {
        assertThat(SpringRedisCheckpointStorage.versionKey("orders"))
                .isEqualTo("occurrent:checkpoint-version:{orders}" + sha256Hex("orders"));
        assertThat(SpringRedisCheckpointStorage.versionKey("{tenant-42}-orders"))
                .isEqualTo("occurrent:checkpoint-version:{tenant-42}" + sha256Hex("{tenant-42}-orders"));
        assertThat(SpringRedisCheckpointStorage.versionKey("{}orders"))
                .isEqualTo("occurrent:checkpoint-version:{{}orders}" + sha256Hex("{}orders"));
        assertThat(SpringRedisCheckpointStorage.versionKey("a}b{c"))
                .isEqualTo("occurrent:checkpoint-version:{a}b{c}" + sha256Hex("a}b{c"));
    }

    /**
     * The pair that broke a plain-separator version key ({@code "{" + tag + "}:" + id}) during the first
     * adversarial review round. {@code "a}:{a"} falls back to hashing itself whole, so its own text doubles as
     * both its tag and its copy, and {@code "{a}:a}:{a"} genuinely extracts the tag {@code "a"}. Both landed on
     * the identical bytes, {@code "{a}:{a}:a}:{a"}, a shared fencing version between two different subscriptions.
     * A SHA-256 digest of the id, not a copy of the id itself, is what keeps them apart now.
     */
    @Test
    void the_pair_that_broke_a_plain_separator_version_key_now_gets_distinct_ones() {
        assertThat(SpringRedisCheckpointStorage.versionKey("a}:{a"))
                .isNotEqualTo(SpringRedisCheckpointStorage.versionKey("{a}:a}:{a"));
    }

    /**
     * The pair that broke a length-prefixed version key ({@code "{" + tag + "}" + len(id) + ":" + id}) during the
     * second adversarial review round, the length prefix closing the first round's gap without closing the
     * underlying one. {@code "a}12:{a"} falls back to hashing itself whole again, and {@code "{a}7:a}12:{a"}
     * genuinely extracts the tag {@code "a"} with a length of 12. Both landed on the identical bytes,
     * {@code "{a}12:{a}7:a}12:{a"}.
     */
    @Test
    void the_pair_that_broke_a_length_prefixed_version_key_now_gets_distinct_ones() {
        assertThat(SpringRedisCheckpointStorage.versionKey("a}12:{a"))
                .isNotEqualTo(SpringRedisCheckpointStorage.versionKey("{a}7:a}12:{a"));
    }

    // Calls the same JDK digest algorithm SpringRedisCheckpointStorage.sha256Hex does, trusted rather than
    // reimplemented the way crc16 below is, since what this test checks is where the digest lands in the version
    // key and what it is computed over, not whether the JDK's SHA-256 is correct.
    private static String sha256Hex(String s) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(s.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Two subscription ids sharing a hash tag are exactly what a Cluster deployment is expected to have, tenant
     * scoping by {@code "{tenant}"} is the documented reason hash tags exist. The tag alone is not a safe version
     * key, since two such ids would then read and write the same fencing version as each other. The SHA-256 digest
     * after the tag is what keeps them apart. Each case here appends {@code "-other"} after the id's own closing
     * brace, so {@code clusterHashTag} extracts the identical tag for both. Confirmed independently before this
     * test was written that without a digest of the full id in the key, these pairs would collide.
     */
    @ParameterizedTest
    @ValueSource(strings = {"{tenant}-orders", "orders-{tenant}", "a{b{c}d}e"})
    void two_ids_sharing_a_hash_tag_still_get_distinct_version_keys(String subscriptionId) {
        String sameTagOtherId = subscriptionId + "-other";

        assertThat(SpringRedisCheckpointStorage.versionKey(subscriptionId))
                .isNotEqualTo(SpringRedisCheckpointStorage.versionKey(sameTagOtherId));
    }

    /**
     * A key with an empty brace pair before a non-empty one, {@code "{}{tenant}"}, is the case an earlier,
     * unanchored version of {@link #hashTag(String)} got wrong. It matched past the empty pair to the valid one
     * after it and returned {@code "tenant"}, while {@code clusterHashTag} stops at the first pair it finds,
     * empty or not, and falls back to the whole key. None of the parameterized cases above exercise this, since
     * none of them has a valid pair sitting after an empty one.
     */
    @Test
    void hash_tag_extraction_falls_back_to_the_whole_key_when_an_earlier_brace_pair_is_empty_even_though_a_later_one_is_not() {
        assertThat(hashTag("{}{tenant}")).isEqualTo("{}{tenant}");
    }

    // Redis Cluster's own slot algorithm (Cluster specification, "Keys hash tags"): hash the substring between the
    // first '{' and the first '}' after it, or the whole key when either brace is missing or none of the key sits
    // between them.
    private static int clusterSlot(String key) {
        return crc16(hashTag(key)) & 0x3FFF;
    }

    // A regex match, not the index-scanning SpringRedisCheckpointStorage.clusterHashTag uses, so a bug specific to
    // one style of scanning is not shared by both sides of the comparisons above. Anchored with "^[^{]*", not a
    // bare find(), because an unanchored "\\{([^}]+)\\}" would skip past a leading empty pair like "{}" and match
    // a later one instead, "{}{tenant}" would come back "tenant" when Cluster, finding only an empty tag in the
    // first pair, hashes the whole key. The prefix before the first '{' can only be non-'{' characters, which
    // pins the match to that first brace specifically, and the whole match failing (no non-empty pair right there)
    // falls back to the whole key exactly as production does.
    private static String hashTag(String key) {
        Matcher matcher = Pattern.compile("^[^{]*\\{([^}]+)\\}").matcher(key);
        return matcher.find() ? matcher.group(1) : key;
    }

    /**
     * Every slot assertion in this class rests on {@link #crc16(String)} matching Cluster's own CRC16, so a subtly
     * wrong polynomial or initial value here would validate the production mapping against the wrong model and
     * every test above could still pass. This is the standard CRC16/XMODEM test vector, not a guess.
     */
    @Test
    void crc16_matches_the_standard_test_vector() {
        assertThat(crc16("123456789")).isEqualTo(0x31C3);
    }

    // CRC16/XMODEM (polynomial 0x1021, initial value 0x0000, no reflection), the variant the Cluster specification's
    // reference implementation uses.
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
