/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.occurrent.eventstore.api.dcb;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.Example;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Tuple;
import net.jqwik.api.Provide;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins that the canonical string of criteria, which {@link DcbMarkerVersions} keeps, tells criteria apart exactly when
 * {@link DcbCriteriaIdentity#same} does, so a read can refuse versions saved from other criteria without refusing the
 * same criteria built another way.
 */
class DcbCriteriaIdentityPropertyTest {

    // Values with the characters the canonical form has to quote or that a naive join would confuse. Only a tab sits
    // between letters, since a criterion strips leading and trailing whitespace from its types and tags
    private static final List<String> VALUES = List.of("A", "B", "C", "a\"b", "c\\d", "e,f", "[g]", "{h}", "i:j", "k\tl", "m\"],[\"n", "A\",\"B");

    @Property(tries = 2000)
    boolean criteria_have_the_same_canonical_form_exactly_when_they_are_the_same(@ForAll("criteria") DcbCriteria first, @ForAll("criteria") DcbCriteria second) {
        boolean sameCanonical = DcbCriteriaIdentity.canonical(first).equals(DcbCriteriaIdentity.canonical(second));

        return sameCanonical == DcbCriteriaIdentity.same(first, second);
    }

    @Property(tries = 2000)
    void criteria_that_are_built_differently_but_hold_the_same_alternatives_are_the_same_and_have_the_same_canonical_form(@ForAll("specs") Spec spec, @ForAll long seed) {
        DcbCriteria plain = spec.build(new Random(0), Shape.PLAIN);
        DcbCriteria rebuilt = spec.build(new Random(seed), Shape.REBUILT);

        assertThat(DcbCriteriaIdentity.same(plain, rebuilt)).as("same(%s, %s)", plain, rebuilt).isTrue();
        assertThat(DcbCriteriaIdentity.canonical(rebuilt)).as("canonical of %s", rebuilt).isEqualTo(DcbCriteriaIdentity.canonical(plain));
    }

    @Example
    void types_given_together_are_not_the_same_as_the_same_types_given_as_alternatives() {
        DcbCriteria together = DcbCriteria.types("A", "B");
        DcbCriteria alternatives = DcbCriteria.anyOf(DcbCriteria.type("A"), DcbCriteria.type("B"));

        assertThat(DcbCriteriaIdentity.same(together, alternatives)).isFalse();
        assertThat(DcbCriteriaIdentity.canonical(together)).isNotEqualTo(DcbCriteriaIdentity.canonical(alternatives));
    }

    @Example
    void tags_are_not_the_same_as_the_same_tags_with_an_excluded_type() {
        Tag a = Tag.of("account", "a");
        DcbCriteria plain = DcbCriteria.tags(a);
        DcbCriteria excluding = DcbCriteria.tags(a).excludingTypes("Y");

        assertThat(DcbCriteriaIdentity.same(plain, excluding)).isFalse();
        assertThat(DcbCriteriaIdentity.canonical(plain)).isNotEqualTo(DcbCriteriaIdentity.canonical(excluding));
    }

    @Example
    void criteria_that_match_every_event_are_not_the_same_as_any_criterion() {
        DcbCriteria criterion = DcbCriteria.type("A");

        assertThat(DcbCriteriaIdentity.same(DcbCriteria.all(), criterion)).isFalse();
        assertThat(DcbCriteriaIdentity.canonical(DcbCriteria.all())).isNotEqualTo(DcbCriteriaIdentity.canonical(criterion));
    }

    @Example
    void one_value_that_looks_like_two_values_is_not_the_same_as_the_two_values() {
        assertThat(DcbCriteriaIdentity.canonical(DcbCriteria.types("A", "B"))).isNotEqualTo(DcbCriteriaIdentity.canonical(DcbCriteria.type("A\",\"B")));
        assertThat(DcbCriteriaIdentity.canonical(DcbCriteria.tags(Tag.of("A"), Tag.of("B")))).isNotEqualTo(DcbCriteriaIdentity.canonical(DcbCriteria.tags(Tag.of("A\",\"B"))));
        assertThat(DcbCriteriaIdentity.canonical(DcbCriteria.type("A").excludingTypes("B", "C")))
                .isNotEqualTo(DcbCriteriaIdentity.canonical(DcbCriteria.type("A").excludingTypes("B\",\"C")));
    }

    @Example
    void a_type_and_a_tag_with_the_same_value_are_not_the_same() {
        DcbCriteria type = DcbCriteria.type("A");
        DcbCriteria tag = DcbCriteria.tags(Tag.of("A"));

        assertThat(DcbCriteriaIdentity.same(type, tag)).isFalse();
        assertThat(DcbCriteriaIdentity.canonical(type)).isNotEqualTo(DcbCriteriaIdentity.canonical(tag));
    }

    @Provide
    Arbitrary<DcbCriteria> criteria() {
        return specs().flatMap(spec -> Arbitraries.longs().map(seed -> spec.build(new Random(seed), Shape.REBUILT)));
    }

    @Provide
    Arbitrary<Spec> specs() {
        Arbitrary<List<Alternative>> alternatives = alternatives().list().ofMinSize(1).ofMaxSize(4);
        return Arbitraries.frequencyOf(
                Tuple.of(1, Arbitraries.just(new Spec(true, List.of()))),
                Tuple.of(9, alternatives.map(list -> new Spec(false, list))));
    }

    private static Arbitrary<Alternative> alternatives() {
        Arbitrary<List<String>> types = Arbitraries.of(VALUES).list().ofMaxSize(2);
        Arbitrary<List<String>> tags = Arbitraries.of(VALUES).list().ofMaxSize(2);
        Arbitrary<List<String>> excluded = Arbitraries.of(VALUES).list().ofMaxSize(2);
        return Combinators.combine(types, tags, excluded).as(Alternative::new)
                .filter(Alternative::isValid);
    }

    private enum Shape {PLAIN, REBUILT}

    private record Alternative(List<String> types, List<String> tags, List<String> excludedTypes) {
        boolean isValid() {
            Set<String> typeSet = Set.copyOf(types);
            return !(typeSet.isEmpty() && tags.isEmpty()) && excludedTypes.stream().noneMatch(typeSet::contains);
        }

        DcbCriterion build(Random random, Shape shape) {
            List<String> types = ordered(this.types, random, shape);
            List<String> tags = ordered(this.tags, random, shape);
            List<String> excluded = ordered(this.excludedTypes, random, shape);
            return new DcbCriterion(new LinkedHashSet<>(types), new LinkedHashSet<>(tags.stream().map(Tag::of).toList()), new LinkedHashSet<>(excluded));
        }

        private static List<String> ordered(List<String> values, Random random, Shape shape) {
            List<String> copy = new ArrayList<>(values);
            if (shape == Shape.REBUILT) {
                Collections.shuffle(copy, random);
                if (!copy.isEmpty() && random.nextBoolean()) {
                    copy.add(copy.get(random.nextInt(copy.size())));
                }
            }
            return copy;
        }
    }

    private record Spec(boolean matchAll, List<Alternative> alternatives) {

        DcbCriteria build(Random random, Shape shape) {
            if (matchAll) {
                return shape == Shape.PLAIN || random.nextBoolean() ? DcbCriteria.all() : DcbCriteria.anyOf(DcbCriteria.all(), DcbCriteria.type("A"));
            }
            List<DcbCriterion> criteria = new ArrayList<>(alternatives.stream().map(alternative -> alternative.build(random, shape)).toList());
            if (shape == Shape.PLAIN) {
                return DcbCriteria.anyOf(criteria);
            }
            Collections.shuffle(criteria, random);
            if (random.nextBoolean()) {
                criteria.add(criteria.get(random.nextInt(criteria.size())));
            }
            return switch (random.nextInt(4)) {
                case 0 -> new DcbCriteria.Items(criteria);
                case 1 -> nested(criteria, random);
                case 2 -> DcbCriteria.anyOf(criteria);
                default -> DcbCriteria.anyOf(criteria.reversed());
            };
        }

        // anyOf(anyOf(x, y), z) in a random grouping
        private static DcbCriteria nested(List<DcbCriterion> criteria, Random random) {
            int split = 1 + random.nextInt(criteria.size());
            DcbCriteria head = DcbCriteria.anyOf(criteria.subList(0, split));
            if (split == criteria.size()) {
                return head;
            }
            return DcbCriteria.anyOf(head, DcbCriteria.anyOf(criteria.subList(split, criteria.size())));
        }
    }
}
