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
 */

package org.occurrent.springboot.common.internal;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(ReplaceUnderscores.class)
class LateLiveStartsTest {

    private final LateLiveStarts lateLiveStarts = new LateLiveStarts();

    @Test
    void an_instance_that_was_put_is_removed_with_its_value_once() {
        Object instance = new Object();
        lateLiveStarts.put(instance, "boundary");

        assertThat(lateLiveStarts.remove(instance)).isEqualTo("boundary");
        assertThat(lateLiveStarts.remove(instance)).isNull();
    }

    @Test
    void an_instance_that_was_never_put_has_no_value() {
        lateLiveStarts.put(new Object(), "boundary");

        assertThat(lateLiveStarts.remove(new Object())).isNull();
    }

    @Test
    void two_distinct_instances_that_are_equal_each_keep_their_own_value() {
        Equal first = new Equal();
        Equal second = new Equal();
        assertThat(first).isEqualTo(second);
        lateLiveStarts.put(first, "first");
        lateLiveStarts.put(second, "second");

        assertThat(lateLiveStarts.remove(second)).isEqualTo("second");
        assertThat(lateLiveStarts.remove(first)).isEqualTo("first");
    }

    @Test
    void an_instance_whose_hash_code_changed_after_it_was_put_is_still_found() {
        Mutable instance = new Mutable();
        lateLiveStarts.put(instance, "boundary");

        instance.state = 42;

        assertThat(lateLiveStarts.remove(instance)).isEqualTo("boundary");
    }

    @Test
    void putting_the_same_instance_again_replaces_its_value() {
        Object instance = new Object();
        lateLiveStarts.put(instance, "old");
        lateLiveStarts.put(instance, "new");

        assertThat(lateLiveStarts.remove(instance)).isEqualTo("new");
        assertThat(lateLiveStarts.remove(instance)).isNull();
    }

    @Test
    void getting_an_instance_reads_its_value_without_removing_it() {
        Object instance = new Object();
        lateLiveStarts.put(instance, "boundary");

        assertThat(lateLiveStarts.get(instance)).isEqualTo("boundary");
        assertThat(lateLiveStarts.get(instance)).isEqualTo("boundary");
        assertThat(lateLiveStarts.remove(instance)).isEqualTo("boundary");
    }

    @Test
    void getting_an_instance_that_was_never_put_gives_no_value() {
        lateLiveStarts.put(new Object(), "boundary");

        assertThat(lateLiveStarts.get(new Object())).isNull();
    }

    @Test
    void getting_one_of_two_equal_instances_gives_its_own_value() {
        Equal first = new Equal();
        Equal second = new Equal();
        lateLiveStarts.put(first, "first");
        lateLiveStarts.put(second, "second");

        assertThat(lateLiveStarts.get(second)).isEqualTo("second");
        assertThat(lateLiveStarts.get(first)).isEqualTo("first");
    }

    private static final class Equal {
        @Override
        public boolean equals(Object other) {
            return other instanceof Equal;
        }

        @Override
        public int hashCode() {
            return 1;
        }
    }

    private static final class Mutable {
        int state;

        @Override
        public boolean equals(Object other) {
            return other instanceof Mutable mutable && mutable.state == state;
        }

        @Override
        public int hashCode() {
            return state;
        }
    }
}
