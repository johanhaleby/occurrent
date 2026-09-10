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

package org.occurrent.springboot.reactor;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A bean built after startup registers on whatever thread asked for it, and nothing serialises those threads,
 * because a lock held across a late registration deadlocks against a thread building one of the collaborators that
 * registration resolves. What keeps it safe instead is that every piece of state a late registration touches is
 * concurrent, and that each of these collections is drained by polling rather than by iterating and then clearing,
 * since an entry added between those two is dropped without being processed.
 * <p>
 * This does not demonstrate any of that. The interleaving cannot be staged from a test, because parking a thread
 * inside a bean factory serialises other singleton creation at the Spring level, so the two threads the hazard
 * needs never overlap. What this catches is the realistic way the property regresses, which is somebody
 * simplifying one of these back to an {@code ArrayList} or a {@code HashSet} while tidying.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class LateRegistrationConcurrencyContractTest {

    @Test
    void the_coordinators_registries_are_concurrent_sets() throws Exception {
        OccurrentReactiveAnnotationBeanPostProcessor coordinator = new OccurrentReactiveAnnotationBeanPostProcessor();
        for (String fieldName : new String[]{"registeredIds", "registeredHandlers", "alreadyBuiltToBeScanned"}) {
            assertThat(valueOf(coordinator, fieldName))
                    .describedAs(fieldName)
                    .isInstanceOf(Set.class)
                    .isInstanceOf(ConcurrentHashMap.KeySetView.class);
        }
    }

    @Test
    void every_collection_a_late_registration_appends_to_is_a_queue_rather_than_a_list() throws Exception {
        assertQueue(ProjectionAnnotationRegistrar.class, "domainFeedsToCatchUp");
        assertQueue(ProjectionAnnotationRegistrar.class, "pushModels");
        assertQueue(ProjectionAnnotationRegistrar.class, "backgroundFeeds");
        assertQueue(ProjectionAnnotationRegistrar.class, "backgroundCatchUps");
    }

    // A Queue rather than any Collection, because ArrayList is a Collection and a List but never a Queue, so this
    // is what a revert to one would fail on. The declared type is also what says the drain polls.
    private static void assertQueue(Class<?> owner, String fieldName) throws Exception {
        Field field = owner.getDeclaredField(fieldName);
        assertThat(field.getType()).describedAs("%s.%s".formatted(owner.getSimpleName(), fieldName)).isEqualTo(Queue.class);
        assertThat(field.getType().isAssignableFrom(ArrayList.class)).isFalse();
    }

    private static Object valueOf(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }
}
