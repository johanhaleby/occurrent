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

import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
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
 * needs never overlap.
 * <p>
 * It does not catch the drain style either, and the drain style is what actually went wrong. One of these
 * collections shipped with the right type and a drain that still iterated and then cleared, and this passed it. The
 * reason each drain says so at the loop itself is that a sentence where the cursor already is beats an assertion in
 * another file. What is left here is somebody putting an {@code ArrayList} or a {@code HashSet} back, which is a
 * smaller regression than a drain that clears.
 * <p>
 * The handoff assertion is a different shape. It reads a type rather than a style, and the type is the whole
 * argument: a handoff carrying only names cannot register a bean whose thread is still creating it, so it has to
 * drop it, and the bean is never scanned. Naming that here keeps the next reader from simplifying the record away.
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

    // Whether a registration may block for a replay and where a delivery is sent are the same question, and one
    // method answers it. Two copies of the condition is the regression, since changing one and not the other sends
    // a blocking replay to the instance captured on the way through. This only catches the copies being made, not
    // the two answers drifting, which is why the method exists rather than the condition being written twice.
    @Test
    void one_method_decides_both_whether_to_block_and_where_to_deliver() throws Exception {
        assertThat(OccurrentReactiveAnnotationBeanPostProcessor.class.getDeclaredMethod(
                "publishedBeanIsResolvable", ConfigurableListableBeanFactory.class, String.class, boolean.class))
                .describedAs("the single condition behind blocking and delivery")
                .isNotNull();
    }

    // The name and the instance travel together through the startup handoff. A queue of names alone leaves the
    // drain with a name it cannot resolve while its thread is still creating it, and the only way out of that is
    // to drop the entry, which loses the bean's annotations for the life of the context.
    @Test
    void the_startup_handoff_carries_the_instance_alongside_the_name() throws Exception {
        Field field = OccurrentReactiveAnnotationBeanPostProcessor.class.getDeclaredField("builtWhileScanning");
        Type element = ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];

        assertThat(element).describedAs("element of builtWhileScanning").isNotEqualTo(String.class);
        assertThat(((Class<?>) element).getRecordComponents())
                .describedAs("what the handoff carries")
                .extracting(RecordComponent::getType)
                .contains(String.class, Object.class);
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
