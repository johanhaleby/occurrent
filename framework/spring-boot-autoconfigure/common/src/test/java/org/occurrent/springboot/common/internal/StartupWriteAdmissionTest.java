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
package org.occurrent.springboot.common.internal;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
class StartupWriteAdmissionTest {

    @Test
    void admits_no_write_until_it_is_opened_and_every_write_after() {
        StartupWriteAdmission admission = new StartupWriteAdmission();

        assertThat(admission.admitsWrite()).as("[before open()]").isFalse();

        admission.open();

        assertThat(admission.admitsWrite()).as("[after open()]").isTrue();
    }

    @Test
    void a_bean_that_starts_its_creation_before_open_is_not_refused() throws Exception {
        StartupWriteAdmission admission = new StartupWriteAdmission();
        Object bean = new Object();

        assertThatCode(() -> admission.synchronousBeanCreationStarts(bean, "receiver", handler())).doesNotThrowAnyException();
        assertThatCode(() -> admission.synchronousBeanCreationEnds(bean, () -> true)).doesNotThrowAnyException();
    }

    @Test
    void a_bean_that_starts_its_creation_after_open_is_refused_by_name_before_it_is_initialized() throws Exception {
        StartupWriteAdmission admission = new StartupWriteAdmission();
        admission.open();

        assertThatThrownBy(() -> admission.synchronousBeanCreationStarts(new Object(), "lateReceiver", handler()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Bean 'lateReceiver' declares the synchronous handler " + Receiver.class.getName() + "#on")
                .hasMessageContaining("is created after the startup scan of the Spring Boot starter registered the synchronous handlers");
    }

    @Test
    void a_bean_whose_creation_began_before_open_and_ended_after_with_its_handler_unregistered_is_refused() throws Exception {
        StartupWriteAdmission admission = new StartupWriteAdmission();
        Object bean = new Object();
        admission.synchronousBeanCreationStarts(bean, "slowReceiver", handler());
        admission.open();

        assertThatThrownBy(() -> admission.synchronousBeanCreationEnds(bean, () -> true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Bean 'slowReceiver' declares the synchronous handler");
    }

    @Test
    void a_bean_whose_creation_began_before_open_and_ended_after_is_not_refused_when_the_scan_registered_its_handler() throws Exception {
        StartupWriteAdmission admission = new StartupWriteAdmission();
        Object bean = new Object();
        admission.synchronousBeanCreationStarts(bean, "registeredReceiver", handler());
        admission.open();
        AtomicBoolean handlerUnregistered = new AtomicBoolean(false);

        assertThatCode(() -> admission.synchronousBeanCreationEnds(bean, handlerUnregistered::get)).doesNotThrowAnyException();
    }

    @Test
    void a_bean_that_never_started_is_not_refused_when_it_ends() {
        StartupWriteAdmission admission = new StartupWriteAdmission();
        admission.open();

        assertThatCode(() -> admission.synchronousBeanCreationEnds(new Object(), () -> true)).doesNotThrowAnyException();
    }

    @Test
    void a_factory_bean_product_is_refused_after_open_and_not_before() throws Exception {
        StartupWriteAdmission admission = new StartupWriteAdmission();

        assertThatCode(() -> admission.synchronousProductBuilt("factoryProduct", handler())).as("[before open()]").doesNotThrowAnyException();

        admission.open();

        assertThatThrownBy(() -> admission.synchronousProductBuilt("factoryProduct", handler()))
                .as("[after open()]")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Bean 'factoryProduct' declares the synchronous handler");
    }

    private static Method handler() throws NoSuchMethodException {
        return Receiver.class.getDeclaredMethod("on");
    }

    static class Receiver {
        void on() {
        }
    }
}
