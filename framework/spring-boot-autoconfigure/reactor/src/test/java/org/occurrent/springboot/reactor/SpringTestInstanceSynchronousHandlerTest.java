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
package org.occurrent.springboot.reactor;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.testkit.engine.EngineExecutionResults;
import org.junit.platform.testkit.engine.EngineTestKit;
import org.occurrent.annotation.SynchronousSubscription;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

/**
 * Spring's test support initializes the test instance under its class name plus ".ORIGINAL", a name without a bean
 * definition. A test class that declares a synchronous handler must still get its instance prepared, since the context
 * never exposes that instance and its handler can't miss a write through the application service.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringTestInstanceSynchronousHandlerTest {

    @Test
    void a_SpringBootTest_class_that_declares_a_synchronous_handler_has_its_test_instance_prepared() {
        EngineExecutionResults results = EngineTestKit.engine("junit-jupiter")
                .selectors(selectClass(HandlerDeclaringSpringBootTest.class))
                .execute();

        List<Throwable> failures = results.testEvents().failed().stream()
                .map(event -> event.getRequiredPayload(TestExecutionResult.class).getThrowable().orElseThrow())
                .toList();
        assertThat(failures).isEmpty();
        results.testEvents().assertStatistics(statistics -> statistics.started(1).succeeded(1).failed(0));
    }

    @Test
    void an_object_that_declares_a_synchronous_handler_is_initialized_under_its_own_class_name_plus_the_original_instance_suffix() {
        new ApplicationContextRunner()
                .withBean(OccurrentReactiveAnnotationBeanPostProcessor.class, OccurrentReactiveAnnotationBeanPostProcessor::new)
                .withUserConfiguration(FactoryBeanSynchronousHandlerRefusalTest.RefusalConfiguration.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();

                    FactoryBeanSynchronousHandlerRefusalTest.SynchronousProduct product = new FactoryBeanSynchronousHandlerRefusalTest.SynchronousProduct();
                    String name = FactoryBeanSynchronousHandlerRefusalTest.SynchronousProduct.class.getName() + AutowireCapableBeanFactory.ORIGINAL_INSTANCE_SUFFIX;

                    assertThatCode(() -> context.getAutowireCapableBeanFactory().initializeBean(product, name))
                            .doesNotThrowAnyException();
                });
    }

    @SpringBootTest(classes = {FactoryBeanSynchronousHandlerRefusalTest.RefusalConfiguration.class, HandlerDeclaringSpringBootTest.ProcessorConfiguration.class})
    static class HandlerDeclaringSpringBootTest {

        @Test
        void the_context_starts() {
        }

        @SynchronousSubscription(id = "spring-test-instance-synchronous-handler")
        void onEvent(LateBeanAnnotationRegistrationTest.TestEvent event) {
        }

        @Configuration(proxyBeanMethods = false)
        static class ProcessorConfiguration {
            @Bean
            static OccurrentReactiveAnnotationBeanPostProcessor processor() {
                return new OccurrentReactiveAnnotationBeanPostProcessor();
            }
        }
    }
}
