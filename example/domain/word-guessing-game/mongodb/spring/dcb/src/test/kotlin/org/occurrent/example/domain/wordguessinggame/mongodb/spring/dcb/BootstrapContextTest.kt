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
package org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.occurrent.application.service.blocking.ApplicationService
import org.occurrent.application.service.blocking.dcb.DcbApplicationService
import org.occurrent.dsl.subscription.blocking.Subscriptions
import org.occurrent.eventstore.api.dcb.DcbEventStore
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore
import org.occurrent.example.domain.wordguessinggame.event.GameEvent
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContext
import org.springframework.core.ResolvableType

@SpringBootTest(classes = [Bootstrap::class, TestBootstrap::class])
class BootstrapContextTest {

    @Autowired
    private lateinit var context: ApplicationContext

    @Autowired
    private lateinit var eventStore: SpringMongoEventStore

    @Test
    fun `manual DCB bootstrap exposes DCB infrastructure without stream application service`() {
        assertThat(context.getBeanNamesForType(DcbEventStore::class.java)).isNotEmpty()
        assertThat(context.getBeanNamesForGenericType(DcbApplicationService::class.java)).isNotEmpty()
        assertThat(context.getBeanNamesForGenericType(Subscriptions::class.java)).isNotEmpty()
        assertThat(context.getBeanNamesForGenericType(ApplicationService::class.java)).isEmpty()
    }

    @Test
    fun `manual DCB bootstrap rejects stream event store APIs`() {
        assertThatThrownBy { eventStore.read("game-stream") }
            .isInstanceOf(UnsupportedOperationException::class.java)
            .hasMessage("STREAM capability is not enabled for this SpringMongoEventStore")
        assertThatThrownBy { eventStore.exists("game-stream") }
            .isInstanceOf(UnsupportedOperationException::class.java)
            .hasMessage("STREAM capability is not enabled for this SpringMongoEventStore")
    }

    private fun ApplicationContext.getBeanNamesForGenericType(type: Class<*>): Array<String> =
        getBeanNamesForType(ResolvableType.forClassWithGenerics(type, GameEvent::class.java))
}
