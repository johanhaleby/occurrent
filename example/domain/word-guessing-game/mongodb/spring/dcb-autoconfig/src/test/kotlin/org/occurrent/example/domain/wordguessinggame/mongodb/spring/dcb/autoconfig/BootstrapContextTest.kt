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

package org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.occurrent.application.service.blocking.ApplicationService
import org.occurrent.application.service.blocking.dcb.DcbApplicationService
import org.occurrent.dsl.decider.Decider
import org.occurrent.dsl.query.blocking.DomainEventQueries
import org.occurrent.dsl.subscription.blocking.Subscriptions
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStoreCapability.DCB
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Import

@SpringBootTest(classes = [Bootstrap::class])
@Import(TestBootstrap::class)
class BootstrapContextTest {

    @Autowired
    private lateinit var applicationContext: ApplicationContext

    @Test
    fun `starts dcb-only occurrent context`() {
        assertThat(applicationContext.getBeansOfType(SpringMongoEventStore::class.java)).hasSize(1)
        assertThat(applicationContext.getBean(EventStoreConfig::class.java).eventStoreCapabilities).containsExactly(DCB)
        assertThat(applicationContext.getBeansOfType(Subscriptions::class.java)).hasSize(1)
        assertThat(applicationContext.getBeansOfType(ApplicationService::class.java)).isEmpty()
        assertThat(applicationContext.getBeansOfType(DomainEventQueries::class.java)).isEmpty()
        assertThat(applicationContext.getBeansOfType(DcbApplicationService::class.java))
            .containsOnlyKeys("occurrentDcbApplicationService")
        assertThat(applicationContext.getBeansOfType(Decider::class.java)).hasSize(1)
    }

    @Test
    fun `dcb-only autoconfig rejects stream event store APIs`() {
        val eventStore = applicationContext.getBean(SpringMongoEventStore::class.java)

        assertThatThrownBy { eventStore.read("game-stream") }
            .isInstanceOf(UnsupportedOperationException::class.java)
            .hasMessage("STREAM capability is not enabled for this SpringMongoEventStore")
        assertThatThrownBy { eventStore.exists("game-stream") }
            .isInstanceOf(UnsupportedOperationException::class.java)
            .hasMessage("STREAM capability is not enabled for this SpringMongoEventStore")
    }
}
