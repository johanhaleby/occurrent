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

package org.occurrent.example.domain.dcbpatterns.optintoken

import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper
import org.occurrent.application.service.blocking.dcb.DcbApplicationService
import org.occurrent.application.service.blocking.dcb.GenericDcbApplicationService
import org.occurrent.dsl.dcb.blocking.execute
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import tools.jackson.module.kotlin.jacksonObjectMapper
import java.net.URI
import java.time.Instant

class OptInTokenTest {

    private val eventStore = InMemoryEventStore()
    private val converter: CloudEventConverter<SignUpEvent> = JacksonCloudEventConverter.Builder<SignUpEvent>(jacksonObjectMapper(), URI.create("urn:occurrent:example:dcb-patterns"))
        .typeMapper(ReflectionCloudEventTypeMapper.simple(SignUpEvent::class.java))
        .idMapper { it.eventId.toString() }
        .build()
    private val applicationService: DcbApplicationService<SignUpEvent> = GenericDcbApplicationService(eventStore, converter)

    private val email = "johan@example.com"
    private val otp = "123456"
    private val initiatedAt = Instant.parse("2026-01-01T00:00:00Z")

    @Test
    fun `confirming within the TTL succeeds`() {
        applicationService.execute(SignUpCommand.InitiateSignUp(email, otp, initiatedAt), signUpDcbDecider)

        applicationService.execute(SignUpCommand.ConfirmSignUp(email, otp, initiatedAt.plus(SignUpTokenPolicy.TTL).minusSeconds(1)), signUpDcbDecider)
        // No exception means the sign-up was confirmed.
    }

    @Test
    fun `confirming a second time is rejected because the token is already consumed`() {
        applicationService.execute(SignUpCommand.InitiateSignUp(email, otp, initiatedAt), signUpDcbDecider)
        applicationService.execute(SignUpCommand.ConfirmSignUp(email, otp, initiatedAt.plusSeconds(60)), signUpDcbDecider)

        assertThatThrownBy {
            applicationService.execute(SignUpCommand.ConfirmSignUp(email, otp, initiatedAt.plusSeconds(120)), signUpDcbDecider)
        }.isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("already confirmed")
    }

    @Test
    fun `confirming after the TTL has expired is rejected`() {
        applicationService.execute(SignUpCommand.InitiateSignUp(email, otp, initiatedAt), signUpDcbDecider)

        val expired = initiatedAt.plus(SignUpTokenPolicy.TTL).plusSeconds(1)
        assertThatThrownBy {
            applicationService.execute(SignUpCommand.ConfirmSignUp(email, otp, expired), signUpDcbDecider)
        }.isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("expired")
    }
}
