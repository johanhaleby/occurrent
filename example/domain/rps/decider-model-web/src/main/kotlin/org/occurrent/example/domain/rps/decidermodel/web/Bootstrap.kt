/*
 *
 *  Copyright 2023 Johan Haleby
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

package org.occurrent.example.domain.rps.decidermodel.web

import nz.net.ultraq.thymeleaf.layoutdialect.LayoutDialect
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper
import org.occurrent.example.domain.rps.decidermodel.GameEvent
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean

@SpringBootApplication
class Bootstrap {

    @Bean
    fun layoutDialect(): LayoutDialect = LayoutDialect()

    @Bean
    fun rpsTypeMapper(): CloudEventTypeMapper<GameEvent> = ReflectionCloudEventTypeMapper.simple(GameEvent::class.java)

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            runApplication<Bootstrap>(*args)
        }
    }
}